import asyncio
import os

from uuid import UUID

from aiokafka import (
    AIOKafkaConsumer,
    TopicPartition,
)

from redis.asyncio import Redis

from config import (
    POSTGRES_DSN,
    REDIS_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    JOBS_TOPIC,
    JOBS_GROUP_ID,
)

from domain.enums import JobStatus

from repositories.postgres_jobs import (
    PostgresJobRepository,
)

from workers.processing import (
    CriticalCheckError,
    process_transaction,
)
import json

WORKER_NAME = os.getenv(
    "WORKER_NAME",
    f"worker-{os.getpid()}",
)

EXTERNAL_CONCURRENCY_LIMIT = 4


async def process_message(
    job_id: UUID,
    repository: PostgresJobRepository,
    semaphore: asyncio.Semaphore,
    redis_client: Redis,
):

    job = await repository.get(
        job_id
    )

    if job is None:

        raise RuntimeError(
            f"Job not found: {job_id}"
        )

    # Повторная доставка Kafka.
    #
    # Job уже полностью закончена —
    # делать повторно ничего не надо.

    if job.status in (
        JobStatus.COMPLETED,
        JobStatus.FAILED,
    ):

        print(
            f"[{WORKER_NAME}] "
            f"SKIP "
            f"job={job.id} "
            f"status={job.status.value}"
        )

        return

    await repository.update_status(
        job.id,
        JobStatus.PROCESSING,
    )

    try:

        result = await process_transaction(
            job,
            semaphore,
            redis_client,
        )

        side_effect_created = (
            await repository.record_side_effect(
                job.id,
                result.decision,
            )
        )

        if side_effect_created:

            print(
                f"[{WORKER_NAME}] "
                f"SIDE EFFECT CREATED "
                f"job={job.id}"
            )

        else:

            print(
                f"[{WORKER_NAME}] "
                f"SIDE EFFECT ALREADY EXISTS "
                f"job={job.id}"
            )

        await repository.save_result(
            job.id,
            result,
        )

        print(
            f"[{WORKER_NAME}] "
            f"COMPLETED "
            f"job={job.id}"
        )

    except CriticalCheckError as exc:

        await repository.save_error(
            job.id,
            str(exc),
        )

        print(
            f"[{WORKER_NAME}] "
            f"FAILED CRITICAL "
            f"job={job.id}: {exc}"
        )

    except Exception as exc:

        await repository.save_error(
            job.id,
            str(exc),
        )

        print(
            f"[{WORKER_NAME}] "
            f"FAILED "
            f"job={job.id}: "
            f"{type(exc).__name__}: "
            f"{exc}"
        )


async def main():

    print(
        f"[{WORKER_NAME}] START "
        f"pid={os.getpid()}"
    )

    repository = PostgresJobRepository(
        dsn=POSTGRES_DSN
    )

    await repository.connect()

    redis_client = Redis.from_url(
        REDIS_URL,
        decode_responses=True,
    )

    await redis_client.ping()

    semaphore = asyncio.Semaphore(
        EXTERNAL_CONCURRENCY_LIMIT
    )

    consumer = AIOKafkaConsumer(
        JOBS_TOPIC,

        bootstrap_servers=(
            KAFKA_BOOTSTRAP_SERVERS
        ),

        group_id=JOBS_GROUP_ID,

        enable_auto_commit=False,

        auto_offset_reset="earliest",
    )

    await consumer.start()

    print(
        f"[{WORKER_NAME}] "
        f"KAFKA CONNECTED"
    )

    try:

        async for message in consumer:

            event = json.loads(
                message.value.decode()
            )

            job_id = UUID(
                event["job_id"]
            )

            event_id = event["event_id"]

            print(
                f"[{WORKER_NAME}] "
                f"RECEIVED "
                f"event={event_id} "
                f"job={job_id} "
                f"partition={message.partition} "
                f"offset={message.offset}"
            )

            await process_message(
                job_id=job_id,
                repository=repository,
                semaphore=semaphore,
                redis_client=redis_client,
            )

            partition = TopicPartition(
                message.topic,
                message.partition,
            )

            await consumer.commit(
                {
                    partition:
                    message.offset + 1
                }
            )

            print(
                f"[{WORKER_NAME}] "
                f"COMMITTED "
                f"partition={message.partition} "
                f"offset={message.offset + 1}"
            )

    finally:

        await consumer.stop()

        await redis_client.aclose()

        await repository.close()

        print(
            f"[{WORKER_NAME}] STOP"
        )


if __name__ == "__main__":

    asyncio.run(
        main()
    )