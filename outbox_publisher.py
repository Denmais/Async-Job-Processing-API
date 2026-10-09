import asyncio
import json
import os

from aiokafka import AIOKafkaProducer

from config import (
    POSTGRES_DSN,
    KAFKA_BOOTSTRAP_SERVERS,
    JOBS_TOPIC,
)

from repositories.postgres_jobs import (
    PostgresJobRepository,
)


POLL_INTERVAL = 0.5

BATCH_SIZE = 100


async def main():

    name = os.getenv(
        "PUBLISHER_NAME",
        "outbox-publisher",
    )

    print(
        f"[{name}] START"
    )

    # =========================================
    # POSTGRES
    # =========================================

    repository = PostgresJobRepository(
        dsn=POSTGRES_DSN
    )

    await repository.connect()

    # =========================================
    # KAFKA PRODUCER
    # =========================================

    producer = AIOKafkaProducer(
        bootstrap_servers=(
            KAFKA_BOOTSTRAP_SERVERS
        )
    )

    await producer.start()

    print(
        f"[{name}] KAFKA CONNECTED"
    )

    try:

        while True:

            events = (
                await repository
                .get_unpublished_outbox_events(
                    limit=BATCH_SIZE
                )
            )

            if not events:

                await asyncio.sleep(
                    POLL_INTERVAL
                )

                continue

            for event in events:

                payload = json.loads(
                    event["payload"]
                )

                message = {
                    "event_id": str(
                        event["id"]
                    ),

                    "event_type": (
                        event["event_type"]
                    ),

                    "job_id": (
                        payload["job_id"]
                    ),
                }

                try:

                    metadata = (
                        await producer
                        .send_and_wait(
                            JOBS_TOPIC,

                            value=(
                                json.dumps(
                                    message
                                ).encode()
                            ),

                            key=(
                                str(
                                    event[
                                        "aggregate_id"
                                    ]
                                ).encode()
                            ),
                        )
                    )

                    print(
                        f"[{name}] "
                        f"PUBLISHED "
                        f"event={event['id']} "
                        f"job={payload['job_id']} "
                        f"partition="
                        f"{metadata.partition} "
                        f"offset="
                        f"{metadata.offset}"
                    )

                    await (
                        repository
                        .mark_outbox_published(
                            event["id"]
                        )
                    )

                except Exception as exc:

                    print(
                        f"[{name}] "
                        f"PUBLISH FAILED "
                        f"event={event['id']} "
                        f"{type(exc).__name__}: "
                        f"{exc}"
                    )

                    await asyncio.sleep(1)

    finally:

        await producer.stop()

        await repository.close()

        print(
            f"[{name}] STOP"
        )


if __name__ == "__main__":

    asyncio.run(
        main()
    )