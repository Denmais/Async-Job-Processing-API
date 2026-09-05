import asyncio
import random

from domain.enums import JobStatus
from domain.models import Job, JobResult
from external_services import (
    TemporaryServiceError,
    check_fraud,
    get_user_risk,
    get_merchant_risk,
)
from repositories.jobs import SqliteJobRepository


MAX_ATTEMPTS = 3
TIMEOUT = 2.2
BASE_RETRY_DELAY = 0.2


async def run_fraud_check(
    job: Job,
    semaphore: asyncio.Semaphore,
):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            async with semaphore:
                async with asyncio.timeout(TIMEOUT):
                    return await check_fraud(
                        user_id=job.transaction.user_id,
                        amount=job.transaction.amount,
                    )

        except asyncio.CancelledError:
            raise

        except (TemporaryServiceError, TimeoutError) as exc:
            if attempt == MAX_ATTEMPTS:
                raise

            delay = (
                BASE_RETRY_DELAY
                * 2 ** (attempt - 1)
                + random.uniform(0, 0.1)
            )

            print(
                f"[fraud] attempt={attempt} failed: {exc}. "
                f"retry={delay:.2f}s"
            )

            await asyncio.sleep(delay)


async def run_user_check(
    job: Job,
    semaphore: asyncio.Semaphore,
):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            async with semaphore:
                async with asyncio.timeout(TIMEOUT):
                    return await get_user_risk(
                        user_id=job.transaction.user_id,
                    )

        except asyncio.CancelledError:
            raise

        except (TemporaryServiceError, TimeoutError) as exc:
            if attempt == MAX_ATTEMPTS:
                raise

            delay = (
                BASE_RETRY_DELAY
                * 2 ** (attempt - 1)
                + random.uniform(0, 0.1)
            )

            print(
                f"[user] attempt={attempt} failed: {exc}. "
                f"retry={delay:.2f}s"
            )

            await asyncio.sleep(delay)


async def run_merchant_check(
    job: Job,
    semaphore: asyncio.Semaphore,
):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            async with semaphore:
                async with asyncio.timeout(TIMEOUT):
                    return await get_merchant_risk(
                        merchant_id=job.transaction.merchant.id,
                    )

        except asyncio.CancelledError:
            raise

        except (TemporaryServiceError, TimeoutError) as exc:
            if attempt == MAX_ATTEMPTS:
                raise

            delay = (
                BASE_RETRY_DELAY
                * 2 ** (attempt - 1)
                + random.uniform(0, 0.1)
            )

            print(
                f"[merchant] attempt={attempt} failed: {exc}. "
                f"retry={delay:.2f}s"
            )

            await asyncio.sleep(delay)


async def process_transaction(
    job: Job,
    semaphore: asyncio.Semaphore,
) -> JobResult:

    async with asyncio.TaskGroup() as tg:

        fraud_task = tg.create_task(
            run_fraud_check(
                job,
                semaphore,
            )
        )

        user_task = tg.create_task(
            run_user_check(
                job,
                semaphore,
            )
        )

        merchant_task = tg.create_task(
            run_merchant_check(
                job,
                semaphore,
            )
        )

    fraud = fraud_task.result()
    user = user_task.result()
    merchant = merchant_task.result()

    if fraud.score >= 0.8:
        decision = "reject"

    elif (
        fraud.score >= 0.5
        or user.risk == "high"
        or merchant.risk == "high"
    ):
        decision = "review"

    else:
        decision = "approve"

    return JobResult(
        fraud_score=fraud.score,
        user_risk=user.risk,
        merchant_risk=merchant.risk,
        decision=decision,
    )


async def job_worker(
    worker_id: int,
    queue: asyncio.Queue,
    repository: SqliteJobRepository,
    semaphore: asyncio.Semaphore,
):
    while True:

        job_id = await queue.get()

        try:
            print(
                f"[worker-{worker_id}] "
                f"received job={job_id}"
            )

            await repository.update_status(
                job_id,
                JobStatus.PROCESSING,
            )

            job = await repository.get(
                job_id
            )

            if job is None:
                print(
                    f"[worker-{worker_id}] "
                    f"job not found={job_id}"
                )
                continue

            result = await process_transaction(
                job,
                semaphore,
            )

            await repository.save_result(
                job_id,
                result,
            )

            print(
                f"[worker-{worker_id}] "
                f"COMPLETED job={job_id}"
            )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            await repository.save_error(
                job_id,
                str(exc),
            )

            print(
                f"[worker-{worker_id}] "
                f"FAILED job={job_id}: {exc}"
            )

        finally:
            queue.task_done()