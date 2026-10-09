import asyncio
import random

from redis.asyncio import Redis

from domain.enums import JobStatus
from domain.models import Job, JobResult

from external_services import (
    FraudResult,
    UserRiskResult,
    MerchantRiskResult,
    TemporaryServiceError,
    check_fraud,
    get_user_risk,
    get_merchant_risk,
)

from repositories.postgres_jobs import (
    PostgresJobRepository,
)


MAX_ATTEMPTS = 3

TIMEOUT = 2.2

BASE_RETRY_DELAY = 0.2

CHECK_CACHE_TTL = 3600

JOB_LOCK_TTL = 60

LEASE_SECONDS = 60
POLL_INTERVAL = 0.5


class CriticalCheckError(Exception):
    pass


# =========================================================
# FRAUD
# critical check
# =========================================================

async def run_fraud_check(
    job: Job,
    semaphore: asyncio.Semaphore,
    redis_client: Redis,
):
    key = f"job:{job.id}:check:fraud"

    # -------------------------
    # CACHE
    # -------------------------

    cached = await redis_client.get(key)

    if cached is not None:

        print(
            f"[fraud] CACHE HIT "
            f"job={job.id}"
        )

        return FraudResult(
            score=float(cached)
        )

    print(
        f"[fraud] CACHE MISS "
        f"job={job.id}"
    )

    # -------------------------
    # RETRY
    # -------------------------

    for attempt in range(
        1,
        MAX_ATTEMPTS + 1,
    ):
        try:

            async with semaphore:

                async with asyncio.timeout(
                    TIMEOUT
                ):
                    result = await check_fraud(
                        user_id=(
                            job.transaction.user_id
                        ),
                        amount=(
                            job.transaction.amount
                        ),
                    )

            # checkpoint сразу после
            # успешного external call

            await redis_client.set(
                key,
                result.score,
                ex=CHECK_CACHE_TTL,
            )

            print(
                f"[fraud] CACHE SET "
                f"job={job.id}"
            )

            return result

        except asyncio.CancelledError:
            raise

        except (
            TemporaryServiceError,
            TimeoutError,
        ) as exc:

            if attempt == MAX_ATTEMPTS:
                raise

            delay = (
                BASE_RETRY_DELAY
                * 2 ** (attempt - 1)
                + random.uniform(0, 0.1)
            )

            print(
                f"[fraud] "
                f"attempt={attempt} failed: "
                f"{type(exc).__name__}: {exc}. "
                f"retry={delay:.2f}s"
            )

            await asyncio.sleep(delay)


# =========================================================
# USER
# optional check
# =========================================================

async def run_user_check(
    job: Job,
    semaphore: asyncio.Semaphore,
    redis_client: Redis,
):
    key = f"job:{job.id}:check:user"

    cached = await redis_client.get(key)

    if cached is not None:

        print(
            f"[user] CACHE HIT "
            f"job={job.id}"
        )

        return UserRiskResult(
            risk=cached
        )

    print(
        f"[user] CACHE MISS "
        f"job={job.id}"
    )

    for attempt in range(
        1,
        MAX_ATTEMPTS + 1,
    ):
        try:

            async with semaphore:

                async with asyncio.timeout(
                    TIMEOUT
                ):
                    result = await get_user_risk(
                        user_id=(
                            job.transaction.user_id
                        ),
                    )

            await redis_client.set(
                key,
                result.risk,
                ex=CHECK_CACHE_TTL,
            )

            print(
                f"[user] CACHE SET "
                f"job={job.id}"
            )

            return result

        except asyncio.CancelledError:
            raise

        except (
            TemporaryServiceError,
            TimeoutError,
        ) as exc:

            if attempt == MAX_ATTEMPTS:
                raise

            delay = (
                BASE_RETRY_DELAY
                * 2 ** (attempt - 1)
                + random.uniform(0, 0.1)
            )

            print(
                f"[user] "
                f"attempt={attempt} failed: "
                f"{type(exc).__name__}: {exc}. "
                f"retry={delay:.2f}s"
            )

            await asyncio.sleep(delay)


# =========================================================
# MERCHANT
# optional check
# =========================================================

async def run_merchant_check(
    job: Job,
    semaphore: asyncio.Semaphore,
    redis_client: Redis,
):
    key = (
        f"job:{job.id}:check:merchant"
    )

    cached = await redis_client.get(key)

    if cached is not None:

        print(
            f"[merchant] CACHE HIT "
            f"job={job.id}"
        )

        return MerchantRiskResult(
            risk=cached
        )

    print(
        f"[merchant] CACHE MISS "
        f"job={job.id}"
    )

    for attempt in range(
        1,
        MAX_ATTEMPTS + 1,
    ):
        try:

            async with semaphore:

                async with asyncio.timeout(
                    TIMEOUT
                ):
                    result = (
                        await get_merchant_risk(
                            merchant_id=(
                                job.transaction
                                .merchant.id
                            ),
                        )
                    )

            await redis_client.set(
                key,
                result.risk,
                ex=CHECK_CACHE_TTL,
            )

            print(
                f"[merchant] CACHE SET "
                f"job={job.id}"
            )

            return result

        except asyncio.CancelledError:
            raise

        except (
            TemporaryServiceError,
            TimeoutError,
        ) as exc:

            if attempt == MAX_ATTEMPTS:
                raise

            delay = (
                BASE_RETRY_DELAY
                * 2 ** (attempt - 1)
                + random.uniform(0, 0.1)
            )

            print(
                f"[merchant] "
                f"attempt={attempt} failed: "
                f"{type(exc).__name__}: {exc}. "
                f"retry={delay:.2f}s"
            )

            await asyncio.sleep(delay)


# =========================================================
# OPTIONAL WRAPPERS
# =========================================================

async def run_optional_user_check(
    job: Job,
    semaphore: asyncio.Semaphore,
    redis_client: Redis,
):
    try:

        return await run_user_check(
            job,
            semaphore,
            redis_client,
        )

    except (
        TemporaryServiceError,
        TimeoutError,
    ) as exc:

        print(
            "[user] optional check unavailable: "
            f"{type(exc).__name__}: {exc}"
        )

        return None


async def run_optional_merchant_check(
    job: Job,
    semaphore: asyncio.Semaphore,
    redis_client: Redis,
):
    try:

        return await run_merchant_check(
            job,
            semaphore,
            redis_client,
        )

    except (
        TemporaryServiceError,
        TimeoutError,
    ) as exc:

        print(
            "[merchant] optional check unavailable: "
            f"{type(exc).__name__}: {exc}"
        )

        return None


# =========================================================
# PROCESS TRANSACTION
# =========================================================

async def process_transaction(
    job: Job,
    semaphore: asyncio.Semaphore,
    redis_client: Redis,
) -> JobResult:

    critical_error = None

    try:

        async with asyncio.TaskGroup() as tg:

            fraud_task = tg.create_task(
                run_fraud_check(
                    job,
                    semaphore,
                    redis_client,
                )
            )

            user_task = tg.create_task(
                run_optional_user_check(
                    job,
                    semaphore,
                    redis_client,
                )
            )

            merchant_task = tg.create_task(
                run_optional_merchant_check(
                    job,
                    semaphore,
                    redis_client,
                )
            )

    except* TemporaryServiceError:

        critical_error = CriticalCheckError(
            "Fraud service unavailable "
            "after retries"
        )

    except* TimeoutError:

        critical_error = CriticalCheckError(
            "Fraud service timeout "
            "after retries"
        )

    if critical_error is not None:
        raise critical_error

    fraud = fraud_task.result()
    user = user_task.result()
    merchant = merchant_task.result()

    user_risk = (
        user.risk
        if user is not None
        else "unknown"
    )

    merchant_risk = (
        merchant.risk
        if merchant is not None
        else "unknown"
    )

    if fraud.score >= 0.8:

        decision = "reject"

    elif (
        fraud.score >= 0.5
        or user_risk == "high"
        or merchant_risk == "high"
        or user_risk == "unknown"
        or merchant_risk == "unknown"
    ):

        decision = "review"

    else:

        decision = "approve"

    return JobResult(
        fraud_score=fraud.score,
        user_risk=user_risk,
        merchant_risk=merchant_risk,
        decision=decision,
    )