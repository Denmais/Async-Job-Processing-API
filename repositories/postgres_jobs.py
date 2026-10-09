import asyncpg

from datetime import datetime, timezone
from uuid import UUID, uuid4

from domain.enums import (
    Currency,
    JobStatus,
)
from domain.models import (
    Job,
    JobResult,
    Merchant,
    Transaction,
)
from datetime import (
    datetime,
    timezone,
    timedelta,
)
import json


class PostgresJobRepository:

    def __init__(
        self,
        dsn: str,
    ):
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None

    # =====================================================
    # CONNECTION POOL
    # =====================================================

    async def connect(self) -> None:

        self.pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=2,
            max_size=5,
        )

        async with self.pool.acquire() as conn:

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id UUID PRIMARY KEY,

                    transaction_id UUID NOT NULL,
                    user_id BIGINT NOT NULL,
                    amount DOUBLE PRECISION NOT NULL,
                    currency TEXT NOT NULL,

                    merchant_id BIGINT NOT NULL,
                    merchant_name TEXT NOT NULL,
                    merchant_country TEXT NOT NULL,

                    status TEXT NOT NULL,
                    locked_by TEXT,
                    locked_until TIMESTAMPTZ,

                    fraud_score DOUBLE PRECISION,
                    user_risk TEXT,
                    merchant_risk TEXT,
                    decision TEXT,
                    error TEXT,

                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    transaction_created_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            await conn.execute(
                """
                    ALTER TABLE jobs
                    ADD COLUMN IF NOT EXISTS locked_by TEXT
                """
            )

            await conn.execute(
                """
                    ALTER TABLE jobs
                    ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS outbox_events (
                    id UUID PRIMARY KEY,

                    aggregate_id UUID NOT NULL,

                    event_type TEXT NOT NULL,

                    payload JSONB NOT NULL,

                    created_at TIMESTAMPTZ NOT NULL,

                    published_at TIMESTAMPTZ
                )
                """
            )

            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS
                    idx_outbox_unpublished

                ON outbox_events (created_at)

                WHERE published_at IS NULL
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS side_effects (
                    id BIGSERIAL PRIMARY KEY,

                    job_id UUID NOT NULL,
                    effect_type TEXT NOT NULL,
                    payload TEXT NOT NULL,

                    created_at TIMESTAMPTZ NOT NULL,

                    UNIQUE(job_id, effect_type)
                )
                """
            )

    async def close(self) -> None:

        if self.pool is not None:
            await self.pool.close()

    def _get_pool(self) -> asyncpg.Pool:

        if self.pool is None:
            raise RuntimeError(
                "Postgres repository is not connected"
            )

        return self.pool

    # =====================================================
    # SAVE JOB
    # =====================================================

    async def save(
        self,
        job: Job,
    ) -> None:

        pool = self._get_pool()

        async with pool.acquire() as conn:

            await conn.execute(
                """
                INSERT INTO jobs (
                    id,
                    transaction_id,

                    user_id,
                    amount,
                    currency,

                    merchant_id,
                    merchant_name,
                    merchant_country,

                    status,

                    created_at,
                    updated_at,
                    transaction_created_at
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7, $8,
                    $9, $10, $11, $12
                )
                """,

                job.id,
                job.transaction.id,

                job.transaction.user_id,
                job.transaction.amount,
                job.transaction.currency.value,

                job.transaction.merchant.id,
                job.transaction.merchant.name,
                job.transaction.merchant.country,

                job.status.value,

                job.created_at,
                job.updated_at,
                job.transaction.created_at,
            )

    # =====================================================
    # GET JOB
    # =====================================================

    async def get(
        self,
        job_id: UUID,
    ) -> Job | None:

        pool = self._get_pool()

        async with pool.acquire() as conn:

            row = await conn.fetchrow(
                """
                SELECT *
                FROM jobs
                WHERE id = $1
                """,
                job_id,
            )

        if row is None:
            return None

        return self._row_to_job(row)

    # =====================================================
    # RECOVERY
    # =====================================================

    async def get_unfinished_job_ids(
        self,
    ) -> list[UUID]:

        pool = self._get_pool()

        async with pool.acquire() as conn:

            rows = await conn.fetch(
                """
                SELECT id
                FROM jobs
                WHERE status IN ($1, $2)
                ORDER BY created_at
                """,
                JobStatus.PENDING.value,
                JobStatus.PROCESSING.value,
            )

        return [
            row["id"]
            for row in rows
        ]

    # =====================================================
    # STATUS
    # =====================================================

    async def update_status(
        self,
        job_id: UUID,
        status: JobStatus,
    ) -> None:

        pool = self._get_pool()

        now = datetime.now(
            timezone.utc
        )

        async with pool.acquire() as conn:

            await conn.execute(
                """
                UPDATE jobs
                SET
                    status = $1,
                    updated_at = $2
                WHERE id = $3
                """,
                status.value,
                now,
                job_id,
            )

    # =====================================================
    # RESULT
    # =====================================================

    async def save_result(
        self,
        job_id: UUID,
        result: JobResult,
    ) -> None:

        pool = self._get_pool()

        now = datetime.now(
            timezone.utc
        )

        async with pool.acquire() as conn:

            await conn.execute(
                """
                UPDATE jobs

                SET
                    fraud_score = $1,
                    user_risk = $2,
                    merchant_risk = $3,
                    decision = $4,

                    error = NULL,

                    status = $5,

                    locked_by = NULL,
                    locked_until = NULL,

                    updated_at = $6

                WHERE id = $7
                """,

                result.fraud_score,
                result.user_risk,
                result.merchant_risk,
                result.decision,

                JobStatus.COMPLETED.value,

                now,

                job_id,
            )

    # =====================================================
    # ERROR
    # =====================================================

    async def save_error(
        self,
        job_id: UUID,
        error: str,
    ) -> None:

        pool = self._get_pool()

        now = datetime.now(
            timezone.utc
        )

        async with pool.acquire() as conn:

            await conn.execute(
                """
                UPDATE jobs

                SET
                    error = $1,
                    status = $2,

                    locked_by = NULL,
                    locked_until = NULL,

                    updated_at = $3

                WHERE id = $4
                """,

                error,
                JobStatus.FAILED.value,
                now,
                job_id,
            )

    # =====================================================
    # IDEMPOTENT SIDE EFFECT
    # =====================================================

    async def record_side_effect(
        self,
        job_id: UUID,
        decision: str,
    ) -> bool:

        pool = self._get_pool()

        now = datetime.now(
            timezone.utc
        )

        async with pool.acquire() as conn:

            row = await conn.fetchrow(
                """
                INSERT INTO side_effects (
                    job_id,
                    effect_type,
                    payload,
                    created_at
                )
                VALUES (
                    $1, $2, $3, $4
                )

                ON CONFLICT (
                    job_id,
                    effect_type
                )
                DO NOTHING

                RETURNING id
                """,

                job_id,
                "decision_notification",
                decision,
                now,
            )

        return row is not None

    # =====================================================
    # GET SIDE EFFECTS
    # =====================================================

    async def get_side_effects(
        self,
        job_id: UUID,
    ):

        pool = self._get_pool()

        async with pool.acquire() as conn:

            return await conn.fetch(
                """
                SELECT *
                FROM side_effects
                WHERE job_id = $1
                ORDER BY id
                """,
                job_id,
            )

    # =====================================================
    # DB ROW → DOMAIN
    # =====================================================

    def _row_to_job(
        self,
        row: asyncpg.Record,
    ) -> Job:

        merchant = Merchant(
            id=row["merchant_id"],
            name=row["merchant_name"],
            country=row["merchant_country"],
        )

        transaction = Transaction(
            id=row["transaction_id"],
            user_id=row["user_id"],
            amount=row["amount"],
            currency=Currency(
                row["currency"]
            ),
            merchant=merchant,
            created_at=row[
                "transaction_created_at"
            ],
        )

        result = None

        if row["fraud_score"] is not None:

            result = JobResult(
                fraud_score=row["fraud_score"],
                user_risk=row["user_risk"],
                merchant_risk=row[
                    "merchant_risk"
                ],
                decision=row["decision"],
            )

        return Job(
            id=row["id"],
            transaction=transaction,
            status=JobStatus(
                row["status"]
            ),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            result=result,
            error=row["error"],
        )

    async def claim_next_job(
        self,
        worker_id: str,
        lease_seconds: int,
    ) -> Job | None:

        pool = self._get_pool()

        now = datetime.now(
            timezone.utc
        )

        locked_until = (
            now
            + timedelta(seconds=lease_seconds)
        )

        async with pool.acquire() as conn:

            async with conn.transaction():

                row = await conn.fetchrow(
                    """
                    SELECT *
                    FROM jobs

                    WHERE
                        status = $1

                        OR

                        (
                            status = $2

                            AND (
                                locked_until IS NULL
                                OR locked_until < NOW()
                            )
                        )

                    ORDER BY created_at

                    FOR UPDATE SKIP LOCKED

                    LIMIT 1
                    """,

                    JobStatus.PENDING.value,
                    JobStatus.PROCESSING.value,
                )

                if row is None:
                    return None

                updated_row = await conn.fetchrow(
                    """
                    UPDATE jobs

                    SET
                        status = $1,
                        locked_by = $2,
                        locked_until = $3,
                        updated_at = $4

                    WHERE id = $5

                    RETURNING *
                    """,

                    JobStatus.PROCESSING.value,
                    worker_id,
                    locked_until,
                    now,
                    row["id"],
                )

        return self._row_to_job(updated_row)

    async def save_with_outbox(
        self,
        job: Job,
    ) -> None:

        pool = self._get_pool()

        async with pool.acquire() as conn:

            async with conn.transaction():

                # =================================
                # JOB
                # =================================

                await conn.execute(
                    """
                    INSERT INTO jobs (
                        id,
                        transaction_id,

                        user_id,
                        amount,
                        currency,

                        merchant_id,
                        merchant_name,
                        merchant_country,

                        status,

                        created_at,
                        updated_at,
                        transaction_created_at
                    )
                    VALUES (
                        $1, $2, $3, $4,
                        $5, $6, $7, $8,
                        $9, $10, $11, $12
                    )
                    """,

                    job.id,
                    job.transaction.id,

                    job.transaction.user_id,
                    job.transaction.amount,
                    job.transaction.currency.value,

                    job.transaction.merchant.id,
                    job.transaction.merchant.name,
                    job.transaction.merchant.country,

                    job.status.value,

                    job.created_at,
                    job.updated_at,
                    job.transaction.created_at,
                )

                # =================================
                # OUTBOX EVENT
                # =================================

                event_id = uuid4()

                payload = json.dumps(
                    {
                        "job_id": str(job.id)
                    }
                )

                await conn.execute(
                    """
                    INSERT INTO outbox_events (
                        id,
                        aggregate_id,
                        event_type,
                        payload,
                        created_at
                    )
                    VALUES (
                        $1,
                        $2,
                        $3,
                        $4::jsonb,
                        $5
                    )
                    """,

                    event_id,
                    job.id,
                    "job_created",
                    payload,
                    job.created_at,
                )

    async def get_unpublished_outbox_events(
        self,
        limit: int = 100,
    ):

        pool = self._get_pool()

        async with pool.acquire() as conn:

            return await conn.fetch(
                """
                SELECT
                    id,
                    aggregate_id,
                    event_type,
                    payload,
                    created_at

                FROM outbox_events

                WHERE published_at IS NULL

                ORDER BY created_at

                LIMIT $1
                """,
                limit,
            )

    async def mark_outbox_published(
        self,
        event_id: UUID,
    ) -> None:

        pool = self._get_pool()

        async with pool.acquire() as conn:

            await conn.execute(
                """
                UPDATE outbox_events

                SET published_at = NOW()

                WHERE id = $1
                """,
                event_id,
            )