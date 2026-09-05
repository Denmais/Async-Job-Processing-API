import asyncio
import aiosqlite

from datetime import datetime, timezone
from uuid import UUID

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


class SqliteJobRepository:

    def __init__(
        self,
        db_path: str = "jobs.db",
    ):
        self.db_path = db_path

        self.db: aiosqlite.Connection | None = None

        self.write_lock = asyncio.Lock()

    async def connect(self) -> None:
        self.db = await aiosqlite.connect(
            self.db_path,
        )

        self.db.row_factory = aiosqlite.Row

        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,

                transaction_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                currency TEXT NOT NULL,

                merchant_id INTEGER NOT NULL,
                merchant_name TEXT NOT NULL,
                merchant_country TEXT NOT NULL,

                status TEXT NOT NULL,

                fraud_score REAL,
                user_risk TEXT,
                merchant_risk TEXT,
                decision TEXT,
                error TEXT,

                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                transaction_created_at TEXT NOT NULL
            )
            """
        )

        await self.db.commit()

    async def close(self) -> None:
        if self.db is not None:
            await self.db.close()

    def _get_db(
        self,
    ) -> aiosqlite.Connection:

        if self.db is None:
            raise RuntimeError(
                "Repository is not connected"
            )

        return self.db

    async def save(
        self,
        job: Job,
    ) -> None:

        db = self._get_db()

        async with self.write_lock:
            await db.execute(
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(job.id),

                    str(job.transaction.id),
                    job.transaction.user_id,
                    job.transaction.amount,
                    job.transaction.currency.value,

                    job.transaction.merchant.id,
                    job.transaction.merchant.name,
                    job.transaction.merchant.country,

                    job.status.value,

                    job.created_at.isoformat(),
                    job.updated_at.isoformat(),
                    job.transaction.created_at.isoformat(),
                ),
            )

            await db.commit()

    async def get(
        self,
        job_id: UUID,
    ) -> Job | None:

        db = self._get_db()

        cursor = await db.execute(
            """
            SELECT *
            FROM jobs
            WHERE id = ?
            """,
            (
                str(job_id),
            ),
        )

        row = await cursor.fetchone()

        await cursor.close()

        if row is None:
            return None

        return self._row_to_job(
            row
        )

    async def update_status(
        self,
        job_id: UUID,
        status: JobStatus,
    ) -> None:

        db = self._get_db()

        now = datetime.now(
            timezone.utc
        )

        async with self.write_lock:
            await db.execute(
                """
                UPDATE jobs
                SET
                    status = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    status.value,
                    now.isoformat(),
                    str(job_id),
                ),
            )

            await db.commit()

    async def save_result(
        self,
        job_id: UUID,
        result: JobResult,
    ) -> None:

        db = self._get_db()

        now = datetime.now(
            timezone.utc
        )

        async with self.write_lock:
            await db.execute(
                """
                UPDATE jobs
                SET
                    fraud_score = ?,
                    user_risk = ?,
                    merchant_risk = ?,
                    decision = ?,

                    error = NULL,

                    status = ?,
                    updated_at = ?

                WHERE id = ?
                """,
                (
                    result.fraud_score,
                    result.user_risk,
                    result.merchant_risk,
                    result.decision,

                    JobStatus.COMPLETED.value,
                    now.isoformat(),

                    str(job_id),
                ),
            )

            await db.commit()

    async def save_error(
        self,
        job_id: UUID,
        error: str,
    ) -> None:

        db = self._get_db()

        now = datetime.now(
            timezone.utc
        )

        async with self.write_lock:
            await db.execute(
                """
                UPDATE jobs
                SET
                    error = ?,
                    status = ?,
                    updated_at = ?

                WHERE id = ?
                """,
                (
                    error,
                    JobStatus.FAILED.value,
                    now.isoformat(),
                    str(job_id),
                ),
            )

            await db.commit()

    def _row_to_job(
        self,
        row: aiosqlite.Row,
    ) -> Job:

        merchant = Merchant(
            id=row["merchant_id"],
            name=row["merchant_name"],
            country=row["merchant_country"],
        )

        transaction = Transaction(
            id=UUID(
                row["transaction_id"]
            ),
            user_id=row["user_id"],
            amount=row["amount"],
            currency=Currency(
                row["currency"]
            ),
            merchant=merchant,
            created_at=datetime.fromisoformat(
                row["transaction_created_at"]
            ),
        )

        result = None

        if row["fraud_score"] is not None:
            result = JobResult(
                fraud_score=row["fraud_score"],
                user_risk=row["user_risk"],
                merchant_risk=row["merchant_risk"],
                decision=row["decision"],
            )

        return Job(
            id=UUID(
                row["id"]
            ),
            transaction=transaction,
            status=JobStatus(
                row["status"]
            ),
            created_at=datetime.fromisoformat(
                row["created_at"]
            ),
            updated_at=datetime.fromisoformat(
                row["updated_at"]
            ),
            result=result,
            error=row["error"],
        )