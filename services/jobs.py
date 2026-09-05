import asyncio

from datetime import datetime, timezone
from uuid import UUID, uuid4

from application.commands import (
    CreateJobCommand,
)
from domain.enums import JobStatus
from domain.models import (
    Job,
    Merchant,
    Transaction,
)
from repositories.jobs import (
    SqliteJobRepository,
)


class JobService:

    def __init__(
        self,
        repository: SqliteJobRepository,
        queue: asyncio.Queue,
    ):
        self.repository = repository
        self.queue = queue

    async def create_job(
        self,
        command: CreateJobCommand,
    ) -> Job:

        merchant = Merchant(
            id=command.merchant_id,
            name=command.merchant_name,
            country=command.merchant_country,
        )

        transaction = Transaction(
            id=uuid4(),
            user_id=command.user_id,
            amount=command.amount,
            currency=command.currency,
            merchant=merchant,
            created_at=datetime.now(
                timezone.utc
            ),
        )

        now = datetime.now(
            timezone.utc
        )

        job = Job(
            id=uuid4(),
            transaction=transaction,
            status=JobStatus.PENDING,
            created_at=now,
            updated_at=now,
        )

        await self.repository.save(
            job
        )

        await self.queue.put(
            job.id
        )

        return job

    async def get_job(self, job_id: UUID) -> Job | None:

        return await self.repository.get(job_id)