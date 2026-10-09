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

from repositories.postgres_jobs import (
    PostgresJobRepository,
)


class JobService:

    def __init__(
        self,
        repository: PostgresJobRepository,
    ):
        self.repository = repository

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

        # ОДНА Postgres transaction:
        #
        # jobs
        # +
        # outbox_events

        await self.repository.save_with_outbox(
            job
        )

        return job

    async def get_job(
        self,
        job_id: UUID,
    ) -> Job | None:

        return await self.repository.get(
            job_id
        )

    async def get_side_effects(
        self,
        job_id: UUID,
    ):

        return await (
            self.repository
            .get_side_effects(job_id)
        )