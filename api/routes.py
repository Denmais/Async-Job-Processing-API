from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
)

from api.dependencies import (
    get_job_service,
)
from api.schemas import (
    JobAccepted,
    JobDetails,
    JobResultResponse,
    MerchantResponse,
    TransactionCreate,
    TransactionResponse,
)
from application.commands import (
    CreateJobCommand,
)
from services.jobs import JobService


router = APIRouter()


@router.post(
    "/jobs",
    response_model=JobAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_job(
    payload: TransactionCreate,

    service: JobService = Depends(
        get_job_service
    ),
):

    command = CreateJobCommand(
        user_id=payload.user_id,
        amount=payload.amount,
        currency=payload.currency,

        merchant_id=payload.merchant.id,
        merchant_name=payload.merchant.name,
        merchant_country=payload.merchant.country,
    )

    job = await service.create_job(
        command
    )

    return JobAccepted(
        job_id=job.id,
        status=job.status,
        created_at=job.created_at,

        links={
            "status": f"/jobs/{job.id}",
        },
    )


@router.get(
    "/jobs/{job_id}",
    response_model=JobDetails,
)
async def get_job(
    job_id: UUID,

    service: JobService = Depends(
        get_job_service
    ),
):

    job = await service.get_job(
        job_id
    )

    if job is None:
        raise HTTPException(
            status_code=404,
            detail="Job not found",
        )

    result = None

    if job.result is not None:
        result = JobResultResponse(
            fraud_score=job.result.fraud_score,
            user_risk=job.result.user_risk,
            merchant_risk=job.result.merchant_risk,
            decision=job.result.decision,
        )

    return JobDetails(
        job_id=job.id,
        status=job.status,

        transaction=TransactionResponse(
            id=job.transaction.id,
            user_id=job.transaction.user_id,
            amount=job.transaction.amount,
            currency=job.transaction.currency,

            merchant=MerchantResponse(
                id=job.transaction.merchant.id,
                name=job.transaction.merchant.name,
                country=job.transaction.merchant.country,
            ),

            created_at=(
                job.transaction.created_at
            ),
        ),

        result=result,
        error=job.error,

        created_at=job.created_at,
        updated_at=job.updated_at,
    )