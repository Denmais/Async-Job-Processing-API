from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field

from domain.enums import Currency, JobStatus


class MerchantCreate(BaseModel):
    id: int = Field(gt=0)

    name: str = Field(
        min_length=1,
        max_length=100,
    )

    country: str = Field(
        min_length=2,
        max_length=2,
    )


class TransactionCreate(BaseModel):
    user_id: int = Field(gt=0)

    amount: float = Field(
        gt=0,
        le=1_000_000,
    )

    currency: Currency
    merchant: MerchantCreate


class JobAccepted(BaseModel):
    job_id: UUID
    status: JobStatus
    created_at: datetime
    links: dict[str, str]


class MerchantResponse(BaseModel):
    id: int
    name: str
    country: str


class TransactionResponse(BaseModel):
    id: UUID
    user_id: int
    amount: float
    currency: Currency
    merchant: MerchantResponse
    created_at: datetime


class JobResultResponse(BaseModel):
    fraud_score: float
    user_risk: str
    merchant_risk: str
    decision: str


class JobDetails(BaseModel):
    job_id: UUID
    status: JobStatus

    transaction: TransactionResponse

    result: JobResultResponse | None = None
    error: str | None = None

    created_at: datetime
    updated_at: datetime