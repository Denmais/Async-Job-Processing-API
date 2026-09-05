from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from domain.enums import Currency, JobStatus


@dataclass
class Merchant:
    id: int
    name: str
    country: str


@dataclass
class Transaction:
    id: UUID
    user_id: int
    amount: float
    currency: Currency
    merchant: Merchant
    created_at: datetime


@dataclass
class JobResult:
    fraud_score: float
    user_risk: str
    merchant_risk: str
    decision: str


@dataclass
class Job:
    id: UUID
    transaction: Transaction
    status: JobStatus
    created_at: datetime
    updated_at: datetime

    result: JobResult | None = None
    error: str | None = None