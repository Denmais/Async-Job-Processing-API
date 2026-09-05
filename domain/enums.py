from enum import Enum


class Currency(str, Enum):
    EUR = "EUR"
    USD = "USD"
    GBP = "GBP"


class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"