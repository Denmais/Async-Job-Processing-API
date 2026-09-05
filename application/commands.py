from dataclasses import dataclass

from domain.enums import Currency


@dataclass
class CreateJobCommand:
    user_id: int
    amount: float
    currency: Currency

    merchant_id: int
    merchant_name: str
    merchant_country: str