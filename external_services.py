import asyncio
import random

from dataclasses import dataclass


class TemporaryServiceError(Exception):
    pass


@dataclass
class FraudResult:
    score: float


@dataclass
class UserRiskResult:
    risk: str


@dataclass
class MerchantRiskResult:
    risk: str


async def check_fraud(
    user_id: int,
    amount: float,
) -> FraudResult:

    print(
        f"[fraud] START user={user_id}"
    )

    await asyncio.sleep(10)

    if random.random() < 0.1:
        raise TemporaryServiceError(
            "fraud service unavailable"
        )

    score = random.uniform(
        0.0,
        1.0,
    )

    print(
        f"[fraud] DONE "
        f"user={user_id} "
        f"score={score:.2f}"
    )

    return FraudResult(
        score=score
    )


async def get_user_risk(
    user_id: int,
) -> UserRiskResult:

    print(
        f"[user-risk] START user={user_id}"
    )

    await asyncio.sleep(
        random.uniform(0.4, 1.6)
    )

    if random.random() < 0.1:
        raise TemporaryServiceError(
            "user service unavailable"
        )

    risk = random.choice(
        [
            "low",
            "medium",
            "high",
        ]
    )

    print(
        f"[user-risk] DONE "
        f"user={user_id} "
        f"risk={risk}"
    )

    return UserRiskResult(
        risk=risk
    )


async def get_merchant_risk(
    merchant_id: int,
) -> MerchantRiskResult:

    print(
        f"[merchant-risk] START "
        f"merchant={merchant_id}"
    )

    await asyncio.sleep(
        random.uniform(0.5, 2.0)
    )

    if random.random() < 0.1:
        raise TemporaryServiceError(
            "merchant service unavailable"
        )

    risk = random.choice(
        [
            "low",
            "medium",
            "high",
        ]
    )

    print(
        f"[merchant-risk] DONE "
        f"merchant={merchant_id} "
        f"risk={risk}"
    )

    return MerchantRiskResult(
        risk=risk
    )