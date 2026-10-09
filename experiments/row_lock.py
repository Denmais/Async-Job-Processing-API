import asyncio

import asyncpg


DSN = (
    "postgresql://"
    "async_user:"
    "async_password"
    "@localhost:5432/"
    "async_jobs"
)


# =========================================================
# PREPARE
# =========================================================

async def prepare(
    pool: asyncpg.Pool,
):

    async with pool.acquire() as conn:

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id BIGINT PRIMARY KEY,
                balance BIGINT NOT NULL
            )
            """
        )


async def reset_balance(
    pool: asyncpg.Pool,
):

    async with pool.acquire() as conn:

        await conn.execute(
            """
            INSERT INTO accounts (
                id,
                balance
            )
            VALUES (
                1,
                100
            )

            ON CONFLICT (id)
            DO UPDATE
            SET balance = 100
            """
        )


async def get_balance(
    pool: asyncpg.Pool,
) -> int:

    async with pool.acquire() as conn:

        return await conn.fetchval(
            """
            SELECT balance
            FROM accounts
            WHERE id = 1
            """
        )


# =========================================================
# BAD VERSION
#
# SELECT
# Python calculation
# UPDATE
# =========================================================

async def naive_add_money(
    name: str,
    pool: asyncpg.Pool,
    amount: int,
):

    async with pool.acquire() as conn:

        async with conn.transaction():

            print(
                f"{name}: BEGIN"
            )

            balance = await conn.fetchval(
                """
                SELECT balance
                FROM accounts
                WHERE id = 1
                """
            )

            print(
                f"{name}: "
                f"read balance={balance}"
            )

            # Специально создаём окно,
            # чтобы другая transaction
            # тоже успела прочитать balance.
            await asyncio.sleep(1)

            new_balance = (
                balance + amount
            )

            print(
                f"{name}: "
                f"calculated "
                f"{balance} + {amount} "
                f"= {new_balance}"
            )

            await conn.execute(
                """
                UPDATE accounts
                SET balance = $1
                WHERE id = 1
                """,
                new_balance,
            )

            print(
                f"{name}: "
                f"UPDATE balance="
                f"{new_balance}"
            )

        print(
            f"{name}: COMMIT"
        )


# =========================================================
# GOOD VERSION
#
# SELECT FOR UPDATE
# =========================================================

async def locked_add_money(
    name: str,
    pool: asyncpg.Pool,
    amount: int,
):

    async with pool.acquire() as conn:

        async with conn.transaction():

            print(
                f"{name}: BEGIN"
            )

            print(
                f"{name}: "
                f"trying SELECT FOR UPDATE"
            )

            balance = await conn.fetchval(
                """
                SELECT balance
                FROM accounts
                WHERE id = 1
                FOR UPDATE
                """
            )

            print(
                f"{name}: "
                f"LOCK ACQUIRED, "
                f"balance={balance}"
            )

            await asyncio.sleep(1)

            new_balance = (
                balance + amount
            )

            await conn.execute(
                """
                UPDATE accounts
                SET balance = $1
                WHERE id = 1
                """,
                new_balance,
            )

            print(
                f"{name}: "
                f"UPDATE balance="
                f"{new_balance}"
            )

        print(
            f"{name}: COMMIT"
        )


# =========================================================
# EVEN SIMPLER
#
# atomic UPDATE
# =========================================================

async def atomic_add_money(
    name: str,
    pool: asyncpg.Pool,
    amount: int,
):

    async with pool.acquire() as conn:

        async with conn.transaction():

            print(
                f"{name}: atomic UPDATE"
            )

            await conn.execute(
                """
                UPDATE accounts
                SET balance = balance + $1
                WHERE id = 1
                """,
                amount,
            )

            print(
                f"{name}: done"
            )


# =========================================================
# MAIN
# =========================================================

async def main():

    pool = await asyncpg.create_pool(
        dsn=DSN,
        min_size=2,
        max_size=2,
    )

    try:

        await prepare(
            pool
        )

        # =====================================
        # TEST 1
        # LOST UPDATE
        # =====================================

        print()
        print(
            "=============================="
        )
        print(
            "TEST 1: WITHOUT LOCK"
        )
        print(
            "=============================="
        )

        await reset_balance(
            pool
        )

        print(
            "START BALANCE = 100"
        )

        await asyncio.gather(

            naive_add_money(
                "transaction-A",
                pool,
                100,
            ),

            naive_add_money(
                "transaction-B",
                pool,
                100,
            ),
        )

        balance = await get_balance(
            pool
        )

        print(
            f"FINAL BALANCE = {balance}"
        )

        # =====================================
        # TEST 2
        # SELECT FOR UPDATE
        # =====================================

        print()
        print(
            "=============================="
        )
        print(
            "TEST 2: SELECT FOR UPDATE"
        )
        print(
            "=============================="
        )

        await reset_balance(
            pool
        )

        print(
            "START BALANCE = 100"
        )

        await asyncio.gather(

            locked_add_money(
                "transaction-A",
                pool,
                100,
            ),

            locked_add_money(
                "transaction-B",
                pool,
                100,
            ),
        )

        balance = await get_balance(
            pool
        )

        print(
            f"FINAL BALANCE = {balance}"
        )

        # =====================================
        # TEST 3
        # ATOMIC UPDATE
        # =====================================

        print()
        print(
            "=============================="
        )
        print(
            "TEST 3: ATOMIC UPDATE"
        )
        print(
            "=============================="
        )

        await reset_balance(
            pool
        )

        print(
            "START BALANCE = 100"
        )

        await asyncio.gather(

            atomic_add_money(
                "transaction-A",
                pool,
                100,
            ),

            atomic_add_money(
                "transaction-B",
                pool,
                100,
            ),
        )

        balance = await get_balance(
            pool
        )

        print(
            f"FINAL BALANCE = {balance}"
        )

    finally:

        await pool.close()


asyncio.run(main())