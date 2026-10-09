import asyncio
import time

import asyncpg


DSN = (
    "postgresql://"
    "async_user:"
    "async_password"
    "@localhost:5432/"
    "async_jobs"
)


async def db_task(task_id: int, pool: asyncpg.Pool):

    started = time.perf_counter()

    print(
        f"task-{task_id}: "
        f"want connection"
    )

    async with pool.acquire() as conn:

        acquired = time.perf_counter()

        waited = acquired - started

        pid = await conn.fetchval(
            "SELECT pg_backend_pid()"
        )

        print(
            f"task-{task_id}: "
            f"ACQUIRED "
            f"pid={pid}, "
            f"waited={waited:.2f}s"
        )

        # Специально держим connection занятым.
        #
        # pg_sleep происходит внутри PostgreSQL.
        await conn.execute(
            "SELECT pg_sleep(2)"
        )

    finished = time.perf_counter()

    print(
        f"task-{task_id}: "
        f"RELEASED "
        f"total={finished - started:.2f}s"
    )


async def main():

    pool = await asyncpg.create_pool(
        dsn=DSN,

        # Специально только 2.
        min_size=2,
        max_size=2,
    )

    try:

        tasks = [
            asyncio.create_task(
                db_task(
                    task_id=i,
                    pool=pool,
                )
            )
            for i in range(1, 11)
        ]

        await asyncio.gather(
            *tasks
        )

    finally:

        await pool.close()


asyncio.run(main())