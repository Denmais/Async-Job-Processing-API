import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import router
from repositories.jobs import SqliteJobRepository
from services.jobs import JobService
from workers.jobs import job_worker


WORKERS_COUNT = 3
EXTERNAL_CONCURRENCY_LIMIT = 4


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("LIFESPAN START")

    repository = SqliteJobRepository(
        db_path="jobs.db"
    )

    await repository.connect()

    queue = asyncio.Queue()

    external_semaphore = asyncio.Semaphore(
        EXTERNAL_CONCURRENCY_LIMIT
    )

    service = JobService(
        repository=repository,
        queue=queue,
    )

    app.state.repository = repository
    app.state.queue = queue
    app.state.job_service = service
    app.state.external_semaphore = external_semaphore

    worker_tasks = [
        asyncio.create_task(
            job_worker(
                worker_id=worker_id,
                queue=queue,
                repository=repository,
                semaphore=external_semaphore,
            )
        )
        for worker_id in range(
            1,
            WORKERS_COUNT + 1
        )
    ]

    print("job_service created:", app.state.job_service)
    print(f"Started {WORKERS_COUNT} workers")

    try:
        yield

    finally:
        print("LIFESPAN STOP")

        for task in worker_tasks:
            task.cancel()

        await asyncio.gather(
            *worker_tasks,
            return_exceptions=True,
        )

        await repository.close()


app = FastAPI(
    title="Async Job Processing API",
    lifespan=lifespan,
)

app.include_router(router)