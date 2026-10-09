from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import router

from config import POSTGRES_DSN

from repositories.postgres_jobs import (
    PostgresJobRepository,
)

from services.jobs import JobService


@asynccontextmanager
async def lifespan(
    app: FastAPI,
):

    print("API START")

    repository = PostgresJobRepository(
        dsn=POSTGRES_DSN
    )

    await repository.connect()

    service = JobService(
        repository=repository
    )

    app.state.repository = repository
    app.state.job_service = service

    try:

        yield

    finally:

        await repository.close()

        print("API STOP")


app = FastAPI(
    title="Async Job Processing API",
    lifespan=lifespan,
)

app.include_router(
    router
)