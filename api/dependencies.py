from fastapi import Request

from services.jobs import JobService


def get_job_service(request: Request) -> JobService:

    return request.app.state.job_service
