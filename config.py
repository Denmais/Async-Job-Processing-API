import os


POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://async_user:async_password@localhost:5432/async_jobs",
)

REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://localhost:6379/0",
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

JOBS_TOPIC = "jobs"

JOBS_GROUP_ID = "job-workers"