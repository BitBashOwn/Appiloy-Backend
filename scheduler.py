from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.redis import RedisJobStore
from redis_client import REDIS_HOST, REDIS_PORT, REDIS_DB

# Use Redis job store for distributed scheduling (prevents duplicate job execution)
jobstores = {
    "default": RedisJobStore(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB
    )
}

# Define executors as a dictionary with APScheduler's built-in configuration
executors = {
    "default": {"type": "threadpool", "max_workers": 20}  # Correct configuration
}

# Define job defaults (optional)
job_defaults = {
    "misfire_grace_time": 300,  # Allow jobs to execute up to 5 minutes late
    "coalesce": False,  # Don't combine missed executions
    "max_instances": 10,  # Allow many instances of the same job to run concurrently
}

# Initialize the scheduler with Redis job store
scheduler = AsyncIOScheduler(
    jobstores=jobstores,
    executors=executors,
    job_defaults=job_defaults
)
