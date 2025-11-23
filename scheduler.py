from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.redis import RedisJobStore
from redis_client import REDIS_HOST, REDIS_PORT, REDIS_DB

# Use Redis job store for distributed scheduling (shares job definitions across workers)
# ✅ MULTIPLE WORKERS SUPPORT: Distributed locking in wrapper_for_send_command prevents duplicates
# All workers can run schedulers safely - Redis locks ensure only ONE worker executes each job
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
    "misfire_grace_time": 60,  # Only run missed jobs if they are < 60s late. If server was down for longer, skip old jobs to prevent flood of duplicate executions.
    "coalesce": True,  # Combine multiple missed runs into one
    "max_instances": 1,  # Only allow 1 instance of a specific job ID
    "replace_existing": True,  # Replace duplicate jobs instead of creating new ones
}

# Initialize the scheduler with Redis job store
scheduler = AsyncIOScheduler(
    jobstores=jobstores,
    executors=executors,
    job_defaults=job_defaults
)
