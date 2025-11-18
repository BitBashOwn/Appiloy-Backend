# import sys

# # Use pysqlite3 if _sqlite3 is not available (for Python compiled without SQLite)
# try:
#     import _sqlite3
# except ImportError:
#     import pysqlite3 as sqlite3
#     sys.modules['sqlite3'] = sqlite3

from apscheduler.schedulers.asyncio import AsyncIOScheduler
# from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

# # Add persistent job store using local SQLite file
# jobstores = {
#     "default": SQLAlchemyJobStore(url='sqlite:///scheduler_jobs.db')
# }

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

# Initialize the scheduler with the correct configuration (using default in-memory jobstore)
scheduler = AsyncIOScheduler(
    # jobstores=jobstores,  # Removed - using default in-memory jobstore instead of SQLite
    executors=executors,
    job_defaults=job_defaults
)
