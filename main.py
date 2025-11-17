# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import JSONResponse
# from routes.routes import router
# from routes.login import login_router
# from routes.devices import devices_router
# from routes.passwordReset import reset_router
# from routes.deviceRegistration import device_router
# from apscheduler.schedulers.background import BackgroundScheduler
# import uvicorn
# from routes.bots import bots_router
# from routes.tasks import tasks_router
# from Bot.discord_bot import bot_instance
# import asyncio
# from scheduler import scheduler

# app = FastAPI()

# allowed_origins = [
#     "http://localhost:5173",
#     "https://appilot-console.vercel.app/",
#     "https://appilot-console-4v67eq436-abdullahnoor-codes-projects.vercel.app/",
#     "https://appilot-console-git-main-abdullahnoor-codes-projects.vercel.app/"
# ]

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#         allow_methods=["*"],
#     allow_headers=["*"],
#     expose_headers=["Set-Cookie"]
# )

# @app.get("/")
# def index():
#     return JSONResponse(content={"message": "running"}, status_code=200)

# app.include_router(router)
# app.include_router(login_router)
# app.include_router(reset_router)
# app.include_router(devices_router)
# app.include_router(bots_router)
# app.include_router(tasks_router)
# app.include_router(device_router, tags=["Android endpoints"])

# #////////////////////////////////////
# # scheduler = BackgroundScheduler()
# scheduler.start()


# @app.on_event("startup")
# async def startup_event():
#     asyncio.create_task(bot_instance.start_bot())


# # for route in app.routes:
# #     print(f"Route: {route.path}, Methods: {route.methods if hasattr(route, 'methods') else 'WebSocket'}")

# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)


# main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from routes.routes import router
from routes.login import login_router
from routes.devices import devices_router
from routes.passwordReset import reset_router
from routes.deviceRegistration import device_router
import uvicorn
from routes.bots import bots_router
from routes.tasks import tasks_router
from routes.tier import tier_router
from routes.twofa import twofa_router
from scheduler import scheduler
import asyncio
from connection_registry import WORKER_ID, cleanup_stale_workers
from Bot.discord_bot import bot_instance
from logger import logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Perform stale worker cleanup at startup of every worker
    logger.info(f"Starting application with worker ID: {WORKER_ID}")
    cleanup_stale_workers()
    logger.info("Starting scheduler...")
    scheduler.start()  # Start the scheduler
    try:
        reloaded_jobs = scheduler.get_jobs()
        job_count = len(reloaded_jobs)
        
        # Validate all loaded jobs and remove invalid ones
        invalid_jobs = []
        for job in reloaded_jobs:
            try:
                if not callable(job.func):
                    invalid_jobs.append(job.id)
                    logger.error(f"[SCHEDULER] Job {job.id} has invalid function reference")
            except Exception as e:
                invalid_jobs.append(job.id)
                logger.error(f"[SCHEDULER] Job {job.id} validation failed: {e}")
        
        # Remove invalid jobs from scheduler
        for job_id in invalid_jobs:
            try:
                scheduler.remove_job(job_id)
                logger.info(f"[SCHEDULER] Removed invalid job {job_id}")
            except Exception as e:
                logger.warning(f"[SCHEDULER] Could not remove invalid job {job_id}: {e}")
        
        valid_count = job_count - len(invalid_jobs)
        
        if valid_count:
            logger.info(f"[SCHEDULER] Reloaded {valid_count} valid jobs from persistent store")
            if invalid_jobs:
                logger.warning(f"[SCHEDULER] Removed {len(invalid_jobs)} invalid jobs")
            
            # Log first 10 valid jobs
            valid_jobs = [j for j in reloaded_jobs if j.id not in invalid_jobs]
            for job in valid_jobs[:10]:
                logger.info(
                    "[SCHEDULER] Job id=%s next_run=%s trigger=%s",
                    job.id,
                    getattr(job.trigger, "run_date", getattr(job, "next_run_time", None)),
                    job.trigger,
                )
            if valid_count > 10:
                logger.info(
                    "[SCHEDULER] Additional jobs not shown in log: %s",
                    valid_count - 10,
                )
        else:
            logger.info("[SCHEDULER] No valid jobs found in persistent store after startup")
        
        # Cleanup expired/stale jobs (older than 7 days past their run time)
        import pytz
        from datetime import datetime, timedelta
        now = datetime.now(pytz.UTC)
        stale_threshold = now - timedelta(days=7)
        stale_jobs = []
        
        for job in scheduler.get_jobs():
            next_run = job.next_run_time
            # If job has no next run time or next run is more than 7 days in the past, it's stale
            if next_run and next_run < stale_threshold:
                stale_jobs.append(job.id)
                logger.warning(f"[SCHEDULER] Found stale job {job.id} (next_run={next_run})")
        
        # Remove stale jobs
        for job_id in stale_jobs:
            try:
                scheduler.remove_job(job_id)
                logger.info(f"[SCHEDULER] Removed stale job {job_id}")
            except Exception as e:
                logger.warning(f"[SCHEDULER] Could not remove stale job {job_id}: {e}")
        
        if stale_jobs:
            logger.info(f"[SCHEDULER] Cleanup: Removed {len(stale_jobs)} stale jobs")
            
    except Exception as scheduler_log_err:
        logger.warning(f"[SCHEDULER] Could not validate/log persisted jobs: {scheduler_log_err}")
    logger.info("Started worker scheduler")

    asyncio.create_task(bot_instance.start_bot())

    yield

    # Cleanup code for shutdown
    logger.info(f"Shutting down worker {WORKER_ID}...")
    # Add code here to clean up this worker's devices on shutdown
    from connection_registry import cleanup_worker_devices

    cleanup_worker_devices()


app = FastAPI(lifespan=lifespan)


# Add request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Request: {request.method} {request.url.path}")
    response = await call_next(request)
    logger.info(
        f"Response: {request.method} {request.url.path} - Status: {response.status_code}"
    )
    return response


allowed_origins = [
    "http://localhost:5173",
    "https://appilot-console.vercel.app/",
    "https://appilot-console-4v67eq436-abdullahnoor-codes-projects.vercel.app/",
    "https://appilot-console-git-main-abdullahnoor-codes-projects.vercel.app/",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Set-Cookie"],
)


@app.get("/")
def index():
    logger.info("Health check endpoint hit")
    return JSONResponse(content={"message": "running"}, status_code=200)


app.include_router(router, tags=["Signup endpoints"])
app.include_router(login_router, tags=["Login endpoints"])
app.include_router(reset_router, tags=["Reset endpoints"])
app.include_router(devices_router, tags=["devices endpoints"])
app.include_router(bots_router, tags=["bot endpoints"])
app.include_router(tasks_router, tags=["Task endpoints"])
app.include_router(tier_router, prefix="/api/tier", tags=["Tier endpoints"])
app.include_router(twofa_router, tags=["2FA endpoints"])
app.include_router(device_router, tags=["Android endpoints"])

if __name__ == "__main__":
    logger.info("Starting application with Uvicorn...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
