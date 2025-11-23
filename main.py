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
import os
from connection_registry import WORKER_ID, cleanup_stale_workers
from Bot.discord_bot import bot_instance
from logger import logger
from redis_client import get_redis_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Perform stale worker cleanup at startup of every worker
    logger.info(f"Starting application with worker ID: {WORKER_ID}")
    cleanup_stale_workers()
    
    # ✅ CRITICAL: Set the main event loop for use with run_coroutine_threadsafe
    # This allows scheduler jobs to execute on the main loop where WebSockets live
    import routes.deviceRegistration as device_registration_module
    try:
        current_loop = asyncio.get_running_loop()
        device_registration_module.main_event_loop = current_loop
        logger.info(f"[SCHEDULER] Main event loop initialized for worker {WORKER_ID}")
    except RuntimeError:
        logger.warning(f"[SCHEDULER] Could not get running event loop during startup")
    
    # Get Redis client for leader election
    redis_client = get_redis_client()
    
    # --- LEADER ELECTION LOGIC ---
    # This key determines who is the active scheduler
    SCHEDULER_LOCK_KEY = "appilot:scheduler:leader"
    SCHEDULER_LOCK_TTL = 30  # seconds - lock expires if leader dies (increased for stability)
    
    # Background task to renew lock if we are leader
    renew_task = None
    
    async def keep_leadership_alive():
        """Continuously renew the scheduler leadership lock"""
        # Renew every 10 seconds (1/3 of TTL) to ensure we renew before expiration
        renewal_interval = 10  # seconds
        
        while True:
            try:
                await asyncio.sleep(renewal_interval)
                
                # ✅ FIXED: Wrap synchronous Redis calls in to_thread to avoid blocking event loop
                current_owner = await asyncio.to_thread(redis_client.get, SCHEDULER_LOCK_KEY)
                current_owner_str = str(current_owner) if current_owner else None
                
                if current_owner_str == str(WORKER_ID):
                    # We still own it - renew the lock by setting it again with expiration
                    await asyncio.to_thread(
                        redis_client.set,
                        SCHEDULER_LOCK_KEY,
                        str(WORKER_ID),
                        ex=SCHEDULER_LOCK_TTL
                    )
                    logger.debug(f"[SCHEDULER] 🔄 Worker {WORKER_ID} renewed scheduler leadership")
                elif current_owner_str is None:
                    # Lock expired - try to reclaim it
                    reclaimed = await asyncio.to_thread(
                        redis_client.set,
                        SCHEDULER_LOCK_KEY,
                        str(WORKER_ID),
                        nx=True,
                        ex=SCHEDULER_LOCK_TTL
                    )
                    if reclaimed:
                        logger.info(f"[SCHEDULER] 🔄 Worker {WORKER_ID} reclaimed scheduler leadership (lock had expired)")
                    else:
                        # Another worker took it
                        new_owner = await asyncio.to_thread(redis_client.get, SCHEDULER_LOCK_KEY)
                        new_owner_str = str(new_owner) if new_owner else "unknown"
                        logger.warning(f"[SCHEDULER] ⚠️ Worker {WORKER_ID} lost leadership (new owner: {new_owner_str}). Stopping scheduler.")
                        if scheduler.running:
                            scheduler.shutdown(wait=False)
                        break
                else:
                    # Another worker owns it
                    logger.warning(f"[SCHEDULER] ⚠️ Worker {WORKER_ID} lost leadership (current owner: {current_owner_str}). Stopping scheduler.")
                    if scheduler.running:
                        scheduler.shutdown(wait=False)
                    break
            except asyncio.CancelledError:
                logger.info(f"[SCHEDULER] Leadership renewal task cancelled for worker {WORKER_ID}")
                break
            except Exception as e:
                logger.error(f"[SCHEDULER] Error renewing leadership: {e}", exc_info=True)
                # Continue trying - don't break on transient errors
                # But wait a bit longer before retrying to avoid hammering Redis
                await asyncio.sleep(1)
    
    # Check if I should be the scheduler using leader election
    # Try to set the key. If it doesn't exist (nx=True), we win the election.
    # We set a short expiration so if this worker dies, another can take over.
    # ✅ FIXED: Wrap synchronous Redis call in to_thread to avoid blocking event loop
    logger.info(f"[SCHEDULER] 🔍 Attempting leader election for worker {WORKER_ID}...")
    try:
        is_leader = await asyncio.to_thread(
            redis_client.set,
            SCHEDULER_LOCK_KEY,
            str(WORKER_ID),
            nx=True,
            ex=SCHEDULER_LOCK_TTL
        )
        logger.info(f"[SCHEDULER] 🔍 Leader election result: is_leader={is_leader} for worker {WORKER_ID}")
    except Exception as election_err:
        logger.error(f"[SCHEDULER] ❌ Leader election failed for worker {WORKER_ID}: {election_err}", exc_info=True)
        is_leader = False
    
    enable_scheduler_env = os.getenv("ENABLE_SCHEDULER", "").lower()
    logger.info(f"[SCHEDULER] 🔍 ENABLE_SCHEDULER environment variable: '{enable_scheduler_env}' (worker {WORKER_ID})")
    
    # Logic:
    # 1. If Env is explicitly True -> Run scheduler (force enable)
    # 2. If Env is explicitly False -> Don't Run (force disable)
    # 3. If Env is unset -> Use Leader Election (Only 1 worker runs it automatically)
    
    should_start_scheduler = False
    
    if enable_scheduler_env in ("true", "1", "yes"):
        should_start_scheduler = True
        logger.info(f"[SCHEDULER] ✅ ENABLE_SCHEDULER={enable_scheduler_env} - Starting scheduler on worker {WORKER_ID} (forced)")
    elif enable_scheduler_env in ("false", "0", "no"):
        should_start_scheduler = False
        logger.info(f"[SCHEDULER] ⏭️ ENABLE_SCHEDULER={enable_scheduler_env} - Skipping scheduler on worker {WORKER_ID} (forced)")
    else:
        # Env not set, use leader election
        logger.info(f"[SCHEDULER] 🔍 ENABLE_SCHEDULER not set, using leader election (worker {WORKER_ID})")
        if is_leader:
            logger.info(f"[SCHEDULER] 👑 Worker {WORKER_ID} elected as Scheduler Leader (auto-election)")
            should_start_scheduler = True
            # Start background task to keep leadership alive
            renew_task = asyncio.create_task(keep_leadership_alive())
            logger.info(f"[SCHEDULER] 🔄 Started leadership renewal task for worker {WORKER_ID}")
        else:
            # ✅ FIXED: Wrap synchronous Redis call in to_thread to avoid blocking event loop
            try:
                current_leader = await asyncio.to_thread(redis_client.get, SCHEDULER_LOCK_KEY)
                current_leader_str = str(current_leader) if current_leader else "unknown"
                logger.info(f"[SCHEDULER] 💤 Worker {WORKER_ID} is a follower (Leader: {current_leader_str})")
            except Exception as leader_check_err:
                logger.error(f"[SCHEDULER] ❌ Failed to check current leader: {leader_check_err}", exc_info=True)
            should_start_scheduler = False
    
    if should_start_scheduler:
        logger.info(f"[SCHEDULER] 🚀 Starting scheduler on worker {WORKER_ID}...")
        logger.info(f"[SCHEDULER] 🔍 Scheduler state before start: running={scheduler.running}, state={getattr(scheduler, 'state', 'unknown')}")
        try:
            scheduler.start()  # Start the scheduler
            logger.info(f"[SCHEDULER] 🔍 Scheduler.start() called successfully for worker {WORKER_ID}")
            
            # Give scheduler a moment to initialize
            await asyncio.sleep(0.1)
            
            # Verify scheduler started successfully
            if scheduler.running:
                logger.info(f"[SCHEDULER] ✅ Scheduler is RUNNING on worker {WORKER_ID}")
            else:
                logger.error(f"[SCHEDULER] ❌ Scheduler failed to start on worker {WORKER_ID}! running={scheduler.running}")
                # Don't proceed with job validation if scheduler didn't start
                should_start_scheduler = False
                # Cancel renewal task if scheduler didn't start
                if renew_task:
                    logger.info(f"[SCHEDULER] 🔄 Cancelling renewal task for worker {WORKER_ID} (scheduler didn't start)")
                    renew_task.cancel()
        except Exception as start_err:
            logger.error(f"[SCHEDULER] ❌ Exception starting scheduler on worker {WORKER_ID}: {start_err}", exc_info=True)
            should_start_scheduler = False
            # Cancel renewal task if scheduler failed to start
            if renew_task:
                logger.info(f"[SCHEDULER] 🔄 Cancelling renewal task for worker {WORKER_ID} (exception during startup)")
                renew_task.cancel()
        
        if should_start_scheduler and scheduler.running:
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
            logger.info(f"[SCHEDULER] Started worker scheduler on {WORKER_ID}")
    else:
        logger.info(f"[SCHEDULER] Skipping scheduler startup on worker {WORKER_ID}")

    asyncio.create_task(bot_instance.start_bot())

    yield

    # Cleanup code for shutdown
    logger.info(f"Shutting down worker {WORKER_ID}...")
    
    # Cancel leadership renewal task if running
    if 'renew_task' in locals() and renew_task and not renew_task.done():
        renew_task.cancel()
        try:
            await renew_task
        except asyncio.CancelledError:
            pass
    
    # Release leadership lock if we held it
    try:
        # ✅ FIXED: Wrap synchronous Redis calls in to_thread to avoid blocking event loop
        current_owner = await asyncio.to_thread(redis_client.get, SCHEDULER_LOCK_KEY)
        current_owner_str = str(current_owner) if current_owner else None
        
        if current_owner_str == str(WORKER_ID):
            await asyncio.to_thread(redis_client.delete, SCHEDULER_LOCK_KEY)
            logger.info(f"[SCHEDULER] 🔓 Worker {WORKER_ID} released scheduler leadership")
    except Exception as cleanup_err:
        logger.warning(f"[SCHEDULER] Error releasing leadership lock: {cleanup_err}")
    
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
