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
import json
import time
from connection_registry import WORKER_ID, cleanup_stale_workers
from Bot.discord_bot import bot_instance
from logger import logger
from redis_client import get_redis_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Perform stale worker cleanup at startup of every worker
    logger.info(f"Starting application with worker ID: {WORKER_ID}")
    await cleanup_stale_workers()
    
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
    monitor_task = None
    
    async def keep_leadership_alive():
        """Continuously renew the scheduler leadership lock"""
        # Renew every 10 seconds (1/3 of TTL) to ensure we renew before expiration
        renewal_interval = 10  # seconds
        consecutive_failures = 0
        max_consecutive_failures = 3
        
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
                    consecutive_failures = 0  # Reset failure counter on success
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
                        consecutive_failures = 0
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
                consecutive_failures += 1
                logger.error(f"[SCHEDULER] Error renewing leadership (failure {consecutive_failures}/{max_consecutive_failures}): {e}", exc_info=True)
                
                # If too many consecutive failures, assume we lost connection and give up leadership
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"[SCHEDULER] ❌ Worker {WORKER_ID} giving up leadership after {consecutive_failures} consecutive failures")
                    if scheduler.running:
                        scheduler.shutdown(wait=False)
                    break
                
                # Wait a bit longer before retrying to avoid hammering Redis
                await asyncio.sleep(2)
    
    async def monitor_and_promote_if_orphaned():
        """Follower task: Monitor for orphaned locks and promote self to leader if needed"""
        check_interval = 15  # Check every 15 seconds
        promotion_wait = 5  # Wait 5 seconds before attempting promotion to avoid race
        
        while True:
            try:
                await asyncio.sleep(check_interval)
                
                # Check if there's a current leader
                current_owner = await asyncio.to_thread(redis_client.get, SCHEDULER_LOCK_KEY)
                current_owner_str = str(current_owner) if current_owner else None
                
                if current_owner_str is None:
                    # No leader exists - this is an ORPHANED state
                    logger.warning(f"[SCHEDULER] ⚠️ Worker {WORKER_ID} detected ORPHANED lock (no leader). Waiting {promotion_wait}s before promotion attempt...")
                    
                    # Wait a bit to let other workers detect this too (avoid thundering herd)
                    await asyncio.sleep(promotion_wait)
                    
                    # Try to become leader
                    promoted = await asyncio.to_thread(
                        redis_client.set,
                        SCHEDULER_LOCK_KEY,
                        str(WORKER_ID),
                        nx=True,
                        ex=SCHEDULER_LOCK_TTL
                    )
                    
                    if promoted:
                        logger.info(f"[SCHEDULER] 👑🚨 Worker {WORKER_ID} PROMOTED to leader after detecting orphaned lock!")
                        
                        # Start the scheduler if not running
                        if not scheduler.running:
                            try:
                                scheduler.start()
                                logger.info(f"[SCHEDULER] ✅ Scheduler started on promoted worker {WORKER_ID}")
                            except Exception as start_err:
                                logger.error(f"[SCHEDULER] ❌ Failed to start scheduler on promotion: {start_err}", exc_info=True)
                                # Release the lock if we can't start the scheduler
                                await asyncio.to_thread(redis_client.delete, SCHEDULER_LOCK_KEY)
                                continue
                        
                        # Start renewal task
                        nonlocal renew_task
                        if renew_task is None or renew_task.done():
                            renew_task = asyncio.create_task(keep_leadership_alive())
                            logger.info(f"[SCHEDULER] 🔄 Started leadership renewal task for promoted worker {WORKER_ID}")
                        
                        # Start queue processor task
                        queue_task = asyncio.create_task(process_pending_schedules())
                        logger.info(f"[QUEUE] 🔄 Started queue processor task for promoted worker {WORKER_ID}")
                        
                        # Stop monitoring since we're now the leader
                        break
                    else:
                        # Another worker got promoted first
                        new_owner = await asyncio.to_thread(redis_client.get, SCHEDULER_LOCK_KEY)
                        logger.info(f"[SCHEDULER] ℹ️ Worker {WORKER_ID} promotion failed - another worker became leader: {new_owner}")
                else:
                    # Leader exists, continue monitoring
                    logger.debug(f"[SCHEDULER] 👀 Worker {WORKER_ID} monitoring - current leader: {current_owner_str}")
                    
            except asyncio.CancelledError:
                logger.info(f"[SCHEDULER] Monitor task cancelled for worker {WORKER_ID}")
                break
            except Exception as e:
                logger.error(f"[SCHEDULER] Error in monitor task: {e}", exc_info=True)
                # Continue monitoring despite errors
                await asyncio.sleep(5)
    
    async def process_pending_schedules():
        """Leader task: Process pending job requests from Redis queue and schedule them"""
        QUEUE_KEY = "appilot:pending_schedules"
        QUEUE_MAX_SIZE = 1000
        QUEUE_ITEM_TTL = 3600  # 1 hour
        process_interval = 10  # Process queue every 10 seconds
        
        logger.info(f"[QUEUE] 🚀 Started queue processor on leader worker {WORKER_ID}")
        
        while True:
            try:
                await asyncio.sleep(process_interval)
                
                # Edge case: Handle Redis unavailable during leader check
                try:
                    # Verify we're still the leader before processing
                    current_owner = await asyncio.to_thread(redis_client.get, SCHEDULER_LOCK_KEY)
                    current_owner_str = str(current_owner) if current_owner else None
                    
                    if current_owner_str != str(WORKER_ID):
                        logger.warning(f"[QUEUE] ⚠️ Worker {WORKER_ID} lost leadership, stopping queue processor")
                        break
                except Exception as redis_check_err:
                    logger.error(f"[QUEUE] ❌ Redis unavailable during leader check: {redis_check_err}")
                    # Wait before retrying to avoid hammering Redis
                    await asyncio.sleep(5)
                    continue
                
                # Get queue size for monitoring
                try:
                    queue_size = await asyncio.to_thread(redis_client.llen, QUEUE_KEY)
                except Exception as queue_size_err:
                    logger.error(f"[QUEUE] ❌ Failed to get queue size (Redis unavailable?): {queue_size_err}")
                    await asyncio.sleep(5)
                    continue
                
                if queue_size == 0:
                    logger.debug(f"[QUEUE] 📭 Queue is empty, nothing to process")
                    continue
                
                # Edge case: Warn if queue is growing too large
                if queue_size > 500:
                    logger.warning(f"[QUEUE] ⚠️ Queue size is large ({queue_size} items) - may indicate slow processing or high load")
                
                logger.info(f"[QUEUE] 📬 Processing {queue_size} pending job requests...")
                
                # Process all pending requests (batch processing)
                processed_count = 0
                failed_count = 0
                
                # Edge case: Limit batch size to prevent long processing cycles
                MAX_BATCH_SIZE = 100
                batch_count = 0
                
                while batch_count < MAX_BATCH_SIZE:
                    batch_count += 1
                    
                    # Pop one item from the queue (right end - FIFO order)
                    try:
                        queue_item_raw = await asyncio.to_thread(redis_client.rpop, QUEUE_KEY)
                    except Exception as redis_pop_err:
                        logger.error(f"[QUEUE] ❌ Redis unavailable during pop: {redis_pop_err}")
                        break  # Exit batch processing, will retry in next cycle
                    
                    if not queue_item_raw:
                        # Queue is empty
                        break
                    
                    try:
                        # Parse the job request
                        import json
                        job_request = json.loads(queue_item_raw)
                        
                        # Validate job request structure
                        required_fields = ["job_id", "trigger_time_utc", "job_name"]
                        if not all(field in job_request for field in required_fields):
                            logger.error(f"[QUEUE] ❌ Invalid job request (missing fields): {job_request.keys()}")
                            failed_count += 1
                            continue
                        
                        # Check if job already exists (avoid duplicates)
                        existing_job = scheduler.get_job(job_request["job_id"])
                        if existing_job:
                            logger.warning(f"[QUEUE] ⚠️ Job {job_request['job_id']} already exists, skipping")
                            processed_count += 1
                            continue
                        
                        # Parse trigger time
                        from datetime import datetime
                        import pytz
                        trigger_time = datetime.fromisoformat(job_request["trigger_time_utc"])
                        if trigger_time.tzinfo is None:
                            trigger_time = pytz.UTC.localize(trigger_time)
                        
                        # Check if trigger time is in the past (with 5-minute grace period)
                        now_utc = datetime.now(pytz.UTC)
                        if trigger_time < now_utc - timedelta(minutes=5):
                            logger.warning(f"[QUEUE] ⏰ Job {job_request['job_id']} trigger time is in the past ({trigger_time}), skipping")
                            failed_count += 1
                            continue
                        
                        # Determine job type and schedule accordingly
                        job_type = job_request.get("job_type", "command")
                        
                        from apscheduler.triggers.date import DateTrigger
                        
                        if job_type == "weekly_reminder":
                            # Weekly reminder job - uses send_weekly_reminder function
                            from routes.deviceRegistration import send_weekly_reminder
                            
                            cmd = job_request.get("command", {})
                            scheduler.add_job(
                                send_weekly_reminder,
                                trigger=DateTrigger(run_date=trigger_time, timezone=pytz.UTC),
                                args=[
                                    cmd.get("task_id"),
                                    cmd.get("day_name"),
                                    cmd.get("start_time"),
                                    cmd.get("schedule_lines"),
                                    cmd.get("time_zone"),
                                    False,
                                    "5 Hours Before Start",
                                ],
                                id=job_request["job_id"],
                                name=job_request["job_name"],
                                replace_existing=True,
                            )
                            logger.info(f"[QUEUE] ✅ Scheduled reminder job {job_request['job_id']} for {trigger_time}")
                        else:
                            # Standard command job - uses wrapper_for_send_command
                            from routes.deviceRegistration import wrapper_for_send_command
                            
                            scheduler.add_job(
                                wrapper_for_send_command,
                                trigger=DateTrigger(run_date=trigger_time, timezone=pytz.UTC),
                                args=[job_request["device_ids"], job_request["command"]],
                                id=job_request["job_id"],
                                name=job_request["job_name"],
                            )
                            logger.info(f"[QUEUE] ✅ Scheduled command job {job_request['job_id']} for {trigger_time}")
                        
                        processed_count += 1
                        
                    except json.JSONDecodeError as json_err:
                        logger.error(f"[QUEUE] ❌ Failed to parse job request JSON: {json_err}")
                        failed_count += 1
                    except Exception as job_err:
                        logger.error(f"[QUEUE] ❌ Failed to schedule job from queue: {job_err}", exc_info=True)
                        failed_count += 1
                
                logger.info(f"[QUEUE] 📊 Batch complete: {processed_count} scheduled, {failed_count} failed")
                
            except asyncio.CancelledError:
                logger.info(f"[QUEUE] Queue processor task cancelled for worker {WORKER_ID}")
                break
            except Exception as e:
                logger.error(f"[QUEUE] ❌ Error in queue processor: {e}", exc_info=True)
                # Continue processing despite errors (wait before retrying)
                await asyncio.sleep(5)
    
    # Add small random delay to prevent all workers from starting simultaneously
    # This helps avoid race conditions during leader election
    import random
    startup_delay = random.uniform(0.1, 1.0)
    logger.info(f"[SCHEDULER] ⏱️ Worker {WORKER_ID} waiting {startup_delay:.2f}s before election (startup jitter)")
    await asyncio.sleep(startup_delay)
    
    # Check if I should be the scheduler using leader election
    # Try to set the key. If it doesn't exist (nx=True), we win the election.
    # We set a short expiration so if this worker dies, another can take over.
    # ✅ FIXED: Wrap synchronous Redis call in to_thread to avoid blocking event loop
    logger.info(f"[SCHEDULER] 🔍 Attempting leader election for worker {WORKER_ID}...")
    
    is_leader = False
    max_election_retries = 3
    
    for attempt in range(1, max_election_retries + 1):
        try:
            is_leader = await asyncio.to_thread(
                redis_client.set,
                SCHEDULER_LOCK_KEY,
                str(WORKER_ID),
                nx=True,
                ex=SCHEDULER_LOCK_TTL
            )
            logger.info(f"[SCHEDULER] 🔍 Leader election result: is_leader={is_leader} for worker {WORKER_ID}")
            break  # Success, exit retry loop
        except Exception as election_err:
            logger.error(f"[SCHEDULER] ❌ Leader election attempt {attempt}/{max_election_retries} failed for worker {WORKER_ID}: {election_err}", exc_info=True)
            
            if attempt < max_election_retries:
                retry_delay = 2 * attempt  # Exponential backoff
                logger.info(f"[SCHEDULER] 🔄 Retrying election in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"[SCHEDULER] ❌ All election attempts failed for worker {WORKER_ID}. Will rely on follower promotion if needed.")
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
            # Start background task to process pending job queue
            queue_task = asyncio.create_task(process_pending_schedules())
            logger.info(f"[QUEUE] 🔄 Started queue processor task for leader worker {WORKER_ID}")
        else:
            # ✅ FIXED: Wrap synchronous Redis call in to_thread to avoid blocking event loop
            try:
                current_leader = await asyncio.to_thread(redis_client.get, SCHEDULER_LOCK_KEY)
                current_leader_str = str(current_leader) if current_leader else "unknown"
                
                if current_leader_str == "unknown" or current_leader_str is None or current_leader_str == "None":
                    logger.warning(f"[SCHEDULER] ⚠️ Worker {WORKER_ID} is a follower but NO LEADER EXISTS (orphaned state detected at startup)!")
                else:
                    logger.info(f"[SCHEDULER] 💤 Worker {WORKER_ID} is a follower (Leader: {current_leader_str})")
                
                # Start monitor task for all followers to handle orphaned locks
                monitor_task = asyncio.create_task(monitor_and_promote_if_orphaned())
                logger.info(f"[SCHEDULER] 👀 Started follower monitor task for worker {WORKER_ID} (will auto-promote if leader fails)")
                
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

    # Background task for device health monitoring
    health_monitor_task = None
    
    async def monitor_device_health():
        """Periodically ping all connected devices and mark offline if no response"""
        PING_INTERVAL = 30  # Send ping every 30 seconds
        PING_TIMEOUT = 15   # INCREASED: Wait 15 seconds for pong response
        MAX_MISSED_PONGS = 3  # NEW: Allow 3 missed pongs before marking offline
        ACTIVITY_GRACE_PERIOD = 180  # NEW: 3 minutes of no activity before considering stale
        
        # Track missed pongs per device
        missed_pong_count = {}
        
        while True:
            try:
                await asyncio.sleep(PING_INTERVAL)
                
                # Get all devices connected to this worker
                from connection_registry import get_devices_for_this_worker, mark_device_offline, track_connection_stability
                from routes.deviceRegistration import device_connections, connection_metadata, active_connections
                
                device_ids = await get_devices_for_this_worker()
                
                if not device_ids:
                    continue
                
                # Send ping to each device
                ping_tasks = []
                for device_id in device_ids:
                    websocket = device_connections.get(device_id)
                    if not websocket or device_id not in connection_metadata:
                        continue
                    
                    metadata = connection_metadata[device_id]
                    last_activity = metadata.get("last_activity", 0)
                    time_since_activity = time.time() - last_activity
                    
                    # NEW: Check if device has recent activity before sending ping
                    if time_since_activity < ACTIVITY_GRACE_PERIOD:
                        # Device is active, reset missed pong count
                        missed_pong_count[device_id] = 0
                        metadata["ping_response_pending"] = False
                        logger.debug(f"[HEALTH] Device {device_id} has recent activity ({time_since_activity:.0f}s), skipping ping")
                        continue
                    
                    # Send ping
                    metadata["last_ping_sent"] = time.time()
                    metadata["ping_response_pending"] = True
                    
                    async def send_ping(ws, dev_id):
                        try:
                            ping_message = {
                                "type": "ping",
                                "timestamp": int(time.time() * 1000),
                            }
                            await ws.send_text(json.dumps(ping_message))
                            logger.debug(f"[HEALTH] Sent ping to device {dev_id}")
                        except Exception as send_err:
                            # If we cannot send the ping, log and skip marking offline
                            logger.error(f"[HEALTH] Failed to send ping to device {dev_id}: {send_err}")
                            if dev_id in connection_metadata:
                                connection_metadata[dev_id]["ping_response_pending"] = False
                            # Increment missed count but don't mark offline immediately
                            missed_pong_count[dev_id] = missed_pong_count.get(dev_id, 0) + 1
                            return
                        
                        # Wait for pong response
                        await asyncio.sleep(PING_TIMEOUT)
                        
                        # Check if we got a response
                        if dev_id in connection_metadata:
                            metadata = connection_metadata[dev_id]
                            
                            # If the websocket listener already cleared the flag, treat as success
                            if not metadata.get("ping_response_pending", False):
                                missed_pong_count[dev_id] = 0
                                logger.debug(f"[HEALTH] Device {dev_id} answered ping (flag cleared)")
                                return
                            
                            current_activity = metadata.get("last_activity", 0)
                            last_ping_sent = metadata.get("last_ping_sent", 0)
                            
                            # Check if there was ANY activity since we sent the ping
                            if current_activity > last_ping_sent:
                                # Device communicated, consider it alive
                                metadata["ping_response_pending"] = False
                                missed_pong_count[dev_id] = 0
                                logger.debug(f"[HEALTH] Device {dev_id} showed activity after ping")
                                return
                            
                            if metadata.get("ping_response_pending", False):
                                # No response to ping
                                missed_pong_count[dev_id] = missed_pong_count.get(dev_id, 0) + 1
                                count = missed_pong_count[dev_id]
                                
                                # Track ping failure for stability monitoring
                                await track_connection_stability(dev_id, "ping_failure")
                                
                                if count >= MAX_MISSED_PONGS:
                                    logger.warning(f"[HEALTH] Device {dev_id} missed {count} pongs - marking offline")
                                    await mark_device_offline(dev_id)
                                    device_connections.pop(dev_id, None)
                                    connection_metadata.pop(dev_id, None)
                                    if ws in active_connections:
                                        active_connections.remove(ws)
                                    try:
                                        await ws.close(code=1000, reason="Health check failed - no ping response")
                                        logger.debug(f"[HEALTH] Closed WebSocket for device {dev_id}")
                                    except Exception as close_err:
                                        logger.warning(f"[HEALTH] Error closing WebSocket for {dev_id}: {close_err}")
                                    missed_pong_count.pop(dev_id, None)
                                else:
                                    logger.info(f"[HEALTH] Device {dev_id} missed pong ({count}/{MAX_MISSED_PONGS})")
                                    metadata["ping_response_pending"] = False  # Reset for next cycle
                    
                    ping_tasks.append(send_ping(websocket, device_id))
                
                # Send all pings concurrently
                if ping_tasks:
                    await asyncio.gather(*ping_tasks, return_exceptions=True)
                    
            except asyncio.CancelledError:
                logger.info("[HEALTH] Device health monitor task cancelled")
                break
            except Exception as e:
                logger.error(f"[HEALTH] Error in device health monitor: {e}")
                await asyncio.sleep(5)  # Wait before retrying
    
    # Start health monitoring task
    health_monitor_task = asyncio.create_task(monitor_device_health())
    logger.info("[HEALTH] Device health monitoring task started")

    # Background task for efficient cleanup of expired pending commands
    async def cleanup_expired_commands_task():
        """Periodically clean up expired pending commands"""
        from connection_registry import cleanup_expired_commands_efficient
        
        while True:
            try:
                await asyncio.sleep(300)  # Run every 5 minutes
                await cleanup_expired_commands_efficient()
            except asyncio.CancelledError:
                logger.info("[CLEANUP] Expired commands cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"[CLEANUP] Error in expired commands cleanup: {e}")
                await asyncio.sleep(60)  # Wait before retrying
    
    cleanup_task = asyncio.create_task(cleanup_expired_commands_task())
    logger.info("[CLEANUP] Expired commands cleanup task started")
    
    # Background task to listen for reconnection events and process queued commands
    async def reconnection_listener_task():
        """Listen for device reconnection events and process queued commands"""
        from connection_registry import (
            RECONNECTION_CHANNEL,
            process_pending_commands_for_device,
            redis_client
        )
        import json
        
        pubsub = redis_client.pubsub()
        await asyncio.to_thread(pubsub.subscribe, RECONNECTION_CHANNEL)
        
        logger.info("[RECONNECT] Reconnection listener task started")
        
        try:
            while True:
                try:
                    message = await asyncio.to_thread(
                        pubsub.get_message,
                        ignore_subscribe_messages=True,
                        timeout=1.0
                    )
                    
                    if message and message.get("type") == "message":
                        try:
                            data = json.loads(message["data"])
                            device_id = data.get("device_id")
                            
                            if device_id:
                                # Process pending commands for reconnected device
                                processed = await process_pending_commands_for_device(device_id)
                                if processed > 0:
                                    logger.info(f"[RECONNECT] Processed {processed} queued commands for device {device_id}")
                        except (json.JSONDecodeError, KeyError) as e:
                            logger.warning(f"[RECONNECT] Invalid reconnection message: {e}")
                        except Exception as e:
                            logger.error(f"[RECONNECT] Error processing reconnection event: {e}")
                            
                except Exception as e:
                    logger.error(f"[RECONNECT] Error in reconnection listener: {e}")
                    await asyncio.sleep(5)  # Wait before retrying
                    
        except asyncio.CancelledError:
            logger.info("[RECONNECT] Reconnection listener task cancelled")
        finally:
            await asyncio.to_thread(pubsub.unsubscribe, RECONNECTION_CHANNEL)
            await asyncio.to_thread(pubsub.close)
    
    reconnection_listener_task_instance = asyncio.create_task(reconnection_listener_task())
    logger.info("[RECONNECT] Reconnection listener task started")

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
    
    # Cancel monitor task if running
    if 'monitor_task' in locals() and monitor_task and not monitor_task.done():
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
    
    # Cancel health monitor task if running
    if 'health_monitor_task' in locals() and health_monitor_task and not health_monitor_task.done():
        health_monitor_task.cancel()
        try:
            await health_monitor_task
        except asyncio.CancelledError:
            pass
    
    # Cancel cleanup task if running
    if 'cleanup_task' in locals() and cleanup_task and not cleanup_task.done():
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass
    
    # Cancel reconnection listener task if running
    if 'reconnection_listener_task_instance' in locals() and reconnection_listener_task_instance and not reconnection_listener_task_instance.done():
        reconnection_listener_task_instance.cancel()
        try:
            await reconnection_listener_task_instance
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
    await cleanup_worker_devices()


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


@app.get("/health/scheduler")
async def scheduler_health():
    """
    Health check endpoint to verify scheduler leader status
    Returns info about current worker and scheduler state
    """
    from redis_client import get_redis_client
    from datetime import datetime
    import asyncio
    
    redis_client = get_redis_client()
    SCHEDULER_LOCK_KEY = "appilot:scheduler:leader"
    QUEUE_KEY = "appilot:pending_schedules"
    
    try:
        # Get current leader from Redis
        current_leader = await asyncio.to_thread(redis_client.get, SCHEDULER_LOCK_KEY)
        current_leader_str = str(current_leader.decode('utf-8')) if current_leader else None
        
        # Get TTL of the lock
        ttl = await asyncio.to_thread(redis_client.ttl, SCHEDULER_LOCK_KEY)
        
        # Get pending jobs count from queue
        pending_jobs_count = await asyncio.to_thread(redis_client.llen, QUEUE_KEY)
        
        # Check if this worker is the leader
        is_current_leader = (current_leader_str == str(WORKER_ID))
        
        response_data = {
            "worker_id": str(WORKER_ID),
            "is_leader": is_current_leader,
            "current_leader": current_leader_str,
            "scheduler_running": scheduler.running,
            "lock_ttl_seconds": ttl if ttl > 0 else 0,
            "pending_jobs_in_queue": pending_jobs_count,
            "status": "healthy" if current_leader_str else "NO_LEADER_DETECTED",
            "timestamp": datetime.now().isoformat()
        }
        
        # Add warning if queue is growing large
        if pending_jobs_count > 100:
            response_data["queue_warning"] = f"Queue has {pending_jobs_count} pending jobs (may indicate slow processing)"
        
        # Log warning if no leader exists
        if not current_leader_str:
            logger.warning("[SCHEDULER HEALTH] ⚠️ NO LEADER DETECTED - orphaned lock state!")
            response_data["warning"] = "No scheduler leader currently elected"
            return JSONResponse(content=response_data, status_code=503)  # Service Unavailable
        
        return JSONResponse(content=response_data, status_code=200)
        
    except Exception as e:
        logger.error(f"[SCHEDULER HEALTH] Error checking scheduler health: {e}", exc_info=True)
        return JSONResponse(
            content={
                "worker_id": str(WORKER_ID),
                "error": str(e),
                "status": "error",
                "timestamp": datetime.now().isoformat()
            },
            status_code=500
        )


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
