from fastapi import APIRouter, Depends, HTTPException, Query
from pymongo import UpdateOne
from models.tasks import taskModel, tasks_collection
from models.bots import bots_collection
from fastapi.responses import JSONResponse
from jose import JWTError
from utils.utils import generate_unique_id, get_current_user
from datetime import datetime, timedelta
from pydantic import BaseModel
from typing import List, Optional
from collections.abc import Mapping, Sequence
import traceback
import pytz
import json
import hashlib
import asyncio
from redis_client import get_redis_client


class deleteRequest(BaseModel):
    tasks: list
    
    
    
class clearOldJobsRequest(BaseModel):
    Task_ids: list
    command: dict

class inputsSaveRequest(BaseModel):
    inputs: list
    id: str

class devicesSaveRequest(BaseModel):
    devices: list
    id: str

class taskCopyRequest(BaseModel):
    taskName: str
    email: str
    bot: str
    status: str = "awaiting"
    serverId: Optional[str] = None
    channelId: Optional[str] = None
    taskTocopy: str  # ID of the task to copy inputs from

tasks_router = APIRouter()

# Initialize Redis client for caching
redis_client = get_redis_client()

def cache_get_json(key):
    """Get JSON data from Redis cache"""
    try:
        value = redis_client.get(key)
        return json.loads(value) if value else None
    except Exception as e:
        print(f"[CACHE ERROR] Failed to get cache key {key}: {e}")
        return None

def cache_set_json(key, obj, ttl_seconds):
    """Set JSON data in Redis cache with TTL"""
    try:
        redis_client.setex(key, ttl_seconds, json.dumps(obj))
        return True
    except Exception as e:
        print(f"[CACHE ERROR] Failed to set cache key {key}: {e}")
        return False

def cache_delete_pattern(pattern):
    """Delete cache keys matching a pattern"""
    try:
        keys = redis_client.keys(pattern)
        if keys:
            redis_client.delete(*keys)
            print(f"[CACHE] Deleted {len(keys)} keys matching pattern: {pattern}")
        return True
    except Exception as e:
        print(f"[CACHE ERROR] Failed to delete cache pattern {pattern}: {e}")
        return False

def get_bots_cache_key(bot_ids):
    """Generate cache key for bot details"""
    if not bot_ids:
        return None
    # Sort IDs for consistent key generation
    sorted_ids = sorted(bot_ids)
    ids_string = ','.join(sorted_ids)
    return f"bots:ids:{hashlib.sha1(ids_string.encode()).hexdigest()}"

def get_tasks_cache_key(email, page=1, limit=50, search=None):
    """Generate cache key for task list responses"""
    search_part = f":{search}" if search else ""
    return f"tasks:list:{email}:{page}:{limit}{search_part}"

def invalidate_user_tasks_cache(email):
    """Invalidate all task cache entries for a specific user"""
    pattern = f"tasks:list:{email}:*"
    return cache_delete_pattern(pattern)


def _json_serialize(value):
    """Recursively convert datetimes and unsupported types to JSON-serializable forms."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {k: _json_serialize(v) for k, v in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_serialize(v) for v in value]
    return value


def process_task_schedule(task):
    """
    Optimized schedule processing for a single task.
    Consolidates all schedule logic into one efficient function.
    """
    try:
        task_status = task.get('status', 'unknown')
        
        # For running tasks, don't use scheduledTime (it should be cleared)
        if task_status == 'running':
            # scheduledTime should be cleared for running tasks, but if it exists, ignore it
            if task.get('scheduledTime'):
                print(f"[DEBUG] Ignoring scheduledTime for running task {task.get('taskName', 'unknown')}")
        
        # Use scheduledTime only for non-running tasks
        if task.get('scheduledTime') and task_status != 'running':
            task['nextRunTime'] = task['scheduledTime']
            task['scheduleSummary'] = 'scheduledTime'
            return
        
        schedule_sources = []
        earliest_future = None
        
        # Source 1: Direct task fields
        exact_start_time = task.get('exactStartTime')
        task_timezone = task.get('timeZone', task.get('scheduleTimeZone', 'UTC'))
        
        if exact_start_time:
            schedule_sources.append('exactStartTime')
            try:
                user_tz = pytz.timezone(task_timezone)
                if isinstance(exact_start_time, str):
                    # Check if it's a time-only string (HH:MM format)
                    if ':' in exact_start_time and len(exact_start_time.split(':')) == 2:
                        # Parse time-only string (e.g., "20:12")
                        hour, minute = map(int, exact_start_time.split(':'))
                        now = datetime.now(user_tz)
                        local_dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                        # If time has passed today, schedule for tomorrow
                        if local_dt < now:
                            local_dt += timedelta(days=1)
                        earliest_future = local_dt.astimezone(pytz.UTC)
                    else:
                        # Parse full datetime string
                        clean_time = exact_start_time.replace('Z', '+00:00')
                        local_dt = datetime.fromisoformat(clean_time)
                        if local_dt.tzinfo is None:
                            local_dt = user_tz.localize(local_dt)
                        earliest_future = local_dt.astimezone(pytz.UTC)
            except Exception as e:
                print(f"[ERROR] Error parsing exactStartTime for task {task.get('taskName', 'unknown')}: {e}")
        
        # Source 2: newSchedules structure (only if no direct time found)
        if not earliest_future:
            new_schedules = task.get('newSchecdules')
            if new_schedules and isinstance(new_schedules, list) and len(new_schedules) > 0:
                schedule_sources.append('newSchecdules')
                for schedule in new_schedules:
                    if isinstance(schedule, dict):
                        sched_exact_time = schedule.get('exactStartTime')
                        sched_timezone = schedule.get('timeZone', 'UTC')
                        if sched_exact_time:
                            try:
                                sched_tz = pytz.timezone(sched_timezone)
                                clean_time = sched_exact_time.replace('Z', '+00:00')
                                local_dt = datetime.fromisoformat(clean_time)
                                if local_dt.tzinfo is None:
                                    local_dt = sched_tz.localize(local_dt)
                                utc_dt = local_dt.astimezone(pytz.UTC)
                                if earliest_future is None or utc_dt < earliest_future:
                                    earliest_future = utc_dt
                            except Exception as e:
                                print(f"[ERROR] Error parsing newSchedules time: {e}")
        
        # Source 3: activeJobs calculation (fallback only if needed)
        if not earliest_future and task.get("isScheduled"):
            schedule_sources.append('activeJobs')
            active_jobs = task.get("activeJobs", [])
            for job in active_jobs:
                start_time = job.get("startTime")
                if start_time:
                    try:
                        if isinstance(start_time, dict) and "$date" in start_time:
                            start_time_str = start_time["$date"]
                            job_start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                        elif isinstance(start_time, datetime):
                            job_start_time = start_time
                        else:
                            continue
                        
                        if job_start_time.tzinfo is None:
                            job_start_time = pytz.UTC.localize(job_start_time)
                        
                        current_utc = datetime.now(pytz.UTC)
                        if job_start_time > current_utc:
                            if earliest_future is None or job_start_time < earliest_future:
                                earliest_future = job_start_time
                                
                        # Convert to ISO format for response
                        job["startTime"] = job_start_time.isoformat()
                        end_time = job.get("endTime")
                        if end_time and isinstance(end_time, datetime):
                            job["endTime"] = end_time.isoformat()
                    except Exception as e:
                        print(f"[ERROR] Error processing activeJobs time: {e}")
        
        # Set nextRunTime based on priority
        if earliest_future:
            task['nextRunTime'] = earliest_future.isoformat()
            print(f"[DEBUG] Using calculated nextRun for task {task.get('taskName', 'unknown')}: {earliest_future.isoformat()}")
        else:
            task['nextRunTime'] = None
        
        # Set schedule summary
        task['scheduleSummary'] = '|'.join(schedule_sources) if schedule_sources else 'no_schedule_data'
        
    except Exception as e:
        print(f"[ERROR] Error processing schedule for task {task.get('taskName', 'unknown')}: {e}")
        task['scheduleSummary'] = 'error_processing_schedule'
        task['nextRunTime'] = None


@tasks_router.post("/create-task")
async def create_Task(task: taskModel, current_user: dict = Depends(get_current_user)):
    try:
        task_dict = task.dict(by_alias=True)
        task_id = generate_unique_id()
        bot = await asyncio.to_thread(
            bots_collection.find_one,
            {"id": task.bot}, {"inputs": 1, "schedules": 1})
        task_dict.update({
            "id": task_id,
            "inputs": bot.get("inputs"),
            "LastModifiedDate": datetime.utcnow().timestamp(),
            "activationDate":  datetime.utcnow(),
            "deviceIds": [],
            "schedules": bot.get("schedules")
        })
        result = await asyncio.to_thread(tasks_collection.insert_one, task_dict)
        
        # Invalidate cache for the user after creating new task
        user_email = current_user.get("email")
        invalidate_user_tasks_cache(user_email)
        print(f"[CACHE] Invalidated task cache for user {user_email} after creating new task {task_id}")
        
        return JSONResponse(content={"message": "Task created successfully!", "id": task_id}, status_code=200)

    except JWTError:
        return JSONResponse(content={"message": "sorry could not create task"}, status_code=400)
    

@tasks_router.post("/create-task-copy")
async def create_Task_copy(task: taskCopyRequest, current_user: dict = Depends(get_current_user)):
    try:
        # Generate new task ID
        task_id = generate_unique_id()
        
        # Find the task to copy inputs from
        task_to_copy = await asyncio.to_thread(
            tasks_collection.find_one,
            {"id": task.taskTocopy, "email": current_user.get("email")}, 
            {"inputs": 1}
        )
        
        if not task_to_copy:
            return JSONResponse(
                content={"message": "Task to copy not found or access denied"}, 
                status_code=404
            )
        
        # Get bot information for schedules
        bot = await asyncio.to_thread(
            bots_collection.find_one,
            {"id": task.bot}, {"inputs": 1, "schedules": 1}
        )
        
        if not bot:
            return JSONResponse(
                content={"message": "Bot not found"}, 
                status_code=404
            )
        
        # Create new task dict
        task_dict = {
            "id": task_id,
            "email": task.email,
            "taskName": task.taskName,
            "status": task.status,
            "bot": task.bot,
            "isScheduled": False,
            "activeJobs": [],
            "inputs": task_to_copy.get("inputs", bot.get("inputs", [])),  # Use copied inputs or bot defaults
            "LastModifiedDate": datetime.utcnow().timestamp(),
            "activationDate": datetime.utcnow(),
            "deviceIds": [],
            "schedules": bot.get("schedules", [])
        }
        
        # Add optional fields if provided
        if task.serverId:
            task_dict["serverId"] = task.serverId
        if task.channelId:
            task_dict["channelId"] = task.channelId
        
        # Insert the new task
        result = await asyncio.to_thread(tasks_collection.insert_one, task_dict)
        
        # Invalidate cache for the user after creating task copy
        user_email = current_user.get("email")
        invalidate_user_tasks_cache(user_email)
        print(f"[CACHE] Invalidated task cache for user {user_email} after creating task copy {task_id}")
        
        return JSONResponse(
            content={"message": "Task created successfully!", "id": task_id}, 
            status_code=200
        )

    except JWTError:
        return JSONResponse(
            content={"message": "Sorry, could not create task"}, 
            status_code=400
        )
    except Exception as e:
        print(f"Error creating task copy: {e}")
        return JSONResponse(
            content={"message": "Error creating task copy", "error": str(e)}, 
            status_code=500
        )
    

@tasks_router.get("/get-task")
async def get_Task(id: str, current_user: dict = Depends(get_current_user)):
    try:
        task = await asyncio.to_thread(
            tasks_collection.find_one,
            {"id": id}, {"_id": 0, "activationDate": 0, "status": 0,"activeJobs":0})
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        if 'activationDate' in task and isinstance(task['activationDate'], datetime):
            task['activationDate'] = task['activationDate'].isoformat()
        # Fetch associated bot details
        bot = await asyncio.to_thread(
            bots_collection.find_one,
            {"id": task.get("bot")}, {
                                       "_id": 0, "platform": 1, "botName": 1, "imagePath": 1, "id": 1})
        if not bot:
            raise HTTPException(status_code=404, detail="Bot not found")

        return JSONResponse(content={"message": "Task fetched successfully!", "task": task, "bot": bot}, status_code=200)

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(
            status_code=400, detail="Error fetching task or bot data")

@tasks_router.get("/get-all-task")
async def get_all_Task(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=200, description="Items per page"),
    search: str = Query(None, description="Search query for task name"),
    include_schedule: bool = Query(False, description="Include schedule computation and fields"),
    current_user: dict = Depends(get_current_user)
):
    print("entered /get-all-task")
    try:
        user_email = current_user.get("email")
        
        # 1. Check cache first
        cache_key = get_tasks_cache_key(user_email, page, limit, search)
        cached_response = cache_get_json(cache_key)
        if cached_response:
            print(f"[CACHE HIT] Returning cached response for {cache_key}")
            return JSONResponse(content=cached_response, status_code=200)
        
        print(f"[CACHE MISS] Fetching fresh data for {cache_key}")
        
        # 2. Build query filter
        query_filter = {"email": user_email}
        if search and search.strip():
            query_filter["taskName"] = {"$regex": search.strip(), "$options": "i"}
        
        # 3. Fetch tasks with pagination (add stable, indexed sort)
        skip = (page - 1) * limit
        # Lightweight projection by default; include heavier fields only when requested
        projection = {
            "_id": 0,
            "id": 1,
            "taskName": 1,
            "status": 1,
            "bot": 1,
            "activationDate": 1,
            "LastModifiedDate": 1,
            "deviceIds": 1,
            "scheduledTime": 1,
            "exactStartTime": 1,
            "timeZone": 1,
            "scheduleTimeZone": 1,
            "nextRunTime": 1,
        }
        if include_schedule:
            projection.update({
                "isScheduled": 1,
                "newSchecdules": 1,
                "activeJobs": 1,
            })
        tasks_cursor = (
            tasks_collection
                .find(query_filter, projection)
                .sort([("LastModifiedDate", -1), ("id", 1)])
                .skip(skip)
                .limit(limit)
                .batch_size(min(200, max(50, limit)))
        )
        tasks = await asyncio.to_thread(list, tasks_cursor)
        
        # 4. Collect all unique bot IDs to avoid N+1 query problem
        bot_ids = list(set(task.get("bot") for task in tasks if task.get("bot")))
        
        # 5. Check bot cache first
        bots_map = {}
        if bot_ids:
            bot_cache_key = get_bots_cache_key(bot_ids)
            cached_bots = cache_get_json(bot_cache_key)
            
            if cached_bots:
                print(f"[CACHE HIT] Using cached bot data for {len(bot_ids)} bots")
                bots_map = cached_bots
            else:
                print(f"[CACHE MISS] Fetching bot data for {len(bot_ids)} bots")
                # Fetch all bots in ONE query instead of individual queries
                bots_cursor = bots_collection.find(
                    {"id": {"$in": bot_ids}},
                    {"_id": 0, "platform": 1, "botName": 1, "imagePath": 1, "id": 1}
                )
                bots = await asyncio.to_thread(list, bots_cursor)
                bots_map = {bot["id"]: bot for bot in bots}
                
                # Cache bot data for 30 minutes (bots don't change frequently)
                if bot_cache_key:
                    cache_set_json(bot_cache_key, bots_map, 1800)
                    print(f"[CACHE] Stored bot data for {len(bot_ids)} bots")
        
        # 6. Process tasks with pre-fetched bot data
        for task in tasks:
            if 'activationDate' in task and isinstance(task['activationDate'], datetime):
                task['activationDate'] = task['activationDate'].isoformat()

            # Use optimized schedule processing function only when requested
            if include_schedule:
                process_task_schedule(task)

            # Remove activeJobs from response to match original behavior
            if not include_schedule:
                task.pop('activeJobs', None)

            # Use pre-fetched bot data instead of individual database queries
            bot_id = task.get("bot")
            if bot_id and bot_id in bots_map:
                task.update({"botDetails": bots_map[bot_id]})
        
        # 7. Prepare response
        response_data = {"message": "Task fetched successfully!", "tasks": tasks}
        response_data_serialized = _json_serialize(response_data)
        
        # 8. Cache the response for 60 seconds (short TTL for freshness)
        if cache_key:
            cache_set_json(cache_key, response_data_serialized, 60)
            print(f"[CACHE] Stored task response for {cache_key}")
        
        return JSONResponse(content=response_data_serialized, status_code=200)
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        traceback.print_exc()
        return JSONResponse(content={"message": "Error fetching task", "error": str(e)}, status_code=400)

@tasks_router.get("/get-scheduled-tasks")
async def get_scheduled_tasks(current_user: dict = Depends(get_current_user)):
    try:
        user_email = current_user.get("email")
        
        # 1. Check cache first
        cache_key = f"tasks:scheduled:{user_email}"
        cached_response = cache_get_json(cache_key)
        if cached_response:
            print(f"[CACHE HIT] Returning cached scheduled tasks for {user_email}")
            return JSONResponse(content=cached_response, status_code=200)
        
        print(f"[CACHE MISS] Fetching fresh scheduled tasks for {user_email}")
        
        # 2. Get scheduled tasks, excluding _id only (keep activeJobs for processing)
        result_cursor = tasks_collection.find(
            {"email": user_email, "status": "scheduled"}, {"_id": 0})
        result = await asyncio.to_thread(list, result_cursor)

        # 3. Collect all unique bot IDs to avoid N+1 query problem
        bot_ids = list(set(task.get("bot") for task in result if task.get("bot")))
        
        # 4. Check bot cache first
        bots_map = {}
        if bot_ids:
            bot_cache_key = get_bots_cache_key(bot_ids)
            cached_bots = cache_get_json(bot_cache_key)
            
            if cached_bots:
                print(f"[CACHE HIT] Using cached bot data for {len(bot_ids)} bots")
                bots_map = cached_bots
            else:
                print(f"[CACHE MISS] Fetching bot data for {len(bot_ids)} bots")
                # Fetch all bots in ONE query instead of individual queries
                bots_cursor = bots_collection.find(
                    {"id": {"$in": bot_ids}},
                    {"_id": 0, "platform": 1, "botName": 1, "imagePath": 1, "id": 1}
                )
                bots = await asyncio.to_thread(list, bots_cursor)
                bots_map = {bot["id"]: bot for bot in bots}
                
                # Cache bot data for 30 minutes (bots don't change frequently)
                if bot_cache_key:
                    cache_set_json(bot_cache_key, bots_map, 1800)
                    print(f"[CACHE] Stored bot data for {len(bot_ids)} bots")

        # 5. Process tasks
        for task in result:
            # Convert activationDate to ISO format if it's a datetime object
            if 'activationDate' in task and isinstance(task['activationDate'], datetime):
                task['activationDate'] = task['activationDate'].isoformat()

            # Use optimized schedule processing function
            process_task_schedule(task)

            # Remove activeJobs from response to match original behavior
            task.pop('activeJobs', None)

            # Use pre-fetched bot data instead of individual database queries
            bot_id = task.get("bot")
            if bot_id and bot_id in bots_map:
                task.update({"botDetails": bots_map[bot_id]})

        # 6. Prepare response and cache it
        response_data = {"message": "Scheduled tasks fetched successfully!", "tasks": result}
        cache_set_json(cache_key, response_data, 15)  # Reduced from 45 to 15 seconds
        print(f"[CACHE] Stored scheduled tasks response for {user_email}")

        return JSONResponse(content=response_data, status_code=200)

    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        traceback.print_exc()
        return JSONResponse(content={"message": "Error fetching scheduled tasks", "error": str(e)}, status_code=400)

@tasks_router.get("/get-running-tasks")
async def get_running_tasks(current_user: dict = Depends(get_current_user)):
    try:
        user_email = current_user.get("email")
        
        # 1. Check cache first
        cache_key = f"tasks:running:{user_email}"
        cached_response = cache_get_json(cache_key)
        if cached_response:
            print(f"[CACHE HIT] Returning cached running tasks for {user_email}")
            return JSONResponse(content=cached_response, status_code=200)
        
        print(f"[CACHE MISS] Fetching fresh running tasks for {user_email}")
        
        # 2. Get running tasks, excluding _id only (keep activeJobs for processing)
        result_cursor = tasks_collection.find(
            {"email": user_email, "status": "running"}, {"_id": 0})
        result = await asyncio.to_thread(list, result_cursor)
        
        # 3. Collect all unique bot IDs to avoid N+1 query problem
        bot_ids = list(set(task.get("bot") for task in result if task.get("bot")))
        
        # 4. Check bot cache first
        bots_map = {}
        if bot_ids:
            bot_cache_key = get_bots_cache_key(bot_ids)
            cached_bots = cache_get_json(bot_cache_key)
            
            if cached_bots:
                print(f"[CACHE HIT] Using cached bot data for {len(bot_ids)} bots")
                bots_map = cached_bots
            else:
                print(f"[CACHE MISS] Fetching bot data for {len(bot_ids)} bots")
                # Fetch all bots in ONE query instead of individual queries
                bots_cursor = bots_collection.find(
                    {"id": {"$in": bot_ids}},
                    {"_id": 0, "platform": 1, "botName": 1, "imagePath": 1, "id": 1}
                )
                bots = await asyncio.to_thread(list, bots_cursor)
                bots_map = {bot["id"]: bot for bot in bots}
                
                # Cache bot data for 30 minutes (bots don't change frequently)
                if bot_cache_key:
                    cache_set_json(bot_cache_key, bots_map, 1800)
                    print(f"[CACHE] Stored bot data for {len(bot_ids)} bots")
        
        # 5. Process tasks
        for task in result:
            if 'activationDate' in task and isinstance(task['activationDate'], datetime):
                task['activationDate'] = task['activationDate'].isoformat()
            
            # Use optimized schedule processing function
            process_task_schedule(task)

            # Remove activeJobs from response to match original behavior
            task.pop('activeJobs', None)
            
            # Use pre-fetched bot data instead of individual database queries
            bot_id = task.get("bot")
            if bot_id and bot_id in bots_map:
                task.update({"botDetails": bots_map[bot_id]})
        
        # 6. Prepare response and cache it
        response_data = {"message": "Running tasks fetched successfully!", "tasks": result}
        cache_set_json(cache_key, response_data, 10)  # Reduced from 30 to 10 seconds (more dynamic)
        print(f"[CACHE] Stored running tasks response for {user_email}")
        
        return JSONResponse(content=response_data, status_code=200)

    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        traceback.print_exc()
        return JSONResponse(content={"message": "Error fetching running tasks", "error": str(e)}, status_code=400)

@tasks_router.delete("/delete-tasks")
async def delete_tasks(tasks: deleteRequest, current_user: dict = Depends(get_current_user)):
    # Clean up scheduler jobs BEFORE deleting tasks from database
    from routes.deviceRegistration import cleanup_task_jobs
    
    total_jobs_removed = 0
    for task_id in tasks.tasks:
        jobs_removed = await cleanup_task_jobs(task_id)
        total_jobs_removed += jobs_removed
    
    if total_jobs_removed > 0:
        print(f"[CLEANUP] Removed {total_jobs_removed} orphaned scheduler jobs before deleting {len(tasks.tasks)} tasks")
    
    # Delete tasks from database
    result = await asyncio.to_thread(
        tasks_collection.delete_many,
        {"id": {"$in": tasks.tasks}, "email": current_user.get("email")})
    
    # Invalidate cache for the user after deleting tasks
    user_email = current_user.get("email")
    invalidate_user_tasks_cache(user_email)
    print(f"[CACHE] Invalidated task cache for user {user_email} after deleting {len(tasks.tasks)} tasks")

    return JSONResponse(content={"message": "Devices deleted successfully"}, status_code=200)

@tasks_router.get("/get-task-fields")
async def get_task_fields(id: str, fields: List[str] = Query(...), current_user: dict = Depends(get_current_user)):
    try:
        projection = {field: 1 for field in fields}
        projection.update({"_id": 0})
        task = await asyncio.to_thread(
            tasks_collection.find_one,
            {"id": id, "email": current_user.get('email')}, projection)
        if task:
            # bot['_id'] = str(bot['_id'])
            return JSONResponse(content={"message": "successfully fetched data", "data": task}, status_code=200)
    except Exception as e:
        return JSONResponse(content={"message": "could not fetch data", "error": str(e)}, status_code=500)

@tasks_router.post("/save-inputs")
async def save_task_inputs(inputs: inputsSaveRequest, current_user: dict = Depends(get_current_user)):
    result = await asyncio.to_thread(
        tasks_collection.update_one,
        {"id": inputs.id, "email": current_user.get("email")},
        {"$set": {"inputs": inputs.inputs}})
    
    # Invalidate cache for the user after updating task inputs
    user_email = current_user.get("email")
    invalidate_user_tasks_cache(user_email)
    print(f"[CACHE] Invalidated task cache for user {user_email} after updating inputs for task {inputs.id}")

    return JSONResponse(content={"message": "Inputs updated successfully"}, status_code=200)

@tasks_router.post("/save-device")
async def save_task_devices(data: devicesSaveRequest, current_user: dict = Depends(get_current_user)):
    result = await asyncio.to_thread(
        tasks_collection.update_one,
        {"id": data.id, "email": current_user.get("email")},
        {"$set": {"deviceIds": data.devices}})
    
    # Invalidate cache for the user after updating task devices
    user_email = current_user.get("email")
    invalidate_user_tasks_cache(user_email)
    print(f"[CACHE] Invalidated task cache for user {user_email} after updating devices for task {data.id}")

    return JSONResponse(content={"message": "devices updated successfully"}, status_code=200)

@tasks_router.put("/update-task")
async def update_task(data: dict, current_user: dict = Depends(get_current_user)):
    print(data)
    result = await asyncio.to_thread(
        tasks_collection.update_one,
        {"id": data["id"], "email": current_user.get("email")},
        {"$set": data["data"]})
    
    # Invalidate cache for the user after updating task
    user_email = current_user.get("email")
    invalidate_user_tasks_cache(user_email)
    print(f"[CACHE] Invalidated task cache for user {user_email} after updating task {data['id']}")

    return JSONResponse(content={"message": "Updated successfully"}, status_code=200)

@tasks_router.put("/clear-old-jobs")
async def clear_old_jobs(tasks: clearOldJobsRequest, current_user: dict = Depends(get_current_user)):
    """
    Clear ALL scheduled jobs for the current user:
    - Removes jobs from APScheduler for every task
    - Clears scheduling metadata from the task documents
    - Ignores the specific Task_ids provided (they are a frontend formality)
    """
    # We still accept the payload shape for backwards compatibility, but we ignore Task_ids
    time_zone = tasks.command.get("timeZone", "UTC")
    user_email = current_user.get("email")
    
    print(f"[LOG] Clearing ALL scheduled jobs for user: {user_email}")
    print(f"[LOG] Frontend-supplied task_ids (ignored): {tasks.Task_ids}")
    print(f"[LOG] Using time zone (for logging only): {time_zone}")
    
    try:
        # Import here to avoid circular imports at module load time
        from routes.deviceRegistration import cleanup_task_jobs
        from scheduler import scheduler
        
        # Fetch all tasks for this user
        all_tasks_cursor = tasks_collection.find({"email": user_email})
        all_tasks = await asyncio.to_thread(list, all_tasks_cursor)
        
        if not all_tasks:
            print(f"[LOG] No tasks found for user {user_email}")
            return JSONResponse(content={"message": "No tasks found for current user"}, status_code=404)
        
        print(f"[LOG] Found {len(all_tasks)} tasks to clear for user {user_email}")
        
        total_scheduler_jobs_removed = 0
        processed_tasks = 0
        
        # First, clean up APScheduler jobs for every task (task-scoped cleanup)
        for task in all_tasks:
            task_id = task.get("id")
            if not task_id:
                continue
            try:
                removed_count = await cleanup_task_jobs(task_id)
                total_scheduler_jobs_removed += removed_count
                processed_tasks += 1
                print(f"[LOG] Task {task_id}: removed {removed_count} scheduler job(s)")
            except Exception as e:
                print(f"[ERROR] Failed to clean scheduler jobs for task {task_id}: {str(e)}")
                # Continue with other tasks even if one fails
        
        # Next, clear in-memory scheduling info for ALL of this user's tasks
        update_result = await asyncio.to_thread(
            tasks_collection.update_many,
            {"email": user_email},
            {
                "$set": {
                    "activeJobs": [],
                    "status": "awaiting",
                },
                "$unset": {
                    "scheduledTime": "",
                    "exactStartTime": "",
                    "nextRunTime": "",
                },
            },
        )
        
        print(
            f"[LOG] Cleared scheduling metadata for {update_result.modified_count} task(s) "
            f"for user {user_email}"
        )
        
        # Invalidate cache for this user after clearing jobs
        invalidate_user_tasks_cache(user_email)
        print(f"[CACHE] Invalidated task cache for user {user_email} after clearing all jobs")
        
        return JSONResponse(
            content={
                "message": "Cleared all scheduled jobs for current user",
                "processed_tasks": processed_tasks,
                "modified_tasks": update_result.modified_count,
                "scheduler_jobs_removed": total_scheduler_jobs_removed,
            },
            status_code=200,
        )
    except Exception as e:
        print(f"[ERROR] General error in clear_old_jobs: {str(e)}")
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"message": f"An error occurred while clearing jobs: {str(e)}"},
        )
        
        
@tasks_router.patch("/unschedule-jobs")
async def unschedule_jobs(tasks: deleteRequest, current_user: dict = Depends(get_current_user)):
    print(f"[LOG] Unscheduling jobs for tasks: {tasks.tasks}")
    
    if not tasks.tasks:
        return JSONResponse(content={"message": "No tasks provided"}, status_code=400)
        
    try:
        # IMPORTANT: Remove jobs from APScheduler BEFORE clearing database
        # This applies to all task types: FixedTime, EveryDayAutomaticRun, and WeeklyRandomizedPlan
        # For weekly tasks, this removes all 7 scheduled jobs from the scheduler
        from scheduler import scheduler
        
        for task_id in tasks.tasks:
            task = await asyncio.to_thread(
                tasks_collection.find_one,
                {"id": task_id, "email": current_user.get("email")},
                {"activeJobs": 1}
            )
            if task and task.get("activeJobs"):
                print(f"[LOG] Removing {len(task['activeJobs'])} scheduled jobs from APScheduler for task {task_id}")
                for job in task["activeJobs"]:
                    job_id = job.get("job_id")
                    if job_id:
                        try:
                            scheduler.remove_job(job_id)
                            print(f"[LOG] ✓ Removed job {job_id} from scheduler")
                        except Exception as e:
                            print(f"[LOG] Could not remove job {job_id} from scheduler (may have already completed): {str(e)}")
                        # Also try to remove any reminder job tied to this job_id
                        try:
                            reminder_id = f"reminder_{job_id}"
                            scheduler.remove_job(reminder_id)
                            print(f"[LOG] ✓ Removed reminder job {reminder_id} from scheduler")
                        except Exception as e:
                            print(f"[LOG] Could not remove reminder job {reminder_id}: {str(e)}")
        
        # Fallback: Remove any APScheduler jobs that reference these task IDs, even if not in activeJobs
        # This handles cases where activeJobs is empty/stale but scheduler still has jobs registered
        task_ids_set = set(tasks.tasks)
        try:
            all_scheduler_jobs = scheduler.get_jobs()
            fallback_removed_count = 0
            fallback_reminder_count = 0
            
            for job in all_scheduler_jobs:
                try:
                    job_task_id = None
                    is_reminder_job = False
                    
                    # Method 1: Check job ID pattern for weekly jobs (main and reminder)
                    # Main job pattern: weekly_{task_id}_day{N}_{date}_{schedule_id}
                    # Reminder pattern: reminder_weekly_{task_id}_day{N}_{date}_{schedule_id}
                    for tid in task_ids_set:
                        if job.id.startswith(f"weekly_{tid}_"):
                            job_task_id = tid
                            break
                        elif job.id.startswith(f"reminder_weekly_{tid}_"):
                            job_task_id = tid
                            is_reminder_job = True
                            break
                    
                    # Method 2: Extract task_id from job args (backwards compatible)
                    if not job_task_id:
                        job_args = list(job.args or [])
                        if job_args:
                            # For command jobs: args = [device_ids, command_dict]
                            # command_dict is typically the last argument
                            cmd = job_args[-1] if len(job_args) > 0 else None
                            if isinstance(cmd, dict):
                                job_task_id = cmd.get("task_id")
                            # For reminder jobs: args = [task_id, day_name, start_time, ...]
                            # task_id is the first argument (string)
                            elif isinstance(job_args[0], str) and job_args[0] in task_ids_set:
                                job_task_id = job_args[0]
                                is_reminder_job = True
                    
                    if job_task_id and job_task_id in task_ids_set:
                        try:
                            scheduler.remove_job(job.id)
                            if is_reminder_job:
                                fallback_reminder_count += 1
                                print(f"[LOG] ✓ Fallback removed orphaned reminder job {job.id} (task {job_task_id})")
                            else:
                                fallback_removed_count += 1
                                print(f"[LOG] ✓ Fallback removed job {job.id} (task {job_task_id}) from scheduler")
                        except Exception as e:
                            print(f"[LOG] Fallback could not remove job {job.id}: {str(e)}")
                        
                        # For main jobs, also try to remove associated reminder job
                        if not is_reminder_job:
                            try:
                                reminder_id = f"reminder_{job.id}"
                                scheduler.remove_job(reminder_id)
                                fallback_reminder_count += 1
                                print(f"[LOG] ✓ Fallback removed reminder job {reminder_id}")
                            except Exception as e:
                                # Reminder might not exist, which is fine
                                pass
                except Exception as e:
                    # Skip jobs that can't be inspected (e.g., wrong arg structure)
                    continue
            
            if fallback_removed_count > 0 or fallback_reminder_count > 0:
                print(f"[LOG] Fallback removal: removed {fallback_removed_count} main jobs and {fallback_reminder_count} reminder jobs that weren't in activeJobs")
        except Exception as e:
            print(f"[LOG] Failed to enumerate scheduler jobs during fallback unschedule: {str(e)}")
        
        # Now update database to clear activeJobs and reset status
        result = await asyncio.to_thread(
            tasks_collection.update_many,
            {"id": {"$in": tasks.tasks}, "email": current_user.get("email")},
            {
                "$set": {
                    "activeJobs": [], 
                    "status": "awaiting"
                },
                "$unset": {
                    "scheduledTime": "",
                    "exactStartTime": "",
                    "nextRunTime": ""
                }
            }
        )
        
        # Also clear the specific time fields in schedules.inputs for EveryDayAutomaticRun
        for task_id in tasks.tasks:
            task = await asyncio.to_thread(
                tasks_collection.find_one,
                {"id": task_id, "email": current_user.get("email")})
            if task and task.get("schedules") and task.get("schedules", {}).get("inputs"):
                schedules_inputs = task["schedules"]["inputs"]
                updated_inputs = []
                
                for input_item in schedules_inputs:
                    if input_item.get("type") == "EveryDayAutomaticRun":
                        # Clear only the time fields, keep the rest
                        updated_input_item = input_item.copy()
                        updated_input_item["startinput"] = ""
                        updated_input_item["endinput"] = ""
                        updated_inputs.append(updated_input_item)
                    else:
                        updated_inputs.append(input_item)
                
                # Update the task with cleared time fields
                await asyncio.to_thread(
                    tasks_collection.update_one,
                    {"id": task_id, "email": current_user.get("email")},
                    {"$set": {"schedules.inputs": updated_inputs}}
                )
        
        if result.matched_count == 0:
            print("[LOG] No tasks found for the provided IDs and user")
            return JSONResponse(content={"message": "No tasks found"}, status_code=404)
        
        # Invalidate cache for the user after unscheduling jobs
        user_email = current_user.get("email")
        invalidate_user_tasks_cache(user_email)
        print(f"[CACHE] Invalidated task cache for user {user_email} after unscheduling {result.modified_count} tasks")
        
        return JSONResponse(content={"message": f"Successfully unscheduled {result.modified_count} tasks."}, status_code=200)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")