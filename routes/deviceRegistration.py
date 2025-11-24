import asyncio

import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Depends

from pymongo import MongoClient, UpdateOne

from pydantic import BaseModel

from typing import Dict, List, Optional, Set

import random

from datetime import datetime, timedelta

from utils.utils import (

    get_current_user,

    check_for_Job_clashes,

    split_message,

    send_split_schedule_notification,

    generate_random_durations_and_start_times,

    parse_time,

)

from models.tasks import tasks_collection

from models.bots import bots_collection
from apscheduler.triggers.date import DateTrigger

import pytz

import json

import uuid

from fastapi.responses import JSONResponse

from Bot.discord_bot import bot_instance

from scheduler import scheduler

from connection_registry import (

    register_device_connection,

    track_reconnection,

    unregister_device_connection,

    is_device_connected,

    log_all_connected_devices,

    WORKER_ID,

)

from routes.command_router import (

    set_device_connections,

    start_command_listener,

    send_commands_to_devices,

)



from redis_client import get_redis_client

from logger import logger

from utils.weekly_scheduler import (
    calculate_daily_bounds,
    generate_weekly_targets,
    validate_weekly_plan,
    generate_independent_schedules,
)
import copy




redis_client = get_redis_client()

main_event_loop = asyncio.get_event_loop()


# Helper function to check if current worker is the scheduler leader
def is_scheduler_leader():
    """Check if current worker is the scheduler leader"""
    SCHEDULER_LOCK_KEY = "appilot:scheduler:leader"
    try:
        current_leader = redis_client.get(SCHEDULER_LOCK_KEY)
        current_leader_str = str(current_leader) if current_leader else None
        return current_leader_str == str(WORKER_ID)
    except Exception as e:
        logger.error(f"[LEADER_CHECK] Failed to check leader status: {e}")
        return False  # Assume follower if check fails


def schedule_or_queue_job(job_id, trigger_time_utc, device_ids, command, job_name, job_type="command"):
    """
    Schedule a job immediately if this worker is the leader, otherwise queue it for the leader to process.
    
    Args:
        job_id: Unique job identifier
        trigger_time_utc: datetime object in UTC timezone
        device_ids: List of device IDs (empty list for reminder jobs)
        command: Command dict or reminder parameters
        job_name: Human-readable job name
        job_type: "command" for device commands, "weekly_reminder" for Discord reminders
    """
    try:
        if is_scheduler_leader():
            # Leader worker: Schedule immediately
            from apscheduler.triggers.date import DateTrigger
            
            if job_type == "weekly_reminder":
                # Reminder job - unpack command parameters
                scheduler.add_job(
                    send_weekly_reminder,
                    trigger=DateTrigger(run_date=trigger_time_utc, timezone=pytz.UTC),
                    args=[
                        command.get("task_id"),
                        command.get("day_name"),
                        command.get("start_time"),
                        command.get("schedule_lines"),
                        command.get("time_zone"),
                        False,
                        "5 Hours Before Start",
                    ],
                    id=job_id,
                    name=job_name,
                    replace_existing=True,
                )
            else:
                # Standard command job
                scheduler.add_job(
                    wrapper_for_send_command,
                    trigger=DateTrigger(run_date=trigger_time_utc, timezone=pytz.UTC),
                    args=[device_ids, command],
                    id=job_id,
                    name=job_name,
                )
            logger.info(f"[SCHEDULE] ✅ Leader scheduled job {job_id} for {trigger_time_utc}")
        else:
            # Follower worker: Queue for leader to process
            QUEUE_KEY = "appilot:pending_schedules"
            QUEUE_MAX_SIZE = 1000
            
            # Edge case: Check queue size to prevent overflow
            try:
                queue_size = redis_client.llen(QUEUE_KEY)
                if queue_size >= QUEUE_MAX_SIZE:
                    logger.error(f"[QUEUE] ❌ Queue overflow! Size: {queue_size}, max: {QUEUE_MAX_SIZE}. Dropping job {job_id}")
                    raise HTTPException(
                        status_code=503,
                        detail=f"Job queue is full ({queue_size} items). Please try again later."
                    )
            except Exception as queue_check_err:
                # If queue size check fails, log but continue (better to try queueing than fail immediately)
                logger.warning(f"[QUEUE] ⚠️ Failed to check queue size: {queue_check_err}")
            
            # Create job request payload
            job_request = {
                "job_id": job_id,
                "trigger_time_utc": trigger_time_utc.isoformat(),
                "device_ids": device_ids,
                "command": command,
                "job_name": job_name,
                "job_type": job_type,
                "queued_at": datetime.now(pytz.UTC).isoformat(),
                "queued_by_worker": str(WORKER_ID),
            }
            
            import json
            
            # Edge case: Handle Redis connection failures
            try:
                redis_client.lpush(QUEUE_KEY, json.dumps(job_request))
                redis_client.expire(QUEUE_KEY, 3600)  # 1 hour TTL
                logger.info(f"[QUEUE] ✅ Follower queued job {job_id} for leader to process")
            except Exception as redis_err:
                logger.error(f"[QUEUE] ❌ Failed to queue job {job_id} to Redis: {redis_err}", exc_info=True)
                raise HTTPException(
                    status_code=503,
                    detail=f"Failed to queue job due to Redis error: {str(redis_err)}"
                )
    except HTTPException:
        # Re-raise HTTP exceptions (queue full, Redis unavailable)
        raise
    except Exception as e:
        # Catch-all for unexpected errors
        logger.error(f"[SCHEDULE] ❌ Unexpected error scheduling/queueing job {job_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to schedule job: {str(e)}"
        )


# MongoDB Connection

client = MongoClient(

    "mongodb+srv://abdullahnoor94:dodge2018@appilot.ds9ll.mongodb.net/?retryWrites=true&w=majority&appName=Appilot"

)



db = client["Appilot"]

device_collection = db["devices"]



# Create Router

device_router = APIRouter(prefix="")



device_connections = {}

active_connections = []

connection_metadata = {}



set_device_connections(device_connections)

listener_thread = start_command_listener()





class DeviceRegistration(BaseModel):

    deviceName: str

    deviceId: str

    model: str

    botName: List[str]

    status: bool = True

    activationDate: str

    email: str





class CommandRequest(BaseModel):

    command: dict

    device_ids: List[str]





class StopTaskCommandRequest(BaseModel):

    command: dict

    Task_ids: List[str]


class PauseTaskCommandRequest(BaseModel):

    command: dict

    Task_ids: List[str]


class ResumeTaskCommandRequest(BaseModel):

    command: dict

    Task_ids: List[str]





@device_router.post("/register_device")

async def register_device_endpoint(device_data: DeviceRegistration):

    return register_device(device_data)


def chunk_text_for_discord(text: str, max_length: int = 900) -> List[str]:
    """
    Split long strings into chunks that fit within Discord embed field limits.
    """
    if not text:
        return [""]

    lines = text.split("\n")
    chunks: List[str] = []
    current_chunk: List[str] = []
    current_length = 0

    for line in lines:
        line_length = len(line) + 1  # include newline
        if current_chunk and current_length + line_length > max_length:
            chunks.append("\n".join(current_chunk))
            current_chunk = [line]
            current_length = line_length
        else:
            current_chunk.append(line)
            current_length += line_length

    if current_chunk:
        chunks.append("\n".join(current_chunk))

    return chunks


def get_method_unit(method_id: int) -> str:
    """Return the unit label ('follows' vs 'unfollows') for a given method."""
    try:
        return "unfollows" if int(method_id) == 4 else "follows"
    except (TypeError, ValueError):
        return "follows"


def format_daily_schedule_lines(
    day_index: int,
    account_names: Optional[List[str]],
    account_method_map: Optional[Dict[str, int]],
    per_account_plan_lookup: Optional[Dict[str, List[int]]],
    warmup_summary: str = "Warmup",
) -> List[str]:
    """
    Build human-readable per-account schedule lines for a specific day.
    """
    lines: List[str] = []
    accounted: set = set()
    account_method_map = account_method_map or {}
    per_account_plan_lookup = per_account_plan_lookup or {}

    for uname in sorted(account_method_map.keys()):
        method_val = account_method_map.get(uname)
        if method_val == 9:
            lines.append(f"{uname}: {warmup_summary}")
        else:
            plan = per_account_plan_lookup.get(uname) or [0] * 7
            target_val = plan[day_index] if 0 <= day_index < len(plan) else 0
            unit_label = get_method_unit(method_val)
            lines.append(f"{uname}: Method {method_val}, {target_val} {unit_label}")
        accounted.add(uname)

    if account_names:
        for uname in sorted(dict.fromkeys(account_names)):
            if uname not in accounted:
                lines.append(f"{uname}: Off")

    return lines




@device_router.get("/device_status/{device_id}")

async def check_device_status(device_id: str):

    device = await asyncio.to_thread(
        device_collection.find_one,
        {"deviceId": device_id}
    )

    if not device:

        raise HTTPException(status_code=404, detail="Device not found")

    return {"device_id": device_id, "status": device["status"]}





@device_router.put("/update_status/{device_id}")

async def update_device_status(device_id: str, status: bool):

    device = await asyncio.to_thread(
        device_collection.find_one,
        {"deviceId": device_id}
    )

    if not device:

        raise HTTPException(status_code=404, detail="Device not found")

    await asyncio.to_thread(
        device_collection.update_one,
        {"deviceId": device_id},
        {"$set": {"status": status}}
    )

    return {"message": f"Device {device_id} status updated to {status}"}





@device_router.get("/device_registration/{device_id}")

async def check_device_registration(device_id: str):

    device = await asyncio.to_thread(
        device_collection.find_one,
        {"deviceId": device_id}
    )

    if not device:

        raise HTTPException(status_code=404, detail="Device not found")



    if not device["status"]:

        await asyncio.to_thread(
            device_collection.update_one,

            {"deviceId": device_id}, {"$set": {"status": True}}

        )



    return True





@device_router.post("/stop_task")

async def stop_task(

    request: StopTaskCommandRequest, current_user: dict = Depends(get_current_user)

):

    command = request.command

    task_ids = request.Task_ids

    time_zone = request.command.get("timeZone", "UTC")

    print(f"[LOG] Received stop command: {command}")

    print(f"[LOG] Task IDs: {task_ids}")



    if not task_ids:

        return JSONResponse(status_code=404, content={"message": "No tasks provided"})



    try:

        # Get current time in the specified timezone

        user_tz = pytz.timezone(time_zone)

        current_time = datetime.now(user_tz)

        print(f"[LOG] Current time: {current_time}")



        # Collect all tasks in a single query instead of querying one by one
        # ✅ FIXED: Run DB Query in Thread to avoid blocking event loop
        tasks = await asyncio.to_thread(
            lambda: list(tasks_collection.find({"id": {"$in": task_ids}}))
        )



        if not tasks:

            return JSONResponse(status_code=404, content={"message": "No tasks found"})



        # Collect all device IDs from old jobs

        all_device_ids = set()

        tasks_to_update = []



        for task in tasks:

            task_id = task.get("id")

            task_name = task.get("taskName", "Unknown Task")



            current_jobs = task.get("activeJobs", [])

            future_jobs = []

            old_jobs = []



            # Separate future jobs from old jobs

            for job in current_jobs:

                # Handle MongoDB datetime format

                start_time = job.get("startTime")

                job_id = job.get("job_id", "unknown_job")

                if not start_time:

                    continue



                try:

                    # Check if startTime is in MongoDB format with $date field

                    if isinstance(start_time, dict) and "$date" in start_time:

                        start_time_str = start_time["$date"]

                        # Convert string time to datetime object

                        job_start_time = datetime.fromisoformat(

                            start_time_str.replace("Z", "+00:00")

                        )

                    # If startTime is already a datetime object

                    elif isinstance(start_time, datetime):

                        job_start_time = start_time

                    else:

                        print(

                            f"[ERROR] Unrecognized startTime format in task {task_id}, job {job_id}: {type(start_time)}"

                        )

                        old_jobs.append(

                            job

                        )  # Consider problematic jobs as old to be safe

                        all_device_ids.update(job.get("device_ids", []))

                        continue



                    # Ensure job_start_time is in UTC before converting to user timezone

                    if job_start_time.tzinfo is None:

                        # If time is naive (no timezone), assume it's in UTC

                        job_start_time = pytz.UTC.localize(job_start_time)



                    # Convert job time to user's timezone for comparison

                    job_start_time = job_start_time.astimezone(user_tz)



                    # Get the job's end time (if available)

                    end_time = job.get("endTime")

                    if end_time:

                        if isinstance(end_time, dict) and "$date" in end_time:

                            end_time_str = end_time["$date"]

                            job_end_time = datetime.fromisoformat(

                                end_time_str.replace("Z", "+00:00")

                            )

                        elif isinstance(end_time, datetime):

                            job_end_time = end_time

                        else:

                            job_end_time = None



                        if job_end_time:

                            if job_end_time.tzinfo is None:

                                # If time is naive (no timezone), assume it's in UTC

                                job_end_time = pytz.UTC.localize(job_end_time)

                            # Convert to user's timezone

                            job_end_time = job_end_time.astimezone(user_tz)

                    else:

                        job_end_time = None



                    # Debug information

                    print(f"[LOG] Job {job_id} start time: {job_start_time}")

                    if job_end_time:

                        print(f"[LOG] Job {job_id} end time: {job_end_time}")



                    # Fix for the time issue - check if the job is scheduled for today

                    if job_start_time.date() == current_time.date():

                        # For jobs scheduled today, compare the full datetime

                        if job_start_time > current_time:

                            print(

                                f"[LOG] Job {job_id} is today but in the future, keeping it"

                            )

                            future_jobs.append(job)

                        else:

                            print(

                                f"[LOG] Job {job_id} is today and in the past, marking as old"

                            )

                            old_jobs.append(job)

                            all_device_ids.update(job.get("device_ids", []))

                    else:

                        # For jobs on different days, compare the dates

                        if job_start_time.date() > current_time.date():

                            print(f"[LOG] Job {job_id} is on a future date, keeping it")

                            future_jobs.append(job)

                        else:

                            print(

                                f"[LOG] Job {job_id} is on a past date, marking as old"

                            )

                            old_jobs.append(job)

                            all_device_ids.update(job.get("device_ids", []))



                except Exception as e:

                    print(

                        f"[ERROR] Error processing job {job_id} in task {task_id}: {str(e)}"

                    )

                    old_jobs.append(job)  # Consider problematic jobs as old to be safe

                    all_device_ids.update(job.get("device_ids", []))



            # Determine new status based on remaining jobs

            new_status = "scheduled" if future_jobs else "awaiting"



            # Print job counts for debugging

            print(

                f"[LOG] Task {task_id}: Total jobs: {len(current_jobs)}, Old jobs: {len(old_jobs)}, Future jobs: {len(future_jobs)}"

            )



            # Save task update information

            tasks_to_update.append(

                {

                    "task_id": task_id,

                    "old_jobs": old_jobs,

                    "new_jobs": future_jobs,

                    "new_status": new_status,

                    "task_name": task_name,

                    "device_ids": task.get("deviceIds", []),

                    "server_id": task.get("serverId"),

                    "channel_id": task.get("channelId"),

                }

            )



        # If no running/old jobs were detected, force-stop by targeting the task's deviceIds

        if not all_device_ids:

            print("[LOG] No old/running jobs detected. Entering force-stop mode using task deviceIds.")

            fallback_device_ids = set()

            for t in tasks:

                fallback_device_ids.update(t.get("deviceIds", []))

            all_device_ids.update(fallback_device_ids)

            if not all_device_ids:

                print("[LOG] No devices found on tasks to force-stop.")



        # Get device info for all devices in a single query
        # ✅ FIXED: Run Device Query in Thread to avoid blocking event loop
        devices = await asyncio.to_thread(
            lambda: list(device_collection.find({"id": {"$in": list(all_device_ids)}}))
        )

        device_names = {

            device.get("id"): device.get("deviceName", device.get("id"))

            for device in devices

        }



        # Check which devices are connected

        connected_devices = []

        not_connected_devices = set()



        for device_id in all_device_ids:

            # websocket = device_connections.get(device_id)

            check = await is_device_connected(device_id)

            device_name = device_names.get(device_id, device_id)



            if check:

                print(f"[LOG] Device {device_id} ({device_name}) is connected.")

                connected_devices.append(device_id)

            else:

                print(f"[LOG] Device {device_id} ({device_name}) is NOT connected.")

                not_connected_devices.add(device_id)



        # If no devices are connected

        if not connected_devices:

            print("[LOG] No connected devices found for targeted devices.")

        else:

            # Send stop command only to devices with old jobs

            print("[LOG] Sending stop command to connected devices with old jobs.")

            print("[LOG] devices found:")

            print(connected_devices)

            result = await send_commands_to_devices(connected_devices, command)

            # for device_id, websocket in connected_devices:

            #     try:

            #         await websocket.send_text(json.dumps(command))

            #         print(f"[LOG] Successfully sent command to device {device_id} ({device_names.get(device_id, device_id)})")

            #     except Exception as e:

            #         print(f"[ERROR] Error sending command to device {device_id} ({device_names.get(device_id, device_id)}): {str(e)}")

            #         not_connected_devices.add(device_id)



        # Update all tasks with bulk write operation

        bulk_operations = []

        for task_update in tasks_to_update:

            # Only update if there are old jobs to remove

            if task_update["old_jobs"]:

                bulk_operations.append(

                    UpdateOne(

                        {"id": task_update["task_id"]},

                        {

                            "$set": {

                                "activeJobs": task_update["new_jobs"],

                                "status": task_update["new_status"],

                            }

                        },

                    )

                )



        if bulk_operations:
            # ✅ FIXED: Run Bulk Write in Thread to avoid blocking event loop
            result = await asyncio.to_thread(
                tasks_collection.bulk_write,
                bulk_operations
            )
            print(f"[LOG] Updated {result.modified_count} tasks")

        else:

            print("[LOG] No tasks needed updating")



        return {

            "message": "Tasks updated successfully",

            "stopped_jobs_count": sum(len(t["old_jobs"]) for t in tasks_to_update),

            "remaining_jobs_count": sum(len(t["new_jobs"]) for t in tasks_to_update),

        }



    except Exception as e:

        print(f"[ERROR] General error in stop_task: {str(e)}")

        import traceback



        traceback.print_exc()

        return JSONResponse(

            status_code=500, content={"message": f"An error occurred: {str(e)}"}

        )


@device_router.post("/pause_task")

async def pause_task(

    request: PauseTaskCommandRequest, current_user: dict = Depends(get_current_user)

):

    command = request.command

    task_ids = request.Task_ids

    time_zone = request.command.get("timeZone", "UTC")

    print(f"[LOG] Received pause command: {command}")

    print(f"[LOG] Task IDs: {task_ids}")



    # Validate request

    if not task_ids:

        return JSONResponse(status_code=400, content={"message": "No tasks provided"})



    # Validate command is one of the pause automation types

    app_name = command.get("appName", "")

    valid_pause_commands = ["pause automation", "pause automation 30 mins", "pause automation on demand"]

    if app_name not in valid_pause_commands:

        return JSONResponse(

            status_code=400, 

            content={"message": f"Invalid command - must be one of: {', '.join(valid_pause_commands)}"}

        )



    # Determine pause type for logging

    pause_type = "30-min" if app_name in ["pause automation", "pause automation 30 mins"] else "on-demand"

    print(f"[LOG] Pause type: {pause_type}")



    try:

        # Get current time in the specified timezone

        user_tz = pytz.timezone(time_zone)

        current_time = datetime.now(user_tz)

        print(f"[LOG] Current time: {current_time}")



        # Collect all tasks in a single query

        tasks = await asyncio.to_thread(
            lambda: list(tasks_collection.find({"id": {"$in": task_ids}}))
        )



        if not tasks:

            return JSONResponse(status_code=404, content={"message": "No tasks found"})



        # Filter for RUNNING or AWAITING tasks (pause works on both)

        # Device might be running automation even if backend shows "awaiting"

        pausable_tasks = []

        pausable_task_ids = []

        all_device_ids = set()



        for task in tasks:

            task_id = task.get("id")

            task_name = task.get("taskName", "Unknown Task")

            task_status = task.get("status", "")



            print(f"[LOG] Task {task_id} ({task_name}) status: {task_status}")



            # Allow pause for both "running" and "awaiting" tasks

            # Awaiting tasks may have devices actively running automation

            if task_status in ["running", "awaiting"]:

                pausable_tasks.append(task)

                pausable_task_ids.append(task_id)

                # Collect device IDs from this task

                device_ids = task.get("deviceIds", [])

                all_device_ids.update(device_ids)

                print(f"[LOG] Task {task_id} (status: {task_status}) is pausable, adding devices: {device_ids}")



        # If no pausable tasks found

        if not pausable_tasks:

            return JSONResponse(

                status_code=400, 

                content={"message": "No selected tasks are currently running or awaiting (only 'running' and 'awaiting' tasks can be paused)"}

            )



        print(f"[LOG] Found {len(pausable_tasks)} pausable tasks (running or awaiting)")

        print(f"[LOG] Devices to target: {list(all_device_ids)}")



        # Get device info for all devices in a single query

        devices = list(device_collection.find({"id": {"$in": list(all_device_ids)}}))

        device_names = {

            device.get("id"): device.get("deviceName", device.get("id"))

            for device in devices

        }



        # Check which devices are connected

        connected_devices = []

        not_connected_devices = set()



        for device_id in all_device_ids:

            check = await is_device_connected(device_id)

            device_name = device_names.get(device_id, device_id)



            if check:

                print(f"[LOG] Device {device_id} ({device_name}) is connected.")

                connected_devices.append(device_id)

            else:

                print(f"[LOG] Device {device_id} ({device_name}) is NOT connected.")

                not_connected_devices.add(device_id)



        # If no devices are connected

        if not connected_devices:

            print("[LOG] No connected devices found for pausable tasks.")

            return JSONResponse(

                status_code=400,

                content={"message": "No devices are currently connected for the tasks"}

            )

        else:

            # Send pause command to connected devices

            print("[LOG] Sending pause command to connected devices.")

            print(f"[LOG] Connected devices: {connected_devices}")

            result = await send_commands_to_devices(connected_devices, command)

            print(f"[LOG] Pause command sent result: {result}")



        # Return success response

        return {

            "message": "Pause command sent successfully",

            "pause_type": pause_type,

            "tasks_paused": len(pausable_tasks),

            "total_tasks": len(task_ids),

            "pausable_tasks": len(pausable_tasks),

            "connected_devices": len(connected_devices),

            "not_connected_devices": len(not_connected_devices)

        }



    except Exception as e:

        print(f"[ERROR] General error in pause_task: {str(e)}")

        import traceback



        traceback.print_exc()

        return JSONResponse(

            status_code=500, content={"message": f"An error occurred: {str(e)}"}

        )




@device_router.post("/resume_task")

async def resume_task(

    request: ResumeTaskCommandRequest, current_user: dict = Depends(get_current_user)

):

    """

    Resume automation from any pause state (30-min or on-demand)

    Cancels scheduled auto-resume if 30-min timer was active

    """

    command = request.command

    task_ids = request.Task_ids

    time_zone = request.command.get("timeZone", "UTC")

    print(f"[LOG] Received resume command: {command}")

    print(f"[LOG] Task IDs: {task_ids}")



    # Validate request

    if not task_ids:

        return JSONResponse(status_code=400, content={"message": "No tasks provided"})



    # Validate command is resume automation

    app_name = command.get("appName", "")

    if app_name != "resume automation":

        return JSONResponse(

            status_code=400,

            content={"message": "Invalid command - must be 'resume automation'"}

        )



    try:

        # Get current time in the specified timezone

        user_tz = pytz.timezone(time_zone)

        current_time = datetime.now(user_tz)

        print(f"[LOG] Current time: {current_time}")



        # Collect all tasks in a single query

        tasks = await asyncio.to_thread(
            lambda: list(tasks_collection.find({"id": {"$in": task_ids}}))
        )



        if not tasks:

            return JSONResponse(status_code=404, content={"message": "No tasks found"})



        # Collect device IDs from all tasks (regardless of status)

        # Resume can be sent to any task, the app will handle whether it's actually paused

        all_device_ids = set()

        task_device_mapping = {}



        for task in tasks:

            task_id = task.get("id")

            task_name = task.get("taskName", "Unknown Task")

            task_status = task.get("status", "")



            print(f"[LOG] Task {task_id} ({task_name}) status: {task_status}")



            # Collect device IDs from this task

            device_ids = task.get("deviceIds", [])

            all_device_ids.update(device_ids)

            task_device_mapping[task_id] = {

                "name": task_name,

                "devices": device_ids,

                "status": task_status

            }

            print(f"[LOG] Task {task_id} devices: {device_ids}")



        if not all_device_ids:

            return JSONResponse(

                status_code=400,

                content={"message": "No devices found for the selected tasks"}

            )



        print(f"[LOG] Total devices to target: {list(all_device_ids)}")



        # Get device info for all devices in a single query

        devices = list(device_collection.find({"id": {"$in": list(all_device_ids)}}))

        device_names = {

            device.get("id"): device.get("deviceName", device.get("id"))

            for device in devices

        }



        # Check which devices are connected

        connected_devices = []

        not_connected_devices = set()



        for device_id in all_device_ids:

            check = await is_device_connected(device_id)

            device_name = device_names.get(device_id, device_id)



            if check:

                print(f"[LOG] Device {device_id} ({device_name}) is connected.")

                connected_devices.append(device_id)

            else:

                print(f"[LOG] Device {device_id} ({device_name}) is NOT connected.")

                not_connected_devices.add(device_id)



        # If no devices are connected

        if not connected_devices:

            print("[LOG] No connected devices found for tasks.")

            return JSONResponse(

                status_code=400,

                content={"message": "No devices are currently connected for the tasks"}

            )

        else:

            # Send resume command to connected devices

            print("[LOG] Sending resume command to connected devices.")

            print(f"[LOG] Connected devices: {connected_devices}")

            result = await send_commands_to_devices(connected_devices, command)

            print(f"[LOG] Resume command sent result: {result}")



        # Return success response

        return {

            "message": "Resume command sent successfully",

            "total_tasks": len(task_ids),

            "tasks_targeted": len(tasks),

            "connected_devices": len(connected_devices),

            "not_connected_devices": len(not_connected_devices),

            "command_sent_to": result.get("success", []),

            "command_failed_for": result.get("failed", [])

        }



    except Exception as e:

        print(f"[ERROR] General error in resume_task: {str(e)}")

        import traceback



        traceback.print_exc()

        return JSONResponse(

            status_code=500, content={"message": f"An error occurred: {str(e)}"}

        )




@device_router.websocket("/ws/{device_id}")

async def websocket_endpoint(websocket: WebSocket, device_id: str):

    await websocket.accept()

    device_info = await asyncio.to_thread(
        device_collection.find_one,
        {"deviceId": device_id},
        {"_id": 0, "deviceName": 1, "email": 1, "status": 1},
    )

    reconnection_count = await track_reconnection(device_id)

    if reconnection_count > 10:

        print(

            f"Warning: Device {device_id} has reconnected {reconnection_count} times in the last hour"

        )



    device_connections[device_id] = websocket

    active_connections.append(websocket)

    connection_metadata[device_id] = {
        "device_info": device_info or {},
        "task_cache": {},
    }



    # Register in Redis

    await register_device_connection(device_id)



    # Update MongoDB status

    await asyncio.to_thread(
        device_collection.update_one,
        {"deviceId": device_id},
        {"$set": {"status": True}},
    )



    print(f"Device {device_id} connected to worker {WORKER_ID}")

    await log_all_connected_devices()

    async def get_task_routing(task_id: str):
        if not task_id:
            return {}
        state = connection_metadata.get(device_id)
        if not state:
            return {}
        cache = state.setdefault("task_cache", {})
        if task_id in cache:
            return cache[task_id]
        task_data = await asyncio.to_thread(
            tasks_collection.find_one,
            {"id": task_id},
            {"serverId": 1, "channelId": 1, "_id": 0},
        )
        cache[task_id] = task_data or {}
        return cache[task_id]

    try:

        while True:

            # Receive JSON message from client

            data = await websocket.receive_text()

            print(f"Message from {device_id}: {data}")



            try:

                payload = json.loads(data)

                message = payload.get("message")

                task_id = payload.get("task_id")

                job_id = payload.get("job_id")

                message_type = payload.get("type")

                print(

                    f"Parsed payload: message={message}, task_id={task_id}, job_id={job_id}"

                )



                device_state = connection_metadata.get(device_id, {})

                cached_device = device_state.get("device_info", {})

                device_name = (

                    cached_device.get("deviceName", device_id)

                    if cached_device

                    else device_id

                )

                if message and message_type != "command":

                    message = f"Device Name: {device_name}\n\n{message}"



                if message_type == "ping":

                    pong_response = {

                        "type": "pong",

                        "timestamp": payload.get("timestamp", int(time.time() * 1000)),

                    }

                    await websocket.send_text(json.dumps(pong_response))

                    print(f"Sent pong response to ({device_id})")

                    continue



                taskData = await get_task_routing(task_id)



                if message_type == "update":
                    print(f"Processing 'update' message for task_id {task_id}")
                    if (
                        taskData
                        and taskData.get("serverId")
                        and taskData.get("channelId")
                    ):
                        server_id = (
                            int(taskData["serverId"])
                            if isinstance(taskData["serverId"], str)
                            and taskData["serverId"].isdigit()
                            else taskData["serverId"]
                        )
                        channel_id = (
                            int(taskData["channelId"])
                            if isinstance(taskData["channelId"], str)
                            and taskData["channelId"].isdigit()
                            else taskData["channelId"]
                        )
                        try:
                            await bot_instance.send_message(
                                {
                                    "message": message,
                                    "task_id": task_id,
                                    "job_id": job_id,
                                    "server_id": server_id,
                                    "channel_id": channel_id,
                                    "type": "update",
                                }
                            )
                        except Exception as e:
                            print(f"Failed to send message to bot: {e}")
                    
                    continue

                elif message_type == "command":
                    try:
                        inner = json.loads(payload.get("message", "{}")) if isinstance(payload.get("message"), str) else (payload.get("message") or {})
                    except Exception:
                        inner = {}

                    action = inner.get("action")
                    if action == "SCHEDULE_WEEKLY_WARMUPS":
                        # Build command compatible with HTTP scheduling flow
                        command_payload = {
                            **{k: v for k, v in inner.items() if k not in ("action",)},
                            "task_id": task_id,
                        }

                        # Ensure required durationType for weekly scheduling
                        if command_payload.get("durationType") != "WeeklyRandomizedPlan":
                            command_payload["durationType"] = "WeeklyRandomizedPlan"

                        # Force warmup-only weekly plan: 4 warmup days, 3 off days
                        command_payload["warmupOnly"] = True
                        command_payload["testMode"] = False
                        # Provide safe defaults to avoid validation paths
                        command_payload.setdefault("followWeeklyRange", [0, 0])
                        command_payload.setdefault("restDaysRange", [4, 4])
                        command_payload.setdefault("offDaysRange", [3, 3])

                        device_ids = [device_id]

                        try:
                            req_obj = CommandRequest(command=command_payload, device_ids=device_ids)
                            current_user_context = {"email": cached_device.get("email", "")}
                            await send_command(req_obj, current_user=current_user_context)  # type: ignore
                            ack = {
                                "type": "command_ack",
                                "task_id": task_id,
                                "job_id": job_id,
                                "action": action,
                                "success": True,
                            }
                            await websocket.send_text(json.dumps(ack))
                        except Exception as schedule_err:
                            err_ack = {
                                "type": "command_ack",
                                "task_id": task_id,
                                "job_id": job_id,
                                "action": action,
                                "success": False,
                                "error": str(schedule_err),
                            }
                            await websocket.send_text(json.dumps(err_ack))
                    
                    continue



                elif message_type in ["error", "final"]:

                    print(f"Processing 'final' message for task_id {task_id}")

                    if (

                        taskData

                        and taskData.get("serverId")

                        and taskData.get("channelId")

                    ):

                        server_id = (

                            int(taskData["serverId"])

                            if isinstance(taskData["serverId"], str)

                            and taskData["serverId"].isdigit()

                            else taskData["serverId"]

                        )

                        channel_id = (

                            int(taskData["channelId"])

                            if isinstance(taskData["channelId"], str)

                            and taskData["channelId"].isdigit()

                            else taskData["channelId"]

                        )



                        message_length = len(message) if message else 0

                        print(f"Message Length: {message_length}")



                        if message_length > 1000:

                            message_chunks = split_message(message)

                            for chunk in message_chunks:

                                try:

                                    await bot_instance.send_message(

                                        {

                                            "message": chunk,

                                            "task_id": task_id,

                                            "job_id": job_id,

                                            "server_id": server_id,

                                            "channel_id": channel_id,

                                            "type": message_type,

                                        }

                                    )

                                except Exception as e:

                                    print(f"Failed to send message to bot: {e}")

                        else:

                            try:

                                await bot_instance.send_message(

                                    {

                                        "message": message,

                                        "task_id": task_id,

                                        "job_id": job_id,

                                        "server_id": server_id,

                                        "channel_id": channel_id,

                                        "type": message_type,

                                    }

                                )

                            except Exception as e:

                                print(f"Failed to send message to bot: {e}")



                    tasks_collection.update_one(

                        {"id": task_id}, {"$pull": {"activeJobs": {"job_id": job_id}}}

                    )



                    # Check if task is still active and update status

                    task = await asyncio.to_thread(
                        tasks_collection.find_one,
                        {"id": task_id}
                    )

                    if task:

                        status = (

                            "awaiting"

                            if len(task.get("activeJobs", [])) == 0

                            else "scheduled"

                        )

                        
                        
                        # If daily task completed, restore schedule for next day

                        if status == "awaiting" and task.get("durationType") == "EveryDayAutomaticRun":

                            try:

                                # Get the original schedule time from the task

                                original_time = task.get("scheduledTime") or task.get("exactStartTime")

                                if original_time:

                                    # Parse the original time

                                    if isinstance(original_time, str):

                                        original_dt = datetime.fromisoformat(original_time.replace('Z', '+00:00'))

                                        if original_dt.tzinfo is None:

                                            original_dt = pytz.UTC.localize(original_dt)

                                    else:

                                        original_dt = original_time
                                    
                                    

                                    # Calculate tomorrow at the same time

                                    tomorrow = datetime.now(pytz.UTC) + timedelta(days=1)

                                    next_schedule = tomorrow.replace(

                                        hour=original_dt.hour,

                                        minute=original_dt.minute,

                                        second=0,

                                        microsecond=0

                                    )

                                    
                                    
                                    # Restore scheduledTime for next day

                                    await asyncio.to_thread(
                                        tasks_collection.update_one,
                                        {"id": task_id},
                                        {
                                            "$set": {
                                                "status": "scheduled",
                                                "scheduledTime": next_schedule.isoformat()
                                            }
                                        }
                                    )

                                    print(f"[SCHEDULE] Restored daily schedule for task {task_id} to {next_schedule}")

                                    
                                    
                                    # Force immediate cache refresh after schedule restoration

                                    try:

                                        from routes.tasks import invalidate_user_tasks_cache

                                        # Get user email from the task

                                        task_user_info = tasks_collection.find_one(

                                            {"id": task_id},

                                            {"email": 1}

                                        )

                                        if task_user_info and task_user_info.get("email"):

                                            user_email = task_user_info["email"]

                                            # Clear all related caches immediately

                                            invalidate_user_tasks_cache(user_email)

                                            
                                            
                                            # Also clear specific endpoint caches for immediate refresh

                                            redis_client.delete(f"tasks:list:{user_email}:*")

                                            redis_client.delete(f"tasks:scheduled:{user_email}")

                                            redis_client.delete(f"tasks:running:{user_email}")

                                            
                                            
                                            print(f"[CACHE] All caches cleared after daily schedule restoration for task {task_id}")

                                    except Exception as cache_error:

                                        print(f"[WARNING] Cache clearing failed after schedule restoration: {cache_error}")

                            except Exception as e:

                                print(f"[ERROR] Failed to restore daily schedule: {e}")

                                # Fallback to normal status update

                                await asyncio.to_thread(
                                    tasks_collection.update_one,
                                    {"id": task_id},
                                    {"$set": {"status": status}}
                                )

                        else:

                            # Normal status update

                            await asyncio.to_thread(
                                tasks_collection.update_one,
                                {"id": task_id},
                                {"$set": {"status": status}}
                            )



                else:

                    print(

                        f"Skipping message send. Missing or empty serverId/channelId for task {task_id}"

                    )



            except json.JSONDecodeError:

                print(f"Invalid JSON received from {device_id}: {data}")



    except WebSocketDisconnect:

        print(f"Device {device_id} disconnected from worker {WORKER_ID}")



        # Update MongoDB

        await asyncio.to_thread(
            device_collection.update_one,
            {"deviceId": device_id},
            {"$set": {"status": False}},
        )



        # Clean up local connections

        active_connections.remove(websocket)

        device_connections.pop(device_id, None)
        connection_metadata.pop(device_id, None)



        # Unregister from Redis

        await unregister_device_connection(device_id)





@device_router.post("/send_command")

async def send_command(

    request: CommandRequest, current_user: dict = Depends(get_current_user)

):

    task_id = request.command.get("task_id")

    command = request.command

    print(command)

    device_ids = request.device_ids

    duration = int(command.get("duration", 0))

    newInputs = request.command.get("newInputs")
    newSchedules = request.command.get("newSchecdules")
    durationType = request.command.get("durationType")
    # --- Patch: Detect MultipleRunTimes from newSchecdules if durationType is missing or not set ---
    # Only override durationType if not set or not recognized
    if not durationType or durationType not in [
            "MultipleRunTimes",
            "DurationWithExactStartTime",
            "ExactStartTime",
            "DurationWithTimeWindow",
            "EveryDayAutomaticRun",
            "WeeklyRandomizedPlan",
            "WeeklyWarmupOnly"
        ]:
        # Try to detect MultipleRunTimes from newSchecdules
        if newSchedules and isinstance(newSchedules, dict):
            sched_inputs = newSchedules.get("inputs", [])
            if isinstance(sched_inputs, list):
                for entry in sched_inputs:
                    if entry.get("type") == "MultipleRunTimes":
                        durationType = "MultipleRunTimes"
                        break
    # --- End Patch ---

    time_zone = request.command.get("timeZone", "UTC")

    # Update only non-null inputs/schedules; never set to null
    update_fields = {"deviceIds": device_ids}
    if newInputs is not None:
        update_fields["inputs"] = newInputs
    if newSchedules is not None:
        update_fields["schedules"] = newSchedules
    if update_fields:
        await asyncio.to_thread(
            tasks_collection.update_one,
            {"id": task_id},
            {"$set": update_fields}
        )


    try:

        user_tz = pytz.timezone(time_zone)

        now = datetime.now(user_tz)

        print(f"Current time in {time_zone}: {now}")

        # --- MultipleRunTimes scheduling ---
        if durationType == "MultipleRunTimes":
            print(f"[LOG] MultipleRunTimes detected for task {task_id}")
            # Clear old scheduled jobs before scheduling new multiple run times
            task = await asyncio.to_thread(
                tasks_collection.find_one,
                {"id": task_id},
                {"activeJobs": 1}
            )
            if task and task.get("activeJobs"):
                for old_job in task["activeJobs"]:
                    job_id_to_remove = old_job.get("job_id")
                    if job_id_to_remove:
                        try:
                            scheduler.remove_job(job_id_to_remove)
                            print(f"[LOG] Removed old job {job_id_to_remove} from scheduler")
                        except Exception as e:
                            print(f"[LOG] Could not remove job {job_id_to_remove} from scheduler: {str(e)}")
            await asyncio.to_thread(
                tasks_collection.update_one,
                {"id": task_id},
                {"$set": {"activeJobs": []}}
            )
            print(f"[LOG] Cleared activeJobs array for task {task_id}")

            # Extract MultipleRunTimes info from newSchecdules
            multiple_schedule = None
            if newSchedules and isinstance(newSchedules, dict):
                inputs = newSchedules.get("inputs", [])
                for entry in inputs:
                    if entry.get("type") == "MultipleRunTimes":
                        multiple_schedule = entry
                        break
            if not multiple_schedule:
                raise HTTPException(status_code=400, detail="MultipleRunTimes schedule not found in newSchecdules")

            run_times = multiple_schedule.get("runStartTimes", [])
            number_of_runs = multiple_schedule.get("numberOfRuns", len(run_times))
            if not run_times or number_of_runs < 1:
                raise HTTPException(status_code=400, detail="No run times provided for MultipleRunTimes")

            scheduled_job_ids = []
            first_job_scheduled = False
            for idx, time_str in enumerate(run_times):
                try:
                    hour, minute = map(int, time_str.split(":"))
                except Exception:
                    raise HTTPException(status_code=400, detail=f"Invalid runStartTime format: {time_str}")
                run_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                end_time = run_time + timedelta(minutes=duration)
                if run_time < now:
                    run_time += timedelta(days=1)
                    end_time += timedelta(days=1)
                run_time_utc = run_time.astimezone(pytz.UTC)
                end_time_utc = end_time.astimezone(pytz.UTC)
                job_id = f"cmd_{uuid.uuid4()}"
                job_command = copy.deepcopy(command)
                job_command["job_id"] = job_id
                job_command["runIndex"] = idx
                
                # Use helper function to schedule or queue
                schedule_or_queue_job(
                    job_id=job_id,
                    trigger_time_utc=run_time_utc,
                    device_ids=device_ids,
                    command=job_command,
                    job_name=f"MultipleRunTimes {idx+1} for devices {device_ids}",
                    job_type="command"
                )
                logger.info(f"[MULTIPLE] 📌 Job {job_id} scheduled for {run_time_utc}")
                
                scheduled_job_ids.append(job_id)
                job_instance = {
                    "job_id": job_id,
                    "startTime": run_time_utc,
                    "endTime": end_time_utc,
                    "device_ids": device_ids,
                    "deliveryStatus": "pending",
                    "deliveryAttempts": 0,
                    "method": durationType,
                    "runIndex": idx,
                    "durationType": durationType,
                }
                if not first_job_scheduled:
                    await asyncio.to_thread(
                        tasks_collection.update_one,
                        {"id": task_id},
                        {
                            "$set": {
                                "status": "scheduled",
                                "nextRunTime": run_time_utc.isoformat()
                            },
                            "$unset": {"scheduledTime": ""},
                            "$push": {"activeJobs": job_instance},
                        },
                    )
                    first_job_scheduled = True
                else:
                    await asyncio.to_thread(
                        tasks_collection.update_one,
                        {"id": task_id},
                        {"$push": {"activeJobs": job_instance}},
                    )
                print(f"[LOG] Scheduled MultipleRunTimes job {job_id} at {run_time_utc}")
            # --- Send Discord notification after scheduling ---
            try:
                task_meta = tasks_collection.find_one(
                    {"id": task_id}, {"_id": 0, "taskName": 1, "serverId": 1, "channelId": 1}
                ) or {}
                server_id = task_meta.get("serverId")
                channel_id = task_meta.get("channelId")
                if server_id and channel_id:
                    lines = []
                    for idx, time_str in enumerate(run_times):
                        job_id = scheduled_job_ids[idx] if idx < len(scheduled_job_ids) else ""
                        run_time = now.replace(hour=int(time_str.split(":")[0]), minute=int(time_str.split(":")[1]), second=0, microsecond=0)
                        if run_time < now:
                            run_time += timedelta(days=1)
                        date_str = run_time.strftime("%b %d")
                        time_str_fmt = run_time.strftime("%H:%M")
                        lines.append(f"Run {idx+1}: {date_str} {time_str_fmt} | Job ID: {job_id}")
                    summary = (
                        f"Multiple Run Times scheduled: {task_meta.get('taskName', 'Unknown Task')}\n"
                        f"Task ID: {task_id}\n"
                        f"Total runs: {number_of_runs}\n"
                        + "\n".join(lines)
                    )
                    await bot_instance.send_message(
                        {
                            "message": summary,
                            "task_id": task_id,
                            "job_id": f"multipleruntimes_summary_{task_id}",
                            "server_id": int(server_id) if isinstance(server_id, str) and server_id.isdigit() else server_id,
                            "channel_id": int(channel_id) if isinstance(channel_id, str) and channel_id.isdigit() else channel_id,
                            "type": "info",
                        }
                    )
                else:
                    print("[LOG] Skipping Discord summary; serverId/channelId not set on task")
            except Exception as e:
                print(f"[LOG] Failed to send MultipleRunTimes Discord summary: {e}")
            return {"message": "MultipleRunTimes command scheduled successfully"}


        if durationType in ["DurationWithExactStartTime", "ExactStartTime"]:

            # Clear old scheduled jobs before scheduling new fixed start time

            print(f"[LOG] Clearing old fixed start time jobs for task {task_id}")

            task = await asyncio.to_thread(
                tasks_collection.find_one,
                {"id": task_id},
                {"activeJobs": 1}
            )

            if task and task.get("activeJobs"):

                for old_job in task["activeJobs"]:

                    job_id_to_remove = old_job.get("job_id")

                    if job_id_to_remove:

                        try:

                            scheduler.remove_job(job_id_to_remove)

                            print(f"[LOG] Removed old job {job_id_to_remove} from scheduler")

                        except Exception as e:

                            # Job might not exist in scheduler or already completed

                            print(f"[LOG] Could not remove job {job_id_to_remove} from scheduler: {str(e)}")
            
            

            # Clear activeJobs array before scheduling new job

            await asyncio.to_thread(
                tasks_collection.update_one,
                {"id": task_id},
                {"$set": {"activeJobs": []}}

            )

            print(f"[LOG] Cleared activeJobs array for task {task_id}")

            
            
            time_str = request.command.get("exactStartTime")

            hour, minute = parse_time(time_str)



            target_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

            end_time_delta = timedelta(minutes=duration)

            target_end_time = target_time + end_time_delta



            if target_time < now:

                target_time += timedelta(days=1)

                target_end_time += timedelta(days=1)



            target_time_utc = target_time.astimezone(pytz.UTC)

            target_end_time_utc = target_end_time.astimezone(pytz.UTC)



            job_id = f"cmd_{uuid.uuid4()}"

            command["job_id"] = job_id

            schedule_single_job(

                target_time_utc,

                target_end_time_utc,

                device_ids,

                command,

                job_id,

                task_id,

            )



        elif durationType == "DurationWithTimeWindow":

            # Clear old scheduled jobs before scheduling new time window

            print(f"[LOG] Clearing old time window jobs for task {task_id}")

            task = tasks_collection.find_one({"id": task_id}, {"activeJobs": 1})

            if task and task.get("activeJobs"):

                for old_job in task["activeJobs"]:

                    job_id_to_remove = old_job.get("job_id")

                    if job_id_to_remove:

                        try:

                            scheduler.remove_job(job_id_to_remove)

                            print(f"[LOG] Removed old job {job_id_to_remove} from scheduler")

                        except Exception as e:

                            # Job might not exist in scheduler or already completed

                            print(f"[LOG] Could not remove job {job_id_to_remove} from scheduler: {str(e)}")
            
            

            # Clear activeJobs array before scheduling new job

            await asyncio.to_thread(
                tasks_collection.update_one,
                {"id": task_id},
                {"$set": {"activeJobs": []}}

            )

            print(f"[LOG] Cleared activeJobs array for task {task_id}")

            
            
            start_time_str = command.get("startInput")

            end_time_str = command.get("endInput")



            start_hour, start_minute = parse_time(start_time_str)

            end_hour, end_minute = parse_time(end_time_str)



            start_time = now.replace(

                hour=start_hour, minute=start_minute, second=0, microsecond=0

            )

            end_time = now.replace(

                hour=end_hour, minute=end_minute, second=0, microsecond=0

            )



            if end_time < start_time:

                end_time += timedelta(days=1)

            if start_time < now:

                start_time += timedelta(days=1)

                end_time += timedelta(days=1)



            time_window = (end_time - start_time).total_seconds() / 60

            if abs(time_window - duration) <= 10:

                job_id = f"cmd_{uuid.uuid4()}"

                command["job_id"] = job_id

                schedule_single_job(

                    start_time, end_time, device_ids, command, job_id, task_id

                )

            else:

                random_durations, start_times = (

                    generate_random_durations_and_start_times(

                        duration, start_time, end_time

                    )

                )

                schedule_split_jobs(

                    start_times, random_durations, device_ids, command, task_id

                )



        elif durationType == "EveryDayAutomaticRun":

            # Clear old scheduled jobs before scheduling new daily run

            print(f"[LOG] Clearing old daily run jobs for task {task_id}")

            task = await asyncio.to_thread(
                tasks_collection.find_one,
                {"id": task_id},
                {"activeJobs": 1}
            )

            if task and task.get("activeJobs"):

                for old_job in task["activeJobs"]:

                    job_id = old_job.get("job_id")

                    if job_id:

                        try:

                            scheduler.remove_job(job_id)

                            print(f"[LOG] Removed old job {job_id} from scheduler")

                        except Exception as e:

                            # Job might not exist in scheduler or already completed

                            print(f"[LOG] Could not remove job {job_id} from scheduler: {str(e)}")
            
            

            # Clear activeJobs array before scheduling new job

            await asyncio.to_thread(
                tasks_collection.update_one,
                {"id": task_id},
                {"$set": {"activeJobs": []}}

            )

            print(f"[LOG] Cleared activeJobs array for task {task_id}")

            
            
            # Now schedule the new daily recurring job

            schedule_recurring_job(command, device_ids)



        elif durationType == "WeeklyRandomizedPlan":
            try:
                logger.info("[WEEKLY] WeeklyRandomizedPlan detected; extracting weekly fields...")

                # Fetch latest task for defaults and bot id
                task_doc = await asyncio.to_thread(
                    tasks_collection.find_one,
                    {"id": task_id}
                ) or {}
                bot_id = task_doc.get("bot")

                # Inputs: prefer newInputs, then inputs in command, then task/bot defaults
                inputs_payload = (
                    request.command.get("newInputs")
                    if request.command.get("newInputs") is not None
                    else request.command.get("inputs")
                )
                if inputs_payload is None:
                    # fallback: existing task inputs or bot defaults
                    inputs_payload = task_doc.get("inputs")
                    if inputs_payload is None and bot_id:
                        bot_doc = bots_collection.find_one({"id": bot_id}, {"inputs": 1}) or {}
                        inputs_payload = bot_doc.get("inputs", [])

                # Schedules: prefer newSchecdules or schedules from command, else task/bot defaults
                schedules_payload = (
                    request.command.get("newSchecdules")
                    if request.command.get("newSchecdules") is not None
                    else request.command.get("schedules")
                )
                if schedules_payload is None:
                    schedules_payload = task_doc.get("schedules")
                    if schedules_payload is None and bot_id:
                        bot_doc = await asyncio.to_thread(
                            bots_collection.find_one,
                            {"id": bot_id},
                            {"schedules": 1}
                        ) or {}
                        schedules_payload = bot_doc.get("schedules", [])

                # Parse weekly-specific fields from command-level
                week_start_raw = command.get("week_start")
                follow_weekly_range = tuple(command.get("followWeeklyRange", []))
                rest_days_range = tuple(command.get("restDaysRange", []))
                off_days_range_raw = command.get("offDaysRange")
                off_days_range = None
                if isinstance(off_days_range_raw, (list, tuple)) and len(off_days_range_raw) == 2:
                    try:
                        off_days_range = (int(off_days_range_raw[0]), int(off_days_range_raw[1]))
                    except Exception:
                        off_days_range = None
                no_two_high_rule_raw = command.get("noTwoHighRule")
                test_mode = bool(command.get("testMode", False))  # Test mode: schedule with 2-min/5-min gaps
                # Resolve method (prefer command-level, else schedules index 2, else default 1)
                resolved_method = command.get("method")
                try:
                    method = int(resolved_method) if resolved_method is not None else None
                except Exception:
                    method = None
                if method is None:
                    try:
                        sched_inputs = None
                        if isinstance(schedules_payload, dict) and isinstance(schedules_payload.get("inputs"), list):
                            sched_inputs = schedules_payload.get("inputs")
                        elif isinstance(schedules_payload, list):
                            sched_inputs = schedules_payload
                        if sched_inputs and len(sched_inputs) >= 3 and isinstance(sched_inputs[2], dict):
                            maybe_method = sched_inputs[2].get("method")
                            if maybe_method is not None and str(maybe_method).strip() != "":
                                method = int(maybe_method)
                    except Exception:
                        method = None
                if method is None:
                    method = 1
                # Clamp method to 1-10 range; rest days will use 9 automatically
                if method < 1 or method > 10:
                    method = 1
                logger.info(f"[WEEKLY] Using method: {method}")
                if test_mode:
                    logger.warning(f"[WEEKLY] ⚠️ TEST MODE ENABLED - Scheduling with 5-minute gaps starting NOW")

                # Warmup-only support: allow missing weekly ranges and week_start
                warmup_only_flag = bool(command.get("warmupOnly", False))
                # Default weekly warmup to TEST mode unless explicitly provided
                # if warmup_only_flag and ("testMode" not in command):
                #     test_mode = True
                #     logger.warning("[WEEKLY] Defaulting to TEST MODE for warmup-only weekly plan")
                if warmup_only_flag:
                    # Provide safe defaults if missing
                    if len(follow_weekly_range) != 2:
                        follow_weekly_range = (0, 0)
                    if len(rest_days_range) != 2:
                        rest_days_range = (4, 4)
                    if not off_days_range_raw:
                        off_days_range_raw = (3, 3)
                else:
                    if not week_start_raw or len(follow_weekly_range) != 2 or len(rest_days_range) != 2:
                        raise HTTPException(
                            status_code=400,
                            detail="Weekly data not found in schedules OR command-level fields",
                        )

                # Resolve no_two_high_rule (default if missing)
                def _coerce_pair(val):
                    try:
                        a, b = val
                        return (int(a), int(b))
                    except Exception:
                        return None
                no_two_high_rule = _coerce_pair(no_two_high_rule_raw) if isinstance(no_two_high_rule_raw, (list, tuple)) else None
                if not off_days_range:
                    try:
                        sched_inputs = None
                        if isinstance(schedules_payload, dict) and isinstance(schedules_payload.get("inputs"), list):
                            sched_inputs = schedules_payload.get("inputs")
                        elif isinstance(schedules_payload, list):
                            sched_inputs = schedules_payload
                        if sched_inputs and len(sched_inputs) >= 3 and isinstance(sched_inputs[2], dict):
                            existing_weekly = sched_inputs[2].get("weeklyData", {})
                            off_pair = existing_weekly.get("offDaysRange")
                            if isinstance(off_pair, (list, tuple)) and len(off_pair) == 2:
                                try:
                                    off_days_range = (int(off_pair[0]), int(off_pair[1]))
                                except Exception:
                                    off_days_range = None
                    except Exception:
                        off_days_range = None
                if not off_days_range:
                    off_days_range = (1, 1)

                if not no_two_high_rule:
                    # Try fallback from schedules payload index 2 if present
                    try:
                        sched_inputs = None
                        if isinstance(schedules_payload, dict) and isinstance(schedules_payload.get("inputs"), list):
                            sched_inputs = schedules_payload.get("inputs")
                        elif isinstance(schedules_payload, list):
                            sched_inputs = schedules_payload
                        if sched_inputs and len(sched_inputs) >= 3 and isinstance(sched_inputs[2], dict):
                            high_val = sched_inputs[2].get("noTwoHighHigh")
                            follow_val = sched_inputs[2].get("noTwoHighFollow")
                            if high_val not in (None, "") and follow_val not in (None, ""):
                                no_two_high_rule = _coerce_pair([high_val, follow_val])
                    except Exception:
                        no_two_high_rule = None
                if not no_two_high_rule:
                    # Backend default
                    no_two_high_rule = (27, 23)

                # Parse week_start as local midnight
                try:
                    # Accept YYYY-MM-DD or ISO datetime
                    if isinstance(week_start_raw, str):
                        clean = week_start_raw.replace("Z", "+00:00")
                        if "T" in clean or "+" in clean:
                            local_dt = datetime.fromisoformat(clean)
                        else:
                            # Date-only string
                            local_dt = datetime.fromisoformat(clean + "T00:00:00")
                    else:
                        # For warmup-only, allow missing week_start and use current time
                        if warmup_only_flag:
                            local_dt = datetime.now(pytz.timezone(time_zone))
                        else:
                            raise ValueError("Invalid week_start")

                    user_tz = pytz.timezone(time_zone)
                    if local_dt.tzinfo is None:
                        local_dt = user_tz.localize(local_dt)
                    else:
                        local_dt = local_dt.astimezone(user_tz)

                    logger.info(f"[WEEKLY] Parsing week_start: '{week_start_raw}' → '{local_dt.isoformat()}'")
                except Exception:
                    raise HTTPException(status_code=400, detail="Invalid week_start format")

                # Test mode: override week_start to use current time for immediate scheduling
                if test_mode:
                    local_dt = datetime.now(user_tz)
                    logger.warning(f"[WEEKLY] Test mode: Overriding week_start to NOW: {local_dt.isoformat()}")

                # Calculate and log daily bounds
                bounds = calculate_daily_bounds(
                    (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                    (int(rest_days_range[0]), int(rest_days_range[1])),
                    (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                    (int(off_days_range[0]), int(off_days_range[1])),
                )
                logger.info(
                    f"[WEEKLY] Weekly Constraints: Total range: {follow_weekly_range[0]}-{follow_weekly_range[1]} follows/week | Rest days: {rest_days_range[0]}-{rest_days_range[1]} | NoTwoHighRule: {no_two_high_rule} | Calculated daily bounds: {bounds['daily_min']}-{bounds['daily_max']} follows/day"
                )

                # Generate and validate schedule
                # Default ranges for rest day likes, comments, and duration (can be made configurable later)
                rest_day_likes_range = (4, 10)  # 4-10 likes per rest day
                rest_day_comments_range = (1, 3)  # 1-3 comments per rest day
                rest_day_duration_range = (30, 120)  # 30-120 minutes per rest day
                
                previous_active_days_map: Optional[Dict[str, Set[int]]] = None
                
                # Extract account usernames for per-account scheduling
                account_usernames_for_caps = []
                try:
                    new_inputs_obj_caps = command.get("newInputs")
                    if isinstance(new_inputs_obj_caps, dict):
                        wrapper_inputs_caps = new_inputs_obj_caps.get("inputs", [])
                        for wrapper_entry_caps in (wrapper_inputs_caps or []):
                            inner_inputs_caps = wrapper_entry_caps.get("inputs", [])
                            for node_caps in (inner_inputs_caps or []):
                                if isinstance(node_caps, dict) and node_caps.get("type") == "instagrmFollowerBotAcountWise":
                                    for acc_caps in (node_caps.get("Accounts") or []):
                                        uname_caps = acc_caps.get("username")
                                        # Layer 1: Filter by frontend enabled flag - only enabled accounts get schedules
                                        if isinstance(uname_caps, str) and acc_caps.get('enabled', True) is not False:
                                            account_usernames_for_caps.append(uname_caps)
                except Exception:
                    account_usernames_for_caps = []

                # Fallback: if no usernames found via newInputs, derive from legacy inputs
                if not account_usernames_for_caps:
                    try:
                        legacy_inputs_arr = command.get("inputs")
                        if isinstance(legacy_inputs_arr, list):
                            for acct in legacy_inputs_arr:
                                uname_legacy = acct.get("username")
                                if isinstance(uname_legacy, str):
                                    account_usernames_for_caps.append(uname_legacy)
                    except Exception:
                        pass
                
                # Get previous history for each account if available
                try:
                    sched_inputs_for_prev = None
                    if isinstance(schedules_payload, dict) and isinstance(schedules_payload.get("inputs"), list):
                        sched_inputs_for_prev = schedules_payload.get("inputs")
                    elif isinstance(schedules_payload, list):
                        sched_inputs_for_prev = schedules_payload
                    
                    if (
                        sched_inputs_for_prev
                        
                        and len(sched_inputs_for_prev) >= 3
                        and isinstance(sched_inputs_for_prev[2], dict)
                    ):
                        prev_generated_map = sched_inputs_for_prev[2].get("generatedSchedulesMap")
                        if prev_generated_map and isinstance(prev_generated_map, dict):
                            previous_active_days_map = {}
                            for uname, sched in prev_generated_map.items():
                                if isinstance(sched, list):
                                    active_set = {
                                        int(day.get("dayIndex"))
                                        for day in sched
                                        if isinstance(day, dict)
                                        and not day.get("isRest")
                                        and not day.get("isOff")
                                        and day.get("dayIndex") is not None
                                    }
                                    if active_set:
                                        previous_active_days_map[uname] = active_set
                except Exception:
                    previous_active_days_map = None

                # Generate INDEPENDENT schedules for each account
                if account_usernames_for_caps:
                    schedules_map = generate_independent_schedules(
                        accounts=account_usernames_for_caps,
                        week_start_dt=local_dt,
                        follow_weekly_range=(int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                        rest_days_range=(int(rest_days_range[0]), int(rest_days_range[1])),
                        no_two_high_rule=(int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                        off_days_range=(int(off_days_range[0]), int(off_days_range[1])),
                        previous_active_days_map=previous_active_days_map,
                        rest_day_likes_range=rest_day_likes_range,
                        rest_day_comments_range=rest_day_comments_range,
                        rest_day_duration_range=rest_day_duration_range
                    )
                    
                    # Add start_local and end_local timestamps to each day entry in schedules_map
                    if schedules_map:
                        for uname, sched in schedules_map.items():
                            for d in sched:
                                idx = int(d.get("dayIndex", 0))
                                is_rest = bool(d.get("isRest", False))
                                is_off = bool(d.get("isOff", False))
                                day_method = int(d.get("method", 0 if is_off else (9 if is_rest else 1)))
                                
                                # Preserve warmup parameters before any modifications
                                preserved_max_likes = d.get("maxLikes")
                                preserved_max_comments = d.get("maxComments")
                                preserved_warmup_duration = d.get("warmupDuration")
                                
                                # If warmup parameters are missing for a rest day, re-randomize them
                                # BUT skip if method is 0 (warmup was disabled due to active accounts)
                                if (is_rest or day_method == 9) and day_method != 0:
                                    # Ensure method is set to 9 for rest days
                                    if is_rest and day_method != 9:
                                        d["method"] = 9
                                        day_method = 9
                                    
                                    if preserved_max_likes is None or preserved_max_comments is None or preserved_warmup_duration is None:
                                        logger.warning(f"[WEEKLY-WARMUP] Missing warmup params for {uname} Day {idx}, re-randomizing...")
                                        preserved_max_likes = random.randint(rest_day_likes_range[0], rest_day_likes_range[1])
                                        preserved_max_comments = random.randint(rest_day_comments_range[0], rest_day_comments_range[1])
                                        preserved_warmup_duration = random.randint(rest_day_duration_range[0], rest_day_duration_range[1])
                                        d["maxLikes"] = preserved_max_likes
                                        d["maxComments"] = preserved_max_comments
                                        d["warmupDuration"] = preserved_warmup_duration
                                
                                if test_mode:
                                    # In test mode, use 5-minute gaps between scheduled days
                                    gap_minutes = 5
                                    start_time_local = local_dt + timedelta(minutes=idx * gap_minutes)
                                    # For warmup-only, keep session short placeholder end; device manages day plan
                                    end_time_local = start_time_local + (timedelta(minutes=1) if warmup_only_flag else timedelta(hours=11))
                                else:
                                    day_local = local_dt + timedelta(days=idx)
                                    if warmup_only_flag:
                                        # Trigger morning window 09:10–12:00 to align device day window
                                        start_window_start = day_local.replace(hour=9, minute=10, second=0, microsecond=0)
                                        start_window_end = day_local.replace(hour=12, minute=0, second=0, microsecond=0)
                                    else:
                                        start_window_start = day_local.replace(hour=11, minute=0, second=0, microsecond=0)
                                        start_window_end = day_local.replace(hour=22, minute=0, second=0, microsecond=0)
                                    window_minutes = int((start_window_end - start_window_start).total_seconds() // 60)
                                    if window_minutes > 0:
                                        random_offset = random.randint(0, window_minutes - 1)
                                        start_time_local = start_window_start + timedelta(minutes=random_offset)
                                        end_time_local = (start_time_local + timedelta(minutes=1)) if warmup_only_flag else start_window_end
                                    else:
                                        # Invalid window, skip timestamps for this entry but preserve warmup params
                                        if is_rest or day_method == 9:
                                            if preserved_max_likes is not None:
                                                d["maxLikes"] = preserved_max_likes
                                            if preserved_max_comments is not None:
                                                d["maxComments"] = preserved_max_comments
                                            if preserved_warmup_duration is not None:
                                                d["warmupDuration"] = preserved_warmup_duration
                                        continue
                                
                                # Add timestamps to the day entry as ISO strings (for JSON serialization)
                                d["start_local"] = start_time_local.isoformat()
                                d["end_local"] = end_time_local.isoformat()
                                
                                # Restore warmup parameters after adding timestamps (in case they were lost)
                                if is_rest or day_method == 9:
                                    if preserved_max_likes is not None:
                                        d["maxLikes"] = preserved_max_likes
                                    if preserved_max_comments is not None:
                                        d["maxComments"] = preserved_max_comments
                                    if preserved_warmup_duration is not None:
                                        d["warmupDuration"] = preserved_warmup_duration
                    
                    # Ensure no warmups occur on days where any account is active
                    if schedules_map:
                        for day_idx in range(7):
                            has_active = False
                            for sched in schedules_map.values():
                                entry = next((d for d in sched if d.get("dayIndex") == day_idx), None)
                                if entry and not entry.get("isRest") and not entry.get("isOff"):
                                    has_active = True
                                    break
                            if not has_active:
                                continue
                            for sched in schedules_map.values():
                                entry = next((d for d in sched if d.get("dayIndex") == day_idx), None)
                                if entry and entry.get("isRest"):
                                    entry["method"] = 0
                                    entry.pop("maxLikes", None)
                                    entry.pop("maxComments", None)
                                    entry.pop("warmupDuration", None)
                    
                    # Use the first account's schedule as the "generated" schedule for backward compatibility/visuals
                    # But the real logic will use schedules_map
                    generated = list(schedules_map.values())[0] if schedules_map else []
                    
                else:
                    # Fallback to old logic if no accounts found (shouldn't happen normally)
                    logger.warning("[WEEKLY] No accounts found for independent scheduling, falling back to single schedule")
                    generated = generate_weekly_targets(
                        local_dt,
                        (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                        (int(rest_days_range[0]), int(rest_days_range[1])),
                        (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                        method,
                        rest_day_likes_range,
                        rest_day_comments_range,
                        rest_day_duration_range,
                        (int(off_days_range[0]), int(off_days_range[1])),
                        previous_active_days=None,
                    )
                    schedules_map = {} # Empty map means fallback behavior

                # Ensure newInputs enabled flags reflect which accounts actually have scheduled activity
                if schedules_map:
                    accounts_with_activity: Set[str] = set()
                    for uname, sched in schedules_map.items():
                        if not isinstance(uname, str) or not isinstance(sched, list):
                            continue
                        has_activity = any(not entry.get("isOff", False) for entry in sched if isinstance(entry, dict))
                        if has_activity:
                            accounts_with_activity.add(uname)
                    if accounts_with_activity:
                        try:
                            new_inputs_obj_enabled = command.get("newInputs")
                            if isinstance(new_inputs_obj_enabled, dict):
                                wrapper_inputs_enabled = new_inputs_obj_enabled.get("inputs", [])
                                for wrapper_entry_enabled in (wrapper_inputs_enabled or []):
                                    inner_inputs_enabled = wrapper_entry_enabled.get("inputs", [])
                                    for node_enabled in (inner_inputs_enabled or []):
                                        if isinstance(node_enabled, dict) and node_enabled.get("type") == "instagrmFollowerBotAcountWise":
                                            for acc_enabled in (node_enabled.get("Accounts") or []):
                                                uname_enabled = acc_enabled.get("username")
                                                if isinstance(uname_enabled, str):
                                                    acc_enabled["enabled"] = uname_enabled in accounts_with_activity
                        except Exception as enabled_err:
                            logger.warning(f"[WEEKLY] Failed to update enabled flags from schedules: {enabled_err}")

                # Special mode: warmup-only weekly plan (4 warmup days, 3 off days)
                is_warmup_only = warmup_only_flag
                if is_warmup_only:
                    warmup_indices = sorted(random.sample(range(7), 4))
                    custom_generated = []
                    for d in range(7):
                        if d in warmup_indices:
                            # Randomize warmup targets within the same ranges used for rest days
                            wl = random.randint(rest_day_likes_range[0], rest_day_likes_range[1])
                            wc = random.randint(rest_day_comments_range[0], rest_day_comments_range[1])
                            wd = random.randint(rest_day_duration_range[0], rest_day_duration_range[1])
                            custom_generated.append({
                                "dayIndex": d,
                                "target": 0,
                                "isRest": True,
                                "isOff": False,
                                "method": 9,
                                "maxLikes": wl,
                                "maxComments": wc,
                                "warmupDuration": wd,
                            })
                        else:
                            custom_generated.append({
                                "dayIndex": d,
                                "target": 0,
                                "isRest": False,
                                "isOff": True,
                                "method": 0,
                            })
                    generated = custom_generated
                else:
                    validate_weekly_plan(
                        generated,
                        (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                        (int(rest_days_range[0]), int(rest_days_range[1])),
                        (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                        (int(off_days_range[0]), int(off_days_range[1])),
                    )

                logger.info(f"[WEEKLY] Generated {len(generated)} days")

                # Compute planned start/end times once and reuse for summary and scheduling
                # Initialize planned_schedule BEFORE the Discord summary block
                planned_schedule = []
                for d in generated:
                    idx = int(d.get("dayIndex", 0))
                    is_rest = bool(d.get("isRest", False))
                    is_off = bool(d.get("isOff", False))
                    target = int(d.get("target", 0))
                    day_method = int(d.get("method", 0 if is_off else (9 if is_rest else 1)))
                    
                    if test_mode:
                        # In test mode, use 5-minute gaps between scheduled days
                        gap_minutes = 5
                        start_time_local = local_dt + timedelta(minutes=idx * gap_minutes)
                        # For warmup-only, keep session short placeholder end; device manages day plan
                        end_time_local = start_time_local + (timedelta(minutes=1) if is_warmup_only else timedelta(hours=11))
                    else:
                        day_local = local_dt + timedelta(days=idx)
                        if is_warmup_only:
                            # Trigger morning window 09:10–12:00 to align device day window
                            start_window_start = day_local.replace(hour=9, minute=10, second=0, microsecond=0)
                            start_window_end = day_local.replace(hour=12, minute=0, second=0, microsecond=0)
                        else:
                            start_window_start = day_local.replace(hour=11, minute=0, second=0, microsecond=0)
                            start_window_end = day_local.replace(hour=22, minute=0, second=0, microsecond=0)
                        window_minutes = int((start_window_end - start_window_start).total_seconds() // 60)
                        if window_minutes <= 0:
                            # Skip invalid window day
                            continue
                        random_offset = random.randint(0, window_minutes - 1)
                        start_time_local = start_window_start + timedelta(minutes=random_offset)
                        end_time_local = (start_time_local + timedelta(minutes=1)) if is_warmup_only else start_window_end
                    
                    entry = {
                        "dayIndex": idx,
                        "isRest": is_rest,
                        "isOff": is_off,
                        "target": target,
                        "method": day_method,
                        "start_local": start_time_local,
                        "end_local": end_time_local,
                    }
                    if is_rest:
                        entry["maxLikes"] = int(d.get("maxLikes", 10))
                        entry["maxComments"] = int(d.get("maxComments", 5))
                        entry["warmupDuration"] = int(d.get("warmupDuration", 60))
                    planned_schedule.append(entry)

                # --- BEGIN full weekly schedule Discord summary ---
                task_meta = {}
                server_id = None
                channel_id = None
                try:
                    task_meta = await asyncio.to_thread(
                        tasks_collection.find_one,
                        {"id": task_id},
                        {"_id": 0, "taskName": 1, "serverId": 1, "channelId": 1}
                    ) or {}

                    server_id = task_meta.get("serverId")
                    channel_id = task_meta.get("channelId")

                    if server_id and channel_id:
                        # Build readable schedule table with actual calendar dates
                        day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                        method_names = {
                            0: "Off Day",
                            1: "Method 1",
                            4: "Method 4",
                            9: "Method 9"
                        }
                        
                        # Add methodLabel to planned_schedule entries for Discord summary
                        for p in planned_schedule:
                            day_method = int(p.get("method", 0))
                            p["methodLabel"] = method_names.get(day_method, f"method {day_method}")
                        
                        # --- Per-account weekly plans (caps only; methods come from newInputs) ---
                        per_account_plans = {}
                        account_usernames = []
                        try:
                            new_inputs_obj = command.get("newInputs")
                            if isinstance(new_inputs_obj, dict):
                                wrapper_inputs = new_inputs_obj.get("inputs", [])
                                for wrapper_entry in (wrapper_inputs or []):
                                    inner_inputs = wrapper_entry.get("inputs", [])
                                    for node in (inner_inputs or []):
                                        if isinstance(node, dict) and node.get("type") == "instagrmFollowerBotAcountWise":
                                            for acc in (node.get("Accounts") or []):
                                                uname = acc.get("username")
                                                # Layer 1: Filter by frontend enabled flag - only enabled accounts get schedules
                                                if isinstance(uname, str) and acc.get('enabled', True) is not False:
                                                    account_usernames.append(uname)
                        except Exception:
                            account_usernames = []

                        if account_usernames:
                            try:
                                from utils.weekly_scheduler import generate_per_account_plans
                                per_account_plans = generate_per_account_plans(
                                    generated,
                                    account_usernames,
                                    (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                                    (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                                )
                            except Exception as e:
                                logger.warning(f"[WEEKLY] Failed to build per-account plans: {e}")
                                per_account_plans = {}

                        # Build account_day_lookup from schedules_map for Discord summary
                        account_day_lookup: Dict[str, Dict[int, Dict]] = {}
                        if schedules_map and isinstance(schedules_map, dict):
                            for uname, sched in schedules_map.items():
                                if not isinstance(uname, str):
                                    continue
                                day_map: Dict[int, Dict] = {}
                                for entry in (sched or []):
                                    if not isinstance(entry, dict):
                                        continue
                                    idx = entry.get("dayIndex")
                                    if isinstance(idx, int):
                                        # Debug: Log warmup parameters for rest days
                                        is_rest = entry.get("isRest", False)
                                        method_val = entry.get("method", 0)
                                        is_off = entry.get("isOff", False)
                                        
                                        # If it's a rest day (isRest=True or method=9), ensure warmup parameters exist
                                        # BUT skip if method is 0 (warmup was disabled due to active accounts)
                                        if (is_rest or method_val == 9) and not is_off and method_val != 0:
                                            # Ensure method is set to 9 for rest days
                                            if is_rest and method_val != 9:
                                                entry["method"] = 9
                                                method_val = 9
                                            
                                            max_likes = entry.get('maxLikes')
                                            max_comments = entry.get('maxComments')
                                            warmup_duration = entry.get('warmupDuration')
                                            
                                            # Always re-randomize for rest days to ensure unique values per account/day
                                            # This ensures no default values (10, 5, 60) are used
                                            needs_randomization = (
                                                max_likes is None or max_comments is None or warmup_duration is None or
                                                max_likes == 10 or max_comments == 5 or warmup_duration == 60
                                            )
                                            
                                            if needs_randomization:
                                                if max_likes is None or max_likes == 10 or max_comments is None or max_comments == 5 or warmup_duration is None or warmup_duration == 60:
                                                    logger.warning(f"[WEEKLY-WARMUP] Missing or default warmup params for {uname} Day {idx} (maxLikes={max_likes}, maxComments={max_comments}, warmupDuration={warmup_duration}), re-randomizing...")
                                                max_likes = random.randint(rest_day_likes_range[0], rest_day_likes_range[1])
                                                max_comments = random.randint(rest_day_comments_range[0], rest_day_comments_range[1])
                                                warmup_duration = random.randint(rest_day_duration_range[0], rest_day_duration_range[1])
                                                entry["maxLikes"] = max_likes
                                                entry["maxComments"] = max_comments
                                                entry["warmupDuration"] = warmup_duration
                                                # Also update the same entry in schedules_map to ensure consistency
                                                if schedules_map and uname in schedules_map:
                                                    for sched_entry in schedules_map[uname]:
                                                        if isinstance(sched_entry, dict) and sched_entry.get("dayIndex") == idx:
                                                            sched_entry["maxLikes"] = max_likes
                                                            sched_entry["maxComments"] = max_comments
                                                            sched_entry["warmupDuration"] = warmup_duration
                                                            sched_entry["method"] = 9
                                                            break
                                            
                                            logger.info(f"[WEEKLY-WARMUP] Account {uname} Day {idx}: isRest={is_rest}, method={method_val}, maxLikes={max_likes}, maxComments={max_comments}, warmupDuration={warmup_duration}")
                                        
                                        day_map[idx] = entry
                                if day_map:
                                    account_day_lookup[uname] = day_map

                        accounts_for_summary = sorted(
                            dict.fromkeys(
                                account_usernames or list(account_day_lookup.keys())
                            )
                        )

                        # Extract per-account methods from newInputs for summary display
                        input_name_to_method_map = {
                            "Follow from Notification Suggestions": 1,
                            "Follow from Profile Followers List": 2,
                            "Follow from Post": 3,
                            "Unfollow Non-Followers": 4,
                            "Accept All Follow Requests": 5,
                            "Follow from Profile Posts": 6,
                            "Switch Account Public To Private": 7,
                            "Switch Account Private To Public": 8,
                            "Standalone Warmup": 9,
                        }

                        def extract_account_methods_for_summary(new_inputs_obj):
                            account_methods = {}
                            try:
                                if isinstance(new_inputs_obj, dict):
                                    wrapper_inputs = new_inputs_obj.get("inputs", [])
                                    for wrapper_entry in (wrapper_inputs or []):
                                        inner_inputs = wrapper_entry.get("inputs", [])
                                        for node in (inner_inputs or []):
                                            if isinstance(node, dict) and node.get("type") == "instagrmFollowerBotAcountWise":
                                                accounts = node.get("Accounts", [])
                                                for acc in (accounts or []):
                                                    username = acc.get("username")
                                                    if not isinstance(username, str):
                                                        continue
                                                    acc_inputs = acc.get("inputs", [])
                                                    enabled_method = None
                                                    for input_block in (acc_inputs or []):
                                                        if isinstance(input_block, dict):
                                                            if input_block.get("input", False):
                                                                name = input_block.get("name", "")
                                                                if name in input_name_to_method_map:
                                                                    m = input_name_to_method_map[name]
                                                                    if enabled_method is None or (m != 9 and enabled_method == 9):
                                                                        enabled_method = m
                                                    if enabled_method:
                                                        account_methods[username] = enabled_method
                            except Exception:
                                pass
                            return account_methods

                        account_methods_for_summary = extract_account_methods_for_summary(command.get("newInputs"))
                        resolved_method = command.get("method")
                        try:
                            if isinstance(resolved_method, list) and len(resolved_method) > 0:
                                base_method_for_summary = int(resolved_method[0])
                            else:
                                base_method_for_summary = int(resolved_method) if resolved_method is not None else 1
                        except (ValueError, TypeError):
                            base_method_for_summary = 1

                        # Build a simple per-day account method map for summary display
                        account_method_map_by_day_for_summary: Dict[int, Dict[str, int]] = {}
                        if account_day_lookup:
                            for di_tmp in range(7):
                                day_map: Dict[str, int] = {}
                                for uname in accounts_for_summary:
                                    day_entry = (
                                        account_day_lookup.get(uname, {}).get(di_tmp)
                                    )
                                    if not day_entry:
                                        continue
                                    entry_method = int(day_entry.get("method", 0))
                                    if day_entry.get("isOff") or entry_method == 0:
                                        continue
                                    if day_entry.get("isRest") or entry_method == 9:
                                        day_map[uname] = 9
                                    else:
                                        chosen_method = account_methods_for_summary.get(uname, entry_method)
                                        day_map[uname] = chosen_method
                                account_method_map_by_day_for_summary[di_tmp] = day_map
                        else:
                            for p in planned_schedule:
                                di_tmp = int(p.get("dayIndex", 0))
                                is_rest_tmp = bool(p.get("isRest", False))
                                is_off_tmp = bool(p.get("isOff", False))
                                planned_m = int(
                                    p.get(
                                        "method",
                                        0 if is_off_tmp else (9 if is_rest_tmp else base_method_for_summary),
                                    )
                                )
                                if is_rest_tmp or is_off_tmp or planned_m == 9:
                                    account_method_map_by_day_for_summary[di_tmp] = {}
                                else:
                                    m = {}
                                    for uname in account_usernames:
                                        m[uname] = account_methods_for_summary.get(
                                            uname, planned_m
                                        )
                                    account_method_map_by_day_for_summary[di_tmp] = m

                        def build_account_day_parts(day_index: int) -> List[str]:
                            if not account_day_lookup or not accounts_for_summary:
                                return []
                            # Use the same ranges as defined in the outer scope, with fallback defaults
                            try:
                                warmup_likes_range = rest_day_likes_range
                                warmup_comments_range = rest_day_comments_range
                                warmup_duration_range = rest_day_duration_range
                            except NameError:
                                # Fallback if variables not in scope
                                warmup_likes_range = (4, 10)
                                warmup_comments_range = (1, 3)
                                warmup_duration_range = (30, 120)
                            
                            parts: List[str] = []
                            method_override_map = account_method_map_by_day_for_summary.get(day_index, {})
                            for uname in accounts_for_summary:
                                day_plan = account_day_lookup.get(uname, {}).get(day_index)
                                if not day_plan:
                                    parts.append(f"{uname}: Off")
                                    continue
                                plan_method = int(day_plan.get("method", 0))
                                if day_plan.get("isOff") or plan_method == 0:
                                    parts.append(f"{uname}: Off")
                                elif (day_plan.get("isRest") or plan_method == 9 or method_override_map.get(uname) == 9) and plan_method != 0:
                                    likes = day_plan.get("maxLikes")
                                    comments = day_plan.get("maxComments")
                                    duration = day_plan.get("warmupDuration")
                                    
                                    # If parameters are missing or match defaults, re-randomize them
                                    if likes is None or comments is None or duration is None or likes == 10 or comments == 5 or duration == 60:
                                        logger.warning(f"[WEEKLY-WARMUP-DISCORD] Missing or default warmup params for {uname} Day {day_index}, re-randomizing...")
                                        likes = random.randint(warmup_likes_range[0], warmup_likes_range[1])
                                        comments = random.randint(warmup_comments_range[0], warmup_comments_range[1])
                                        duration = random.randint(warmup_duration_range[0], warmup_duration_range[1])
                                        # Update the day_plan so it's saved for future use
                                        day_plan["maxLikes"] = likes
                                        day_plan["maxComments"] = comments
                                        day_plan["warmupDuration"] = duration
                                    
                                    parts.append(
                                        f"{uname}: Warmup - {likes} likes, {comments} comments, {duration}min"
                                    )
                                else:
                                    final_method = method_override_map.get(uname, plan_method)
                                    target_val = int(
                                        day_plan.get(
                                            "target",
                                            (
                                                (per_account_plans.get(uname) or [0] * 7)[day_index]
                                                if per_account_plans
                                                else 0
                                            ),
                                        )
                                    )
                                    unit_label = get_method_unit(final_method)
                                    parts.append(
                                        f"{uname}: Method {final_method}, {target_val} {unit_label}"
                                    )
                            return parts

                        # Build summary lines with planned start times (show per-account caps on active days; no global headline)
                        lines = []
                        # Determine if this is a warmup-only plan (weekly target range 0–0)
                        try:
                            is_warmup_only_plan = int(follow_weekly_range[0]) == 0 and int(follow_weekly_range[1]) == 0
                        except Exception:
                            is_warmup_only_plan = False
                        for p in planned_schedule:
                            actual_day_name = day_names[p["start_local"].weekday()]
                            date_str = p["start_local"].strftime("%b %d")
                            time_str = p["start_local"].strftime("%H:%M")
                            heading = f"📅 **{actual_day_name} {date_str}** {time_str}:"
                            di = int(p.get("dayIndex", 0))
                            if account_day_lookup:
                                parts = build_account_day_parts(di)
                                if parts:
                                    label = f"{heading}\n" + "\n".join(parts)
                                else:
                                    label = f"{heading}\n{p['methodLabel']}"
                                lines.append(label)
                                continue

                            if p["isOff"]:
                                if is_warmup_only_plan:
                                    # Display off-days as warmup entries in warmup-only plans
                                    try:
                                        rand_likes = random.randint(0, 5)
                                        rand_comments = random.randint(0, 2)
                                        rand_duration = random.randint(5, 10)
                                    except Exception:
                                        rand_likes, rand_comments, rand_duration = 0, 0, 5
                                    label = f"{heading}\nOff day Warmup - {rand_likes} likes, {rand_comments} comments, {rand_duration}min"
                                else:
                                    label = f"📅 **{actual_day_name} {date_str}**:\nOff day - No tasks scheduled"
                            elif p["isRest"]:
                                label = f"{heading}\n{p['methodLabel']} - {p.get('maxLikes', 10)} likes, {p.get('maxComments', 5)} comments, {p.get('warmupDuration', 60)}min"
                            else:
                                per_day_map = account_method_map_by_day_for_summary.get(di) or {}
                                parts = []
                                for uname in sorted(account_usernames):
                                    if uname in per_day_map:
                                        account_m = per_day_map[uname]
                                        if account_m == 9:
                                            # For warmup, do not show or assign follows
                                            parts.append(f"{uname}: Method 9")
                                        else:
                                            max_f = (per_account_plans.get(uname) or [0]*7)[di] if per_account_plans else 0
                                            unit_label = get_method_unit(account_m)
                                            parts.append(f"{uname}: Method {account_m}, {max_f} {unit_label} (max)")
                                if parts:
                                    label = f"{heading}\n" + "\n".join(parts)
                                else:
                                    label = f"{heading}\n{p['methodLabel']}"
                            lines.append(label)
                        
                        test_mode_indicator = "TEST MODE (2-min gaps)\n" if test_mode else ""
                        summary = (
                            f"Weekly plan scheduled: {task_meta.get('taskName', 'Unknown Task')}\n"
                            f"{test_mode_indicator}\n" + "\n".join(lines)
                        )

                        summary_chunks = chunk_text_for_discord(summary)
                        for idx, chunk in enumerate(summary_chunks, start=1):
                            prefix = "" if idx == 1 else f"(Part {idx})\n"
                            await bot_instance.send_message(
                                {
                                    "message": f"{prefix}{chunk}",
                                    "task_id": task_id,
                                    "job_id": f"weekly_summary_{task_id}_part{idx}",
                                    "server_id": int(server_id) if isinstance(server_id, str) and server_id.isdigit() else server_id,
                                    "channel_id": int(channel_id) if isinstance(channel_id, str) and channel_id.isdigit() else channel_id,
                                    "type": "info",
                                }
                            )
                    else:
                        logger.info(
                            "[WEEKLY] Skipping full schedule summary; serverId/channelId not set on task"
                        )
                except Exception as e:
                    logger.warning(f"[WEEKLY] Failed to send full weekly schedule summary: {e}")
                # --- END full weekly schedule Discord summary ---

                # Build schedules structure with WeeklyRandomizedPlan entry
                weekly_data = {
                    "week_start": local_dt.replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),
                    "followWeeklyRange": [int(follow_weekly_range[0]), int(follow_weekly_range[1])],
                    "restDaysRange": [int(rest_days_range[0]), int(rest_days_range[1])],
                    "noTwoHighRule": [int(no_two_high_rule[0]), int(no_two_high_rule[1])],
                    "offDaysRange": [int(off_days_range[0]), int(off_days_range[1])],
                    "method": method,
                }

                weekly_entry = {
                    "type": "WeeklyRandomizedPlan",
                    "weeklyData": weekly_data,
                    "generatedSchedule": generated,
                    "generatedSchedulesMap": schedules_map,  # Store the per-account map for history tracking
                }

                # Normalize schedules payload to dict with inputs array
                if isinstance(schedules_payload, dict) and "inputs" in schedules_payload:
                    schedules_inputs = schedules_payload.get("inputs", [])
                elif isinstance(schedules_payload, list):
                    schedules_inputs = schedules_payload
                    schedules_payload = {"type": "schedules", "inputs": schedules_inputs}
                else:
                    schedules_inputs = []
                    schedules_payload = {"type": "schedules", "inputs": []}

                # Preserve existing schedule cards; only overwrite the weekly block at index 2
                for entry in schedules_inputs:
                    if isinstance(entry, dict):
                        entry["selected"] = False

                while len(schedules_inputs) < 3:
                    schedules_inputs.append({})

                weekly_entry_selected = dict(weekly_entry)
                weekly_entry_selected["selected"] = True
                schedules_inputs[2] = weekly_entry_selected
                schedules_payload["inputs"] = schedules_inputs

                # Persist inputs/schedules/newInputs (avoid nulls)
                persist_update = {"deviceIds": device_ids}
                if inputs_payload is not None:
                    persist_update["inputs"] = inputs_payload
                if schedules_payload is not None:
                    persist_update["schedules"] = schedules_payload
                # IMPORTANT: Also persist newInputs from command as "inputs" in DB (consistent with daily/fixed-time)
                new_inputs_from_command = command.get("newInputs")
                if new_inputs_from_command is not None:
                    persist_update["inputs"] = new_inputs_from_command
                    # Log sample of what we're persisting
                    try:
                        sample_block = new_inputs_from_command.get("inputs", [{}])[0].get("inputs", [{}])[0].get("Accounts", [{}])[0].get("inputs", [{}])[0]
                        logger.info(f"[WEEKLY] Persisting inputs with sample block: name={sample_block.get('name')}, minDaily={sample_block.get('minFollowsDaily')}, maxDaily={sample_block.get('maxFollowsDaily')}")
                    except:
                        pass
                await asyncio.to_thread(
                    tasks_collection.update_one,
                    {"id": task_id},
                    {"$set": persist_update}
                )
                logger.info(f"[WEEKLY] Persisted to DB: inputs={new_inputs_from_command is not None}, schedules={schedules_payload is not None}")

                # NOTE: Instant caps logic DISABLED - each job applies its own caps when it runs
                # Applying instant caps breaks rest days (Day 0 would get Day 1's caps)
                # The per-job mutation logic in send_command_to_devices handles this correctly
                logger.info("[WEEKLY] Skipping instant caps - will be applied per-job when each day runs")

                # Clear old WEEKLY jobs before scheduling weekly plan (preserve Fixed/Daily jobs)
                print(f"[WEEKLY] Clearing old weekly jobs for task {task_id}")
                task = await asyncio.to_thread(
                    tasks_collection.find_one,
                    {"id": task_id},
                    {"activeJobs": 1}
                )
                if task and task.get("activeJobs"):
                    for old_job in task["activeJobs"]:
                        try:
                            if old_job.get("durationType") == "WeeklyRandomizedPlan":
                                old_job_id = old_job.get("job_id")
                                if old_job_id:
                                    try:
                                        scheduler.remove_job(old_job_id)
                                    except Exception:
                                        pass
                                    # Also remove the reminder job for this weekly job if present
                                    try:
                                        scheduler.remove_job(f"reminder_{old_job_id}")
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                # Remove only weekly entries from activeJobs in DB
                try:
                    await asyncio.to_thread(
                        tasks_collection.update_one,
                        {"id": task_id},
                        {"$pull": {"activeJobs": {"durationType": "WeeklyRandomizedPlan"}}}
                    )
                except Exception:
                    pass

                # Schedule 7 jobs with random local start time 11:00–22:00, end at 22:00
                # Also schedule "1 hour before" reminder notifications
                scheduled_job_ids = []
                notify_daily = bool(command.get("notifyDaily", False))
                total_target = 0
                active_days = 0
                warmup_days = 0
                first_job_scheduled = False  # Track if we've set nextRunTime yet
                # Extract account usernames once for per-account plan mutation
                account_usernames_for_caps = []
                try:
                    new_inputs_obj_caps = command.get("newInputs")
                    if isinstance(new_inputs_obj_caps, dict):
                        wrapper_inputs_caps = new_inputs_obj_caps.get("inputs", [])
                        for wrapper_entry_caps in (wrapper_inputs_caps or []):
                            inner_inputs_caps = wrapper_entry_caps.get("inputs", [])
                            for node_caps in (inner_inputs_caps or []):
                                if isinstance(node_caps, dict) and node_caps.get("type") == "instagrmFollowerBotAcountWise":
                                    for acc_caps in (node_caps.get("Accounts") or []):
                                        uname_caps = acc_caps.get("username")
                                        # Layer 1: Filter by frontend enabled flag - only enabled accounts get schedules
                                        if isinstance(uname_caps, str) and acc_caps.get('enabled', True) is not False:
                                            account_usernames_for_caps.append(uname_caps)
                except Exception:
                    account_usernames_for_caps = []

                # Fallback: if no usernames found via newInputs, derive from legacy inputs
                if not account_usernames_for_caps:
                    try:
                        legacy_inputs_arr = command.get("inputs")
                        if isinstance(legacy_inputs_arr, list):
                            for acct in legacy_inputs_arr:
                                uname_legacy = acct.get("username")
                                # Layer 1: Filter by frontend enabled flag - only enabled accounts get schedules
                                if isinstance(uname_legacy, str) and acct.get('enabled', True) is not False:
                                    account_usernames_for_caps.append(uname_legacy)
                    except Exception:
                        pass

                # Reuse same per-account plan if available; otherwise compute now
                if account_usernames_for_caps:
                    # If we generated independent schedules, we can build a compatible "caps plan" from them
                    if 'schedules_map' in locals() and schedules_map:
                        per_account_plans_caps = {}
                        for uname, sched in schedules_map.items():
                            # Convert list of day-dicts into list of targets
                            targets_list = [0] * 7
                            for d in sched:
                                idx = d.get("dayIndex")
                                t = d.get("target", 0)
                                if isinstance(idx, int) and 0 <= idx < 7:
                                    targets_list[idx] = t
                            per_account_plans_caps[uname] = targets_list
                    elif 'per_account_plans' in locals() and per_account_plans:
                        per_account_plans_caps = per_account_plans
                    else:
                        try:
                            from utils.weekly_scheduler import generate_per_account_plans
                            per_account_plans_caps = generate_per_account_plans(
                                generated,
                                account_usernames_for_caps,
                                (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                                (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                            )
                        except Exception as e:
                            logger.warning(f"[WEEKLY] Fallback per-account plan compute failed: {e}")
                            per_account_plans_caps = {}
                else:
                    per_account_plans_caps = {}

                # Build per-day account method map from newInputs (no randomization)
                base_account_usernames = []
                if account_usernames_for_caps:
                    base_account_usernames = list(dict.fromkeys(account_usernames_for_caps))
                elif 'account_usernames' in locals() and account_usernames:
                    base_account_usernames = list(dict.fromkeys(account_usernames))
                elif per_account_plans_caps:
                    base_account_usernames = list(dict.fromkeys(per_account_plans_caps.keys()))

                input_name_to_method_map = {
                    "Follow from Notification Suggestions": 1,
                    "Follow from Profile Followers List": 2,
                    "Follow from Post": 3,
                    "Unfollow Non-Followers": 4,
                    "Accept All Follow Requests": 5,
                    "Follow from Profile Posts": 6,
                    "Switch Account Public To Private": 7,
                    "Switch Account Private To Public": 8,
                    "Standalone Warmup": 9,
                }

                def extract_account_methods_from_newinputs(new_inputs_obj):
                    methods = {}
                    try:
                        if isinstance(new_inputs_obj, dict):
                            for wrap in (new_inputs_obj.get("inputs") or []):
                                for node in (wrap.get("inputs") or []):
                                    if isinstance(node, dict) and node.get("type") == "instagrmFollowerBotAcountWise":
                                        for acc in (node.get("Accounts") or []):
                                            uname = acc.get("username")
                                            # Layer 1: Filter by frontend enabled flag - only enabled accounts get method extraction
                                            if not isinstance(uname, str) or acc.get('enabled', True) is False:
                                                continue
                                            enabled_m = None
                                            for blk in (acc.get("inputs") or []):
                                                if isinstance(blk, dict) and blk.get("input", False):
                                                    name = blk.get("name", "")
                                                    if name in input_name_to_method_map:
                                                        mval = input_name_to_method_map[name]
                                                        if enabled_m is None or (mval != 9 and enabled_m == 9):
                                                            enabled_m = mval
                                            if enabled_m:
                                                methods[uname] = enabled_m
                    except Exception:
                        pass
                    return methods

                account_methods_from_inputs = extract_account_methods_from_newinputs(command.get("newInputs"))
                resolved_method_any = command.get("method")
                try:
                    if isinstance(resolved_method_any, list) and len(resolved_method_any) > 0:
                        base_day_method = int(resolved_method_any[0])
                    else:
                        base_day_method = int(resolved_method_any) if resolved_method_any is not None else 1
                except (ValueError, TypeError):
                    base_day_method = 1

                account_method_map_by_day = {}
                if base_account_usernames:
                    # Use schedule map to build the method map if available (Account-Centric)
                    if 'schedules_map' in locals() and schedules_map:
                        # Iterate 0..6 days
                        for di in range(7):
                            method_map = {}
                            # For each account, find its status on this day
                            for uname, sched in schedules_map.items():
                                # Find the day object for this index
                                day_obj = next((d for d in sched if d.get("dayIndex") == di), None)
                                if not day_obj:
                                    continue
                                
                                is_rest = bool(day_obj.get("isRest"))
                                is_off = bool(day_obj.get("isOff"))
                                plan_method_val = int(day_obj.get("method", 0))
                                if is_off or plan_method_val == 0:
                                    continue  # Account is off/idle this day
                                if is_rest or plan_method_val == 9:
                                    method_map[uname] = 9
                                    continue

                                selected_method = account_methods_from_inputs.get(uname)
                                if selected_method is None:
                                    selected_method = base_day_method
                                method_map[uname] = int(selected_method)
                            
                            if method_map:
                                account_method_map_by_day[di] = method_map
                            
                            # Log distribution
                            m1 = sum(1 for v in method_map.values() if v == 1)
                            m4 = sum(1 for v in method_map.values() if v == 4)
                            warmups = sum(1 for v in method_map.values() if v == 9)
                            logger.info(f"[WEEKLY-METHODS] Day {di} independent account method distribution: method1={m1}, method4={m4}, warmups={warmups}")

                    else:
                        # Old logic (Device-Centric / Single Schedule)
                        for planned_day in planned_schedule:
                            di = int(planned_day.get("dayIndex", 0))
                            is_rest_day = bool(planned_day.get("isRest", False))
                            is_off_day = bool(planned_day.get("isOff", False))
                            planned_method = int(planned_day.get("method", 0 if is_off_day else (9 if is_rest_day else base_day_method)))

                            if is_rest_day or is_off_day or planned_method == 9:
                                account_method_map_by_day[di] = {}
                                continue

                            method_map = {}
                            for uname in base_account_usernames:
                                method_map[uname] = account_methods_from_inputs.get(uname, planned_method)

                            account_method_map_by_day[di] = method_map
                            m1 = sum(1 for v in method_map.values() if v == 1)
                            m4 = sum(1 for v in method_map.values() if v == 4)
                            logger.info(f"[WEEKLY-METHODS] Day {di} account method distribution: method1={m1}, method4={m4}")

                # (Merged per-account method details into the main summary above; no separate method map message)

                # Determine active days for device (Union of all accounts) based on the generated method map
                device_active_days = sorted(account_method_map_by_day.keys())
                last_scheduled_day_index = device_active_days[-1] if device_active_days else None
                test_mode_instant_sent = False

                # Schedule jobs for every day that has at least one active/warmup account
                for day_index in range(7):
                    # Get the account map for this day
                    account_map = account_method_map_by_day.get(day_index)
                    
                    # If no accounts are active/warming up, skip this day
                    if not account_map:
                        continue
                        
                    # Determine aggregate day properties
                    # If any account is active (1 or 4), the device runs in active mode.
                    # If all accounts are warming up (9), the device runs in warmup mode.
                    
                    active_methods = [m for m in account_map.values() if m in (1, 4)]
                    warmup_methods = [m for m in account_map.values() if m == 9]
                    
                    if active_methods:
                        # Active day
                        day_method = active_methods[0] # Base method for device job
                        is_rest = False
                        is_off = False
                    elif warmup_methods:
                        # Warmup only day
                        day_method = 9
                        is_rest = True
                        is_off = False
                    else:
                        # Should not be reachable if account_map is populated
                        continue

                    # Determine Start/End Times & Targets from independent schedules
                    start_times = []
                    end_times = []
                    day_target_sum = 0
                    representative_p = {}
                    
                    if 'schedules_map' in locals() and schedules_map:
                         for uname, sched in schedules_map.items():
                             if uname not in account_map:
                                 continue
                             d_obj = next((d for d in sched if d.get("dayIndex") == day_index), None)
                             if d_obj:
                                 if d_obj.get("start_local"):
                                     # Convert ISO string back to datetime object
                                     start_time_str = d_obj["start_local"]
                                     if isinstance(start_time_str, str):
                                         start_times.append(datetime.fromisoformat(start_time_str))
                                     else:
                                         start_times.append(start_time_str)
                                 if d_obj.get("end_local"):
                                     # Convert ISO string back to datetime object
                                     end_time_str = d_obj["end_local"]
                                     if isinstance(end_time_str, str):
                                         end_times.append(datetime.fromisoformat(end_time_str))
                                     else:
                                         end_times.append(end_time_str)
                                 day_target_sum += int(d_obj.get("target", 0))
                                 # Capture params from the first relevant account (esp. for warmup params)
                                 if not representative_p:
                                     representative_p = d_obj
                                 elif d_obj.get("method") == 9 and representative_p.get("method") != 9:
                                     # Prefer warmup params if we are in warmup mode (though mixing shouldn't happen)
                                     representative_p = d_obj
                    
                    # Fallback: try to pull from planned_schedule (legacy/single-schedule)
                    p_fallback = None
                    if not start_times:
                        p_fallback = next((p for p in planned_schedule if p.get("dayIndex") == day_index), None)
                        if p_fallback and not p_fallback.get("isOff"):
                             start_times.append(p_fallback["start_local"])
                             end_times.append(p_fallback["end_local"])
                             if day_target_sum == 0:
                                 day_target_sum = int(p_fallback.get("target", 0))
                    
                    if not representative_p and p_fallback:
                        representative_p = p_fallback
                    
                    p = representative_p or {}
                    
                    if not start_times:
                        logger.warning(f"[WEEKLY] No start times found for day {day_index}, skipping.")
                        continue

                    # Use the window that covers all accounts (min start, max end)
                    start_time_local = min(start_times)
                    end_time_local = max(end_times)
                    target_count = day_target_sum
                    
                    total_target += target_count
                    if is_rest:
                        warmup_days += 1
                    else:
                        active_days += 1

                    start_time_utc = start_time_local.astimezone(pytz.UTC)
                    end_time_utc = end_time_local.astimezone(pytz.UTC)

                    # Include day_index in job_id to ensure uniqueness across all 7 days
                    job_id = f"weekly_{task_id}_day{day_index}_{start_time_local.strftime('%Y-%m-%d')}"
                    
                    # Deep copy command to prevent shared object references between jobs
                    job_command = copy.deepcopy(command)
                    job_command.update({
                        "job_id": job_id,
                        # For device execution:
                        "dayIndex": day_index,
                        "weeklyLastDayIndex": last_scheduled_day_index,
                        "weeklyRenewalTrigger": bool(last_scheduled_day_index is not None and day_index == last_scheduled_day_index),
                        # Device should rely on per-account caps only
                        "dailyTarget": 0,
                        "method": day_method,  # Base method; per-account overrides provided via accountMethodMap
                    })

                    # For the device, each weekly job should look like a normal ExactStartTime run
                    # so that it follows the same execution path as one-off commands.
                    try:
                        job_command["durationType"] = "ExactStartTime"
                        # Provide an exactStartTime string similar to manual runs
                        try:
                            job_command["exactStartTime"] = start_time_local.strftime("%H:%M")
                        except Exception:
                            pass
                        # Weekly metadata is only needed on the server side; the device can ignore it
                        job_command.pop("newSchecdules", None)
                        job_command.pop("schedules", None)
                    except Exception as duration_patch_err:
                        logger.warning(f"[WEEKLY] Failed to normalize weekly job durationType for device: {duration_patch_err}")

                    account_method_map = account_method_map_by_day.get(day_index, {})
                    job_command["accountMethodMap"] = account_method_map
                    # If warmup day, use the new daily warmup scheduler command type
                    if day_method == 9 and is_rest:
                        job_command["type"] = "WARMUPS_DAILY_START"
                    
                    warmup_summary_text = "Warmup"

                    # Add likes, comments, and duration for rest days (method 9)
                    if day_method == 9 and is_rest:
                        max_likes = int(p.get("maxLikes", 10))
                        max_comments = int(p.get("maxComments", 5))
                        warmup_duration = int(p.get("warmupDuration", 60))
                        job_command.update({
                            "maxLikes": max_likes,
                            "maxComments": max_comments,
                            "warmupDuration": warmup_duration
                        })
                        
                        # Set enabled flags for warmup days based on accountMethodMap
                        account_method_map_warmup = account_method_map_by_day.get(day_index, {})
                        try:
                            warmup_adjusted_inputs = copy.deepcopy(job_command.get("newInputs"))
                            if isinstance(warmup_adjusted_inputs, dict):
                                warmup_inputs_arr = warmup_adjusted_inputs.get("inputs", [])
                                for warmup_wrap in (warmup_inputs_arr or []):
                                    warmup_inner = warmup_wrap.get("inputs", [])
                                    for warmup_node in (warmup_inner or []):
                                        if isinstance(warmup_node, dict) and warmup_node.get("type") == "instagrmFollowerBotAcountWise":
                                            for warmup_acc in (warmup_node.get("Accounts") or []):
                                                warmup_uname = warmup_acc.get("username")
                                                if warmup_uname in account_method_map_warmup:
                                                    warmup_acc["enabled"] = True
                                                else:
                                                    warmup_acc["enabled"] = False
                            job_command["newInputs"] = warmup_adjusted_inputs
                        except Exception as e:
                            logger.warning(f"[WEEKLY] Failed to set enabled flags for warmup day: {e}")
                        warmup_summary_text = f"Warmup - {max_likes} likes, {max_comments} comments, {warmup_duration}min"
                    # Note: method 1 = Follow Suggestions, 4 = Unfollow Non-Followers, 9 = Warmup

                    # Apply per-account caps into newInputs and legacy inputs for active days (method != 9)
                    if day_method != 9 and account_usernames_for_caps and per_account_plans_caps:
                        try:
                            logger.info(f"[WEEKLY-CAPS] Applying per-account caps for day {day_index}, base method {day_method}")
                            logger.info(f"[WEEKLY-CAPS] Accounts: {account_usernames_for_caps}")
                            logger.info(f"[WEEKLY-CAPS] Per-account plans available: {list(per_account_plans_caps.keys())}")

                            account_method_map = job_command.get("accountMethodMap") or {}
                            if account_method_map:
                                method1_count = sum(1 for m in account_method_map.values() if m == 1)
                                method4_count = sum(1 for m in account_method_map.values() if m == 4)
                                logger.info(
                                    f"[WEEKLY-METHODS] Day {day_index} account method map attached: method1={method1_count}, method4={method4_count}"
                                )

                            def compute_caps(target_value):
                                try:
                                    daily_total = int(target_value)
                                except (TypeError, ValueError):
                                    daily_total = 0
                                min_daily = max(1, daily_total - 1)
                                min_hourly = max(1, int(daily_total * 0.2))
                                max_hourly = max(min_hourly + 1, int(daily_total * 0.5))
                                return daily_total, min_daily, min_hourly, max_hourly

                            follow_block_names = {"Follow from Notification Suggestions", "Follow from Profile Posts"}

                            def set_follow_caps(block, target_value):
                                block["input"] = True
                                daily_total, min_daily, min_hourly, max_hourly = compute_caps(target_value)
                                if "minFollowsDaily" in block:
                                    block["minFollowsDaily"] = min_daily
                                if "maxFollowsDaily" in block:
                                    block["maxFollowsDaily"] = daily_total
                                if "minFollowsPerHour" in block:
                                    block["minFollowsPerHour"] = min_hourly
                                if "maxFollowsPerHour" in block:
                                    block["maxFollowsPerHour"] = max_hourly

                            def disable_follow_block(block):
                                block["input"] = False

                            def set_unfollow_caps(block, target_value):
                                block["input"] = True
                                daily_total, min_daily, min_hourly, max_hourly = compute_caps(target_value)
                                block["minFollowsDaily"] = min_daily
                                block["maxFollowsDaily"] = daily_total
                                block["minFollowsPerHour"] = min_hourly
                                block["maxFollowsPerHour"] = max_hourly

                            def disable_unfollow_block(block):
                                block["input"] = False

                            adjusted_inputs = copy.deepcopy(job_command.get("newInputs"))
                            if isinstance(adjusted_inputs, dict):
                                inputs_arr = adjusted_inputs.get("inputs", [])
                                for wrap in (inputs_arr or []):
                                    inner = wrap.get("inputs", [])
                                    for node in (inner or []):
                                        if isinstance(node, dict) and node.get("type") == "instagrmFollowerBotAcountWise":
                                            per_account_map = account_method_map or {}
                                            for acc in (node.get("Accounts") or []):
                                                uname = acc.get("username")
                                                # Process ALL accounts to ensure enabled flag is set correctly for daily schedule
                                                account_inputs = acc.get("inputs") or []
                                                
                                                # Set enabled flag based on whether account is scheduled for this day
                                                if uname in per_account_map:
                                                    acc["enabled"] = True
                                                else:
                                                    acc["enabled"] = False
                                                    # No tasks for this account today – disable all toggle blocks
                                                    for blk in account_inputs:
                                                        if isinstance(blk, dict) and "input" in blk:
                                                            blk["input"] = False
                                                    continue

                                                chosen_method = per_account_map[uname]
                                                v = (per_account_plans_caps.get(uname) or [0] * 7)[day_index]
                                                for blk in account_inputs:
                                                    name = blk.get("name") or ""
                                                    is_unfollow_block = (
                                                        blk.get("type") == "toggleAndUnFollowInputs"
                                                        or name.startswith("Unfollow Non-Followers")
                                                    )
                                                    if chosen_method == 1:
                                                        if name in follow_block_names:
                                                            set_follow_caps(blk, v)
                                                        elif is_unfollow_block:
                                                            disable_unfollow_block(blk)
                                                    elif chosen_method == 4:
                                                        if name in follow_block_names:
                                                            disable_follow_block(blk)
                                                        elif is_unfollow_block:
                                                            set_unfollow_caps(blk, v)
                            job_command["newInputs"] = adjusted_inputs

                            # Also update legacy structure `inputs` so the device (which reads legacy) gets the same per-account caps
                            adjusted_legacy = copy.deepcopy(job_command.get("inputs"))
                            if isinstance(adjusted_legacy, list):
                                per_account_map = account_method_map or {}
                                active_account_entries = []
                                switch_entries = []
                                other_entries = []
                                for acct in adjusted_legacy:
                                    if isinstance(acct, dict) and "username" in acct and "inputs" in acct:
                                        uname_legacy = acct.get("username")
                                        # Process ALL accounts to ensure enabled flag is set correctly for daily schedule
                                        
                                        # Set enabled flag based on whether account is scheduled for this day
                                        if uname_legacy in per_account_map:
                                            acct["enabled"] = True
                                        else:
                                            acct["enabled"] = False
                                        
                                        blocks = acct.get("inputs")
                                        if uname_legacy not in per_account_map:
                                            # Off-schedule account: disable ALL automation inputs so automationType remains empty
                                            if isinstance(blocks, list):
                                                for blk in blocks:
                                                    if isinstance(blk, dict):
                                                        # Disable all possible automation inputs
                                                        automation_inputs = [
                                                            "Follow from Notification Suggestions",
                                                            "Follow from Profile Followers List",
                                                            "Follow from Post",
                                                            "Unfollow Non-Followers",
                                                            "Accept All Follow Requests",
                                                            "Follow from Profile Posts",
                                                            "Switch Account Public To Private",
                                                            "Switch Account Private To Public",
                                                            "Standalone Warmup",
                                                            "Enhanced Warmup",
                                                            "Auto Post"
                                                        ]
                                                        for input_key in automation_inputs:
                                                            if input_key in blk:
                                                                blk[input_key] = False
                                            continue

                                        chosen_method_legacy = per_account_map[uname_legacy]
                                        v = (per_account_plans_caps.get(uname_legacy) or [0] * 7)[day_index]
                                        daily_total, min_daily, min_hourly, max_hourly = compute_caps(v)
                                        if isinstance(blocks, list):
                                            unfollow_configured_legacy = False
                                            for blk in blocks:
                                                if chosen_method_legacy == 1:
                                                    for follow_key in follow_block_names:
                                                        if follow_key in blk:
                                                            blk[follow_key] = True
                                                            blk["minFollowsDaily"] = min_daily
                                                            blk["maxFollowsDaily"] = daily_total
                                                            blk["minFollowsPerHour"] = min_hourly
                                                            blk["maxFollowsPerHour"] = max_hourly
                                                    if "Unfollow Non-Followers" in blk:
                                                        blk["Unfollow Non-Followers"] = False
                                                elif chosen_method_legacy == 4:
                                                    for follow_key in follow_block_names:
                                                        if follow_key in blk:
                                                            blk[follow_key] = False
                                                    if "Unfollow Non-Followers" in blk:
                                                        blk["Unfollow Non-Followers"] = True
                                                        blk["minFollowsDaily"] = min_daily
                                                        blk["maxFollowsDaily"] = daily_total
                                                        blk["minFollowsPerHour"] = min_hourly
                                                        blk["maxFollowsPerHour"] = max_hourly
                                                        unfollow_configured_legacy = True
                                            if chosen_method_legacy == 4 and unfollow_configured_legacy:
                                                logger.info(
                                                    f"[WEEKLY-CAPS] Set unfollow caps for {uname_legacy}: daily={daily_total}, hourly=({min_hourly}-{max_hourly})"
                                                )
                                        active_account_entries.append(acct)
                                        continue

                                    if isinstance(acct, dict) and (
                                        "Switch Account Public To Private" in acct
                                        or "Switch Account Private To Public" in acct
                                    ):
                                        switch_entries.append(acct)
                                    else:
                                        other_entries.append(acct)

                                if per_account_map:
                                    adjusted_legacy = active_account_entries + other_entries + switch_entries
                                job_command["inputs"] = adjusted_legacy
                            else:
                                job_command["inputs"] = adjusted_legacy
                        except Exception as e:
                            logger.warning(f"[WEEKLY] Failed to inject per-account caps: {e}")

                    schedule_accounts = (
                        base_account_usernames
                        or account_usernames_for_caps
                        or list(account_method_map.keys())
                    )
                    per_account_plan_lookup = per_account_plans_caps if isinstance(per_account_plans_caps, dict) else {}
                    daily_schedule_lines = format_daily_schedule_lines(
                        day_index,
                        schedule_accounts,
                        account_method_map,
                        per_account_plan_lookup,
                        warmup_summary_text,
                    )

                    # Ensure ALL accounts are included in payload (not trimmed)
                    # Off-schedule accounts have enabled=false and all inputs disabled
                    # This allows device to match all accounts and build accountUsernames correctly
                    # Device skips accounts with no automation type (all inputs false)

                    # Add job to scheduler
                    try:
                        # Use helper function to schedule or queue
                        schedule_or_queue_job(
                            job_id=job_id,
                            trigger_time_utc=start_time_utc,
                            device_ids=device_ids,
                            command=job_command,
                            job_name=f"Weekly day {day_index} for devices {device_ids}",
                            job_type="command"
                        )
                        logger.info(f"[WEEKLY] 📌 Job {job_id} scheduled for {start_time_utc} (UTC) / {start_time_local.strftime('%Y-%m-%d %H:%M:%S')} ({time_zone})")
                        
                        scheduled_job_ids.append(job_id)
                        # Update DB activeJobs immediately
                        job_instance = {
                            "job_id": job_id,
                            "startTime": start_time_utc,
                            "endTime": end_time_utc,
                            "device_ids": device_ids,
                            "deliveryStatus": "pending",
                            "deliveryAttempts": 0,
                            "method": day_method,
                            "dayIndex": day_index,
                            "durationType": durationType,
                            "accountMethodMap": account_method_map,
                        }
                        
                        # Set nextRunTime only for the first scheduled job
                        # Use asyncio.to_thread to prevent blocking the event loop
                        if not first_job_scheduled:
                            await asyncio.to_thread(
                                tasks_collection.update_one,
                                {"id": task_id},
                                {
                                    "$set": {
                                        "status": "scheduled",
                                        "nextRunTime": start_time_utc.isoformat()  # Set next run time to first job's start time
                                    },
                                    "$unset": {"scheduledTime": ""},
                                    "$push": {"activeJobs": job_instance},
                                },
                            )
                            # Don't set first_job_scheduled = True here - it will be set after the reminder is sent
                        else:
                            # For subsequent jobs, only push to activeJobs
                            await asyncio.to_thread(
                                tasks_collection.update_one,
                                {"id": task_id},
                                {
                                    "$push": {"activeJobs": job_instance},
                                },
                            )
                        # Optional per-day notification (disabled by default for weekly plans)
                        if notify_daily:
                            task_for_notify = await asyncio.to_thread(
                                tasks_collection.find_one,
                                {"id": task_id}
                            )
                            device_docs = await asyncio.to_thread(
                                list,
                                device_collection.find(
                                    {"deviceId": {"$in": device_ids}},
                                    {"deviceId": 1, "deviceName": 1, "_id": 0},
                                )
                            )
                            device_names = [doc.get("deviceName", "Unknown Device") for doc in device_docs]
                            asyncio.create_task(
                                send_schedule_notification(
                                    task_for_notify,
                                    device_names,
                                    start_time_local,
                                    end_time_local,
                                    time_zone,
                                    job_id,
                                )
                            )
                        # Log with actual calendar date for clarity
                        day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                        actual_day_name = day_names[start_time_local.weekday()]
                        logger.info(
                            f"[WEEKLY] 📅 Scheduling Day {day_index} ({actual_day_name} {start_time_local.strftime('%b %d')}): start={start_time_local.strftime('%H:%M')} end={end_time_local.strftime('%H:%M')} target={target_count} method={day_method} rest={is_rest}"
                        )
                        
                        # Schedule daily schedule reminder (one per day)
                        reminder_job_id = f"reminder_{job_id}"
                        schedule_lines_payload = daily_schedule_lines or [f"Device target: {target_count} {get_method_unit(day_method)}"]
                        
                        # Send first day reminder immediately when weekly plan is created (async to avoid blocking)
                        if not first_job_scheduled:
                            try:
                                # Call async version directly since we're in an async context
                                asyncio.create_task(
                                    _send_weekly_reminder_async(
                                        task_id,
                                        actual_day_name,
                                        start_time_local.strftime('%Y-%m-%d %H:%M'),
                                        schedule_lines_payload,
                                        time_zone,
                                        False,
                                        "Immediate (First Day Schedule)",
                                    )
                                )
                                logger.info(f"[WEEKLY] ⏰ Queued immediate daily schedule reminder for first day (Day {day_index})")
                            except Exception as reminder_err:
                                logger.warning(f"[WEEKLY] Could not queue immediate first day reminder: {reminder_err}")
                            # Set flag immediately since reminder is queued (not waiting for completion)
                            first_job_scheduled = True
                        else:
                            # For other days, send reminder 5 hours before start, or immediately if less than 5 hours away
                            reminder_time_utc = start_time_utc - timedelta(hours=5)
                            now_utc = datetime.now(pytz.UTC)
                            
                            # If 5 hours before is in the past (less than 5 hours away), send immediately (async to avoid blocking)
                            if reminder_time_utc <= now_utc:
                                try:
                                    # Call async version directly since we're in an async context
                                    asyncio.create_task(
                                        _send_weekly_reminder_async(
                                            task_id,
                                            actual_day_name,
                                            start_time_local.strftime('%Y-%m-%d %H:%M'),
                                            schedule_lines_payload,
                                            time_zone,
                                            False,
                                            "Immediate (Less than 5 hours away)",
                                        )
                                    )
                                    logger.info(f"[WEEKLY] ⏰ Queued immediate daily schedule reminder for Day {day_index} (less than 5 hours away)")
                                except Exception as reminder_err:
                                    logger.warning(f"[WEEKLY] Could not queue immediate reminder: {reminder_err}")
                            else:
                                # Schedule for 5 hours before start
                                try:
                                    # Use helper function to schedule or queue reminder
                                    reminder_command = {
                                        "reminder_type": "weekly_schedule",
                                        "task_id": task_id,
                                        "day_name": actual_day_name,
                                        "start_time": start_time_local.strftime('%Y-%m-%d %H:%M'),
                                        "schedule_lines": schedule_lines_payload,
                                        "time_zone": time_zone,
                                    }
                                    schedule_or_queue_job(
                                        job_id=reminder_job_id,
                                        trigger_time_utc=reminder_time_utc,
                                        device_ids=[],
                                        command=reminder_command,
                                        job_name=f"Reminder for day {day_index} of task {task_id}",
                                        job_type="weekly_reminder"
                                    )
                                    logger.info(f"[WEEKLY] ⏰ Scheduled daily schedule reminder for Day {day_index}: 5 hours before start")
                                except Exception as reminder_err:
                                    logger.warning(f"[WEEKLY] Could not schedule reminder: {reminder_err}")
                    except Exception as e:
                        # Cleanup previously scheduled jobs if failure
                        for jid in scheduled_job_ids:
                            try:
                                scheduler.remove_job(jid)
                            except Exception:
                                pass
                        tasks_collection.update_one({"id": task_id}, {"$set": {"activeJobs": []}})
                        raise HTTPException(status_code=500, detail=f"Failed to schedule weekly job: {str(e)}")

                logger.info(
                    f"[WEEKLY] ✅ Weekly plan scheduled: {task_id} | Total weekly target: {total_target} | Active days: {active_days} | Warmup days: {warmup_days} | Rest days: {7 - active_days - warmup_days}"
                )

                # Invalidate cache for immediate frontend refresh
                try:
                    from routes.tasks import invalidate_user_tasks_cache
                    user_email = current_user.get("email")
                    if user_email:
                        # Clear all related caches immediately
                        invalidate_user_tasks_cache(user_email)
                        
                        # Also clear specific endpoint caches for immediate refresh
                        redis_client.delete(f"tasks:list:{user_email}:*")
                        redis_client.delete(f"tasks:scheduled:{user_email}")
                        redis_client.delete(f"tasks:running:{user_email}")
                        
                        logger.info(f"[WEEKLY] Cache cleared for user {user_email} after scheduling weekly plan")
                except Exception as cache_error:
                    logger.warning(f"[WEEKLY] Cache invalidation failed: {cache_error}")

            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"[WEEKLY] Error scheduling weekly plan: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        # Schedule persistence logic for schedule_task commands

        command_type = request.command.get("type", "")

        if command_type in ["schedule_task"]:

            # Extract schedule details from the command

            schedule_data = command.get("schedule", {})

            exact_start_time = schedule_data.get("exactStartTime") or request.command.get("exactStartTime")

            time_zone_for_schedule = schedule_data.get("timeZone", time_zone)

            
            
            if exact_start_time:

                try:

                    # Convert scheduled time to UTC for consistent storage

                    local_tz = pytz.timezone(time_zone_for_schedule)

                    utc_tz = pytz.UTC

                    
                    
                    # Parse the local time and convert to UTC

                    local_dt = datetime.fromisoformat(exact_start_time.replace('Z', ''))

                    if local_dt.tzinfo is None:

                        local_dt = local_tz.localize(local_dt)

                    utc_dt = local_dt.astimezone(utc_tz)

                    
                    
                    # Update the task with schedule information

                    task_name = command.get("taskName")

                    if task_name:

                        update_result = await asyncio.to_thread(

                            tasks_collection.update_one,

                            {"taskName": task_name, "email": current_user.get("email")},

                            {

                                "$set": {

                                    "scheduledTime": utc_dt.isoformat(),

                                    "lastScheduleUpdate": datetime.utcnow().isoformat(),

                                    "scheduleTimeZone": time_zone_for_schedule,

                                    "exactStartTime": exact_start_time

                                }

                            }

                        )

                        
                        
                        if update_result.modified_count > 0:

                            print(f"[DEBUG] Successfully updated schedule for task {task_name}")

                            # Force immediate cache refresh for all related endpoints

                            try:

                                from routes.tasks import invalidate_user_tasks_cache

                                user_email = current_user.get("email")

                                # Clear all related caches immediately

                                invalidate_user_tasks_cache(user_email)

                                
                                
                                # Also clear specific endpoint caches for immediate refresh

                                redis_client.delete(f"tasks:list:{user_email}:*")

                                redis_client.delete(f"tasks:scheduled:{user_email}")

                                redis_client.delete(f"tasks:running:{user_email}")

                                
                                
                                print(f"[CACHE] All caches cleared after schedule update for {task_name}")

                            except Exception as cache_error:

                                print(f"[WARNING] Cache clearing failed: {cache_error}")

                        else:

                            # Try updating by task_id if taskName didn't work

                            if task_id:

                                update_result = await asyncio.to_thread(

                                    tasks_collection.update_one,

                                    {"id": task_id, "email": current_user.get("email")},

                                    {

                                        "$set": {

                                            "scheduledTime": utc_dt.isoformat(),

                                            "lastScheduleUpdate": datetime.utcnow().isoformat(),

                                            "scheduleTimeZone": time_zone_for_schedule,

                                            "exactStartTime": exact_start_time

                                        }

                                    }

                                )

                                if update_result.modified_count > 0:

                                    print(f"[DEBUG] Successfully updated schedule for task ID {task_id}")

                                    # Force immediate cache refresh for all related endpoints

                                    try:

                                        from routes.tasks import invalidate_user_tasks_cache

                                        user_email = current_user.get("email")

                                        # Clear all related caches immediately

                                        invalidate_user_tasks_cache(user_email)

                                        
                                        
                                        # Also clear specific endpoint caches for immediate refresh

                                        redis_client.delete(f"tasks:list:{user_email}:*")

                                        redis_client.delete(f"tasks:scheduled:{user_email}")

                                        redis_client.delete(f"tasks:running:{user_email}")

                                        
                                        
                                        print(f"[CACHE] All caches cleared after schedule update for task ID {task_id}")

                                    except Exception as cache_error:

                                        print(f"[WARNING] Cache clearing failed: {cache_error}")

                                else:

                                    print(f"[WARNING] Could not find task to update schedule: {task_name or task_id}")
                    
                    

                except Exception as e:

                    print(f"[ERROR] Failed to persist schedule update: {str(e)}")



        return {"message": "Command scheduled successfully"}



    except pytz.exceptions.UnknownTimeZoneError:

        raise HTTPException(status_code=400, detail=f"Invalid timezone: {time_zone}")

    except ValueError as e:

        raise HTTPException(status_code=400, detail=f"Invalid time format: {str(e)}")

    except Exception as e:

        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")





def register_device(device_data: DeviceRegistration):

    device = device_collection.find_one({"deviceId": device_data.deviceId})

    if device:

        raise HTTPException(status_code=400, detail="Device already registered")

    device_collection.insert_one(device_data.dict())

    return {

        "message": "Device registered successfully",

        "deviceId": device_data.deviceId,

    }





async def _send_weekly_reminder_async(
    task_id,
    day_name,
    start_time_formatted,
    schedule_lines,
    time_zone,
    test_mode: bool = False,
    time_before_label: Optional[str] = None,
):
    """
    Async implementation of weekly reminder - runs non-blocking operations.
    """
    try:
        from Bot.discord_bot import get_bot_instance
        
        # Fetch task details from database (async to avoid blocking)
        task = await asyncio.to_thread(
            tasks_collection.find_one,
            {"id": task_id},
            {"taskName": 1, "serverId": 1, "channelId": 1, "status": 1, "_id": 0}
        )
        
        # Skip if task unscheduled or missing discord config
        if not task or not task.get("serverId") or not task.get("channelId"):
            logger.warning(f"[WEEKLY-REMINDER] Cannot send reminder: task {task_id} missing Discord config")
            return
        task_name = task.get("taskName", "Weekly Task")
        server_id = task.get("serverId")
        channel_id = task.get("channelId")
        
        # Format message based on mode
        time_before = (
            time_before_label
            if time_before_label
            else ("Instant (Test Mode)" if test_mode else "2 Hours Before Start")
        )
        schedule_body = "\n".join(schedule_lines or []).strip() or "No accounts scheduled."
        message = (
            f"📋 **Daily Schedule Reminder**\n"
            f"⏱️ **Notification**: {time_before}\n"
            f"📅 **Day**: {day_name}\n"
            f"⏰ **Start Time**: {start_time_formatted} ({time_zone})\n"
            f"🧩 **Task**: {task_name}\n"
            f"\n**Accounts**:\n{schedule_body}"
        )
        
        # Send via Discord bot (async to avoid blocking)
        bot = get_bot_instance()
        await bot.send_message({
            "message": message,
            "task_id": task_id,
            "job_id": f"reminder_{task_id}",
            "server_id": int(server_id) if isinstance(server_id, str) and server_id.isdigit() else server_id,
            "channel_id": int(channel_id) if isinstance(channel_id, str) and channel_id.isdigit() else channel_id,
            "type": "info",
        })
        
        logger.info(f"[WEEKLY-REMINDER] Sent daily schedule reminder for task {task_id}, day {day_name}")
    
    except Exception as e:
        logger.error(f"[WEEKLY-REMINDER] Failed to send reminder: {e}")


def send_weekly_reminder(
    task_id,
    day_name,
    start_time_formatted,
    schedule_lines,
    time_zone,
    test_mode: bool = False,
    time_before_label: Optional[str] = None,
):
    """
    Thread-safe wrapper for sending weekly reminders.
    When called from scheduler (threadpool), creates event loop and runs async version.
    When called directly, runs async version in current context.
    """
    try:
        # Check if we're already in an event loop
        try:
            current_loop = asyncio.get_event_loop()
            if current_loop.is_running():
                # We're in a running event loop (e.g., from API request), create new one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(
                        _send_weekly_reminder_async(
                            task_id,
                            day_name,
                            start_time_formatted,
                            schedule_lines,
                            time_zone,
                            test_mode,
                            time_before_label,
                        )
                    )
                finally:
                    loop.close()
            else:
                # We have a loop but it's not running, use it
                return current_loop.run_until_complete(
                    _send_weekly_reminder_async(
                        task_id,
                        day_name,
                        start_time_formatted,
                        schedule_lines,
                        time_zone,
                        test_mode,
                        time_before_label,
                    )
                )
        except RuntimeError:
            # No event loop exists, create one (e.g., from scheduler threadpool)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(
                    _send_weekly_reminder_async(
                        task_id,
                        day_name,
                        start_time_formatted,
                        schedule_lines,
                        time_zone,
                        test_mode,
                        time_before_label,
                    )
                )
            finally:
                loop.close()
    except Exception as e:
        logger.error(f"[WEEKLY-REMINDER] Wrapper failed: {e}")


def wrapper_for_send_command(device_ids, command):

    """

    Thread-safe wrapper for sending commands to devices with distributed locking
    
    Uses Redis lock to ensure only ONE worker executes each job, even if multiple schedulers are running.

    """

    job_id = command.get("job_id", "unknown")
    task_id = command.get("task_id", "unknown")
    
    # Redis lock key for this job execution
    lock_key = f"job_execution_lock:{job_id}"
    lock_timeout = 300  # 5 minutes - enough time for job execution
    
    # Try to acquire distributed lock (only one worker can execute)
    # Use SET with NX (only if not exists) and EX (expiration) for atomic lock acquisition
    lock_acquired = redis_client.set(lock_key, WORKER_ID, nx=True, ex=lock_timeout)
    
    if not lock_acquired:
        # Another worker is already executing this job
        current_executor = redis_client.get(lock_key) or "unknown"
        logger.info(f"[SCHEDULER-JOB] ⏭️ Skipping job {job_id} - already being executed by worker {current_executor}")
        return None
    
    logger.info(f"[SCHEDULER-JOB] 🔒 Lock acquired for job {job_id} by worker {WORKER_ID}")
    
    try:
        # ✅ CRITICAL FIX: Use run_coroutine_threadsafe to execute on the MAIN event loop
        # WebSockets (device_connections) were created on the Main Uvicorn Event Loop
        # Creating a new loop with asyncio.run() would cause RuntimeError: Task attached to different loop
        # This allows access to the existing WebSockets from the scheduler threadpool
        
        # Verify main_event_loop is available and running
        if main_event_loop is None:
            logger.error(f"[SCHEDULER-JOB] ❌ main_event_loop is None for job {job_id}")
            raise RuntimeError("main_event_loop is not initialized. Cannot execute job.")
        
        if not main_event_loop.is_running():
            logger.error(f"[SCHEDULER-JOB] ❌ main_event_loop is not running for job {job_id}")
            raise RuntimeError("main_event_loop is not running. Cannot execute job.")
        
        # Execute the coroutine on the main event loop
        # This ensures WebSocket operations work correctly
        future = asyncio.run_coroutine_threadsafe(
            send_command_to_devices(device_ids, command),
            main_event_loop
        )
        
        # Wait for the result (blocking this thread, which is fine for APScheduler threadpool)
        # Timeout of 300 seconds (5 minutes) matches the lock timeout
        result = future.result(timeout=300)
        
        logger.info(f"[SCHEDULER-JOB] ✅ Job {job_id} completed successfully")
        return result

    except Exception as e:
        logger.error(f"[SCHEDULER-JOB] ❌ Job {job_id} failed with error: {e}", exc_info=True)
        import traceback
        traceback.print_exc()
        raise  # Re-raise to let APScheduler handle it
    finally:
        # Always release the lock after execution (or on error)
        try:
            # Only delete if we still own the lock (check value matches our worker ID)
            current_lock_value = redis_client.get(lock_key)
            if current_lock_value == WORKER_ID:
                redis_client.delete(lock_key)
                logger.info(f"[SCHEDULER-JOB] 🔓 Lock released for job {job_id}")
            else:
                logger.warning(f"[SCHEDULER-JOB] ⚠️ Lock for job {job_id} was already released or taken by another worker")
        except Exception as lock_err:
            logger.warning(f"[SCHEDULER-JOB] ⚠️ Failed to release lock for job {job_id}: {lock_err}")





async def send_command_to_devices(device_ids, command):

    """Send command to devices with improved async handling"""

    logger.info(f"Executing command for devices: {device_ids}")



    # Import here to avoid circular imports

    from Bot.discord_bot import get_bot_instance



    task_id = command.get("task_id")

    job_id = command.get("job_id")

    is_recurring = command.get("isRecurring", False)
    
    # LOG: Verify weekly plan fields are in command
    method_value = command.get("method")
    daily_target = command.get("dailyTarget")
    day_index = command.get("dayIndex")
    logger.info(f"[WEEKLY-DEBUG] Command received - method: {method_value}, dailyTarget: {daily_target}, dayIndex: {day_index}, task_id: {task_id}")
    
    # Map method number to input name
    method_to_input_map = {
        1: "Follow from Notification Suggestions",
        2: "Follow from Profile Followers List",
        3: "Follow from Post",
        4: "Unfollow Non-Followers",
        5: "Accept All Follow Requests",
        6: "Follow from Profile Posts",
        7: "Switch Account Public To Private",
        8: "Switch Account Private To Public",
        9: "Standalone Warmup",
    }
    
    # Modify inputs to enable the correct method and disable others
    # Check for per-account method map (weekly randomization)
    account_method_map = command.get("accountMethodMap", {})
    
    if method_value in method_to_input_map:
        target_input_name = method_to_input_map[method_value]
        
        if account_method_map:
            logger.info(f"[METHOD-ENABLE] Using per-account method map for task {task_id}")
        else:
            logger.info(f"[METHOD-ENABLE] Detected method: {method_value} for task {task_id}, enabling '{target_input_name}'")
        
        # Modify legacy inputs format
        inputs_list = command.get("inputs", [])
        if isinstance(inputs_list, list):
            for account_data in inputs_list:
                if isinstance(account_data, dict) and "inputs" in account_data:
                    username = account_data.get("username")
                    account_inputs = account_data.get("inputs")
                    if not isinstance(account_inputs, list):
                        continue

                    # In weekly/per-account mode, ONLY accounts present in account_method_map
                    # should have an active method. Others must have all methods disabled.
                    if account_method_map:
                        if username not in account_method_map:
                            # Disable all methods for accounts not scheduled today
                            for block in account_inputs:
                                if isinstance(block, dict):
                                    for method_name in method_to_input_map.values():
                                        if method_name in block:
                                            block[method_name] = False
                            continue
                        account_method = account_method_map.get(username)
                    else:
                        # Non-weekly flows: fall back to global method for all accounts
                        account_method = method_value

                    account_target_name = method_to_input_map.get(account_method, target_input_name)

                    for block in account_inputs:
                        if isinstance(block, dict):
                            # Disable all methods
                            for method_name in method_to_input_map.values():
                                if method_name in block:
                                    block[method_name] = False
                            # Enable the target method for this account
                            if account_target_name in block:
                                block[account_target_name] = True
                                if account_method == 9:
                                    # Use randomized warmup parameters from command
                                    warmup_duration = command.get("warmupDuration", 60)
                                    max_likes = command.get("maxLikes", 10)
                                    max_comments = command.get("maxComments", 5)
                                    block["warmupDuration"] = warmup_duration
                                    block["maxLikes"] = max_likes
                                    block["maxComments"] = max_comments
                                if account_method_map:
                                    logger.info(f"[METHOD-ENABLE] {username}: method {account_method} → '{account_target_name}'")
                                else:
                                    logger.info(f"[METHOD-ENABLE] Enabled '{account_target_name}' in legacy inputs")
        
        # Also modify newInputs format (account-wise structure)
        new_inputs = command.get("newInputs")
        if isinstance(new_inputs, dict):
            inputs_wrapper = new_inputs.get("inputs", [])
            if isinstance(inputs_wrapper, list):
                for entry in inputs_wrapper:
                    if isinstance(entry, dict):
                        inner_inputs = entry.get("inputs", [])
                        if isinstance(inner_inputs, list):
                            for node in inner_inputs:
                                # Account-wise structure
                                if isinstance(node, dict) and node.get("type") == "instagrmFollowerBotAcountWise":
                                    accounts = node.get("Accounts", [])
                                    if isinstance(accounts, list):
                                        for account in accounts:
                                            username = account.get("username")
                                            acc_inputs = account.get("inputs", [])
                                            if not isinstance(acc_inputs, list):
                                                continue

                                            # In weekly/per-account mode, ONLY accounts present in account_method_map
                                            # should have an active method. Others must have all methods disabled.
                                            if account_method_map:
                                                if username not in account_method_map:
                                                    # Disable all methods for accounts not scheduled today
                                                    for block in acc_inputs:
                                                        if isinstance(block, dict):
                                                            name = block.get("name", "")
                                                            if name in method_to_input_map.values():
                                                                block["input"] = False
                                                    continue
                                                account_method = account_method_map.get(username)
                                            else:
                                                # Non-weekly flows: fall back to global method for all accounts
                                                account_method = method_value

                                            account_target_name = method_to_input_map.get(account_method, target_input_name)

                                            for block in acc_inputs:
                                                if isinstance(block, dict):
                                                    name = block.get("name", "")
                                                    # Disable all methods
                                                    if name in method_to_input_map.values():
                                                        block["input"] = False
                                                    # Enable the target method for this account
                                                    if name == account_target_name:
                                                        block["input"] = True
                                                        if account_method == 9:
                                                            # Use randomized warmup parameters from command
                                                            warmup_duration = command.get("warmupDuration", 60)
                                                            max_likes = command.get("maxLikes", 10)
                                                            max_comments = command.get("maxComments", 5)
                                                            block["warmupDuration"] = warmup_duration
                                                            block["maxLikes"] = max_likes
                                                            block["maxComments"] = max_comments
                                                        if account_method_map:
                                                            logger.info(f"[METHOD-ENABLE] {username}: method {account_method} → '{account_target_name}' (account-wise)")
                                                        else:
                                                            logger.info(f"[METHOD-ENABLE] Enabled '{account_target_name}' (account-wise)")

        skip_payload_filter = bool(account_method_map)

        if skip_payload_filter:
            logger.info("[PAYLOAD-FILTER] Skipping account trimming - weekly account map present (per-day enable/disable handled upstream)")
        else:
            # Filter out accounts with enabled=false or all inputs false to prevent app from getting stuck
            def has_active_automation(account_data, is_new_inputs_format=False):
                """Check if account has enabled=true and at least one automation input set to true"""
                # Check enabled flag first
                enabled = account_data.get("enabled", True)  # Default to True if not specified
                if enabled is False:
                    return False
                
                # Get account inputs
                account_inputs = account_data.get("inputs", [])
                if not isinstance(account_inputs, list):
                    return False
                
                # List of automation input names to check
                automation_input_names = [
                    "Follow from Notification Suggestions",
                    "Follow from Profile Followers List",
                    "Follow from Post",
                    "Unfollow Non-Followers",
                    "Accept All Follow Requests",
                    "Follow from Profile Posts",
                    "Switch Account Public To Private",
                    "Switch Account Private To Public",
                    "Standalone Warmup",
                    "Enhanced Warmup",
                    "Auto Post"
                ]
                
                # Check if at least one automation input is enabled
                for block in account_inputs:
                    if not isinstance(block, dict):
                        continue
                    
                    if is_new_inputs_format:
                        # New format: check "input" field
                        block_name = block.get("name", "")
                        if block_name in automation_input_names:
                            if block.get("input") is True:
                                return True
                    else:
                        # Legacy format: check direct boolean fields
                        for input_name in automation_input_names:
                            if input_name in block and block[input_name] is True:
                                return True
                
                return False
                

            # Filter accounts from newInputs structure
            filtered_new_inputs_count = 0
            new_inputs_filtered = command.get("newInputs")
            if isinstance(new_inputs_filtered, dict):
                inputs_wrapper_filtered = new_inputs_filtered.get("inputs", [])
                if isinstance(inputs_wrapper_filtered, list):
                    for entry_filtered in inputs_wrapper_filtered:
                        if isinstance(entry_filtered, dict):
                            inner_inputs_filtered = entry_filtered.get("inputs", [])
                            if isinstance(inner_inputs_filtered, list):
                                for node_filtered in inner_inputs_filtered:
                                    if isinstance(node_filtered, dict) and node_filtered.get("type") == "instagrmFollowerBotAcountWise":
                                        accounts_filtered = node_filtered.get("Accounts", [])
                                        if isinstance(accounts_filtered, list):
                                            original_count = len(accounts_filtered)
                                            # Filter accounts that don't have active automation
                                            filtered_accounts = [
                                                acc for acc in accounts_filtered
                                                if has_active_automation(acc, is_new_inputs_format=True)
                                            ]
                                            filtered_count = original_count - len(filtered_accounts)
                                            filtered_new_inputs_count += filtered_count
                                            if filtered_count > 0:
                                                filtered_usernames = [
                                                    acc.get("username", "unknown") 
                                                    for acc in accounts_filtered 
                                                    if not has_active_automation(acc, is_new_inputs_format=True)
                                                ]
                                                logger.info(f"[PAYLOAD-FILTER] Filtered {filtered_count} accounts from newInputs: {', '.join(filtered_usernames)} (enabled=false or all inputs false)")
                                            node_filtered["Accounts"] = filtered_accounts

            # Filter accounts from legacy inputs structure
            filtered_legacy_count = 0
            legacy_inputs = command.get("inputs")
            if isinstance(legacy_inputs, list):
                original_legacy_count = len(legacy_inputs)
                filtered_legacy = []
                filtered_legacy_usernames = []
                for acct in legacy_inputs:
                    if isinstance(acct, dict):
                        if has_active_automation(acct, is_new_inputs_format=False):
                            filtered_legacy.append(acct)
                        else:
                            filtered_legacy_usernames.append(acct.get("username", "unknown"))
                filtered_legacy_count = original_legacy_count - len(filtered_legacy)
                if filtered_legacy_count > 0:
                    logger.info(f"[PAYLOAD-FILTER] Filtered {filtered_legacy_count} accounts from legacy inputs: {', '.join(filtered_legacy_usernames)} (enabled=false or all inputs false)")
                command["inputs"] = filtered_legacy

            # Log final account counts
            if filtered_new_inputs_count > 0 or filtered_legacy_count > 0:
                total_filtered = filtered_new_inputs_count + filtered_legacy_count
                logger.info(f"[PAYLOAD-FILTER] Total accounts filtered: {total_filtered} (newInputs: {filtered_new_inputs_count}, legacy: {filtered_legacy_count})")
            else:
                # Log that no filtering was needed
                logger.info(f"[PAYLOAD-FILTER] No accounts filtered - all accounts have active automation")

        # Reorder inputs array: account objects first, switch objects last
        # Android app expects accounts before switch objects (skips last 2 objects: size() - 2)
        legacy_inputs_ordered = command.get("inputs")
        if isinstance(legacy_inputs_ordered, list):
            account_objects = []
            switch_objects = []
            other_objects = []
            
            for item in legacy_inputs_ordered:
                if not isinstance(item, dict):
                    other_objects.append(item)
                    continue
                
                # Check if it's an account object (has "username" and "inputs" keys)
                if "username" in item and "inputs" in item:
                    account_objects.append(item)
                # Check if it's a switch object (has "Switch Account Public To Private" or "Switch Account Private To Public" keys)
                elif "Switch Account Public To Private" in item or "Switch Account Private To Public" in item:
                    switch_objects.append(item)
                else:
                    other_objects.append(item)
            
            # Reorder: accounts first, then other objects, then switch objects last
            reordered_inputs = account_objects + other_objects + switch_objects
            if len(reordered_inputs) != len(legacy_inputs_ordered):
                logger.warning(f"[PAYLOAD-ORDER] Inputs array length mismatch after reordering: {len(legacy_inputs_ordered)} -> {len(reordered_inputs)}")
            else:
                if account_objects and switch_objects:
                    logger.info(f"[PAYLOAD-ORDER] Reordered inputs: {len(account_objects)} accounts, {len(other_objects)} other, {len(switch_objects)} switches")
                command["inputs"] = reordered_inputs



    try:

        # Persist per-day follow caps into DB inputs based on dailyTarget (weekly flow)
        try:
            task_id_local = command.get("task_id")
            method_local = command.get("method")
            daily_target = command.get("dailyTarget")
            # Check if this is a weekly task with dailyTarget set
            if task_id_local and isinstance(method_local, int) and daily_target is not None:
                target_cap = int(daily_target)
                # Skip rest days (dailyTarget: 0) AND warmup days (method: 9)
                if target_cap > 0 and method_local != 9:

                    # Use fresh inputs from command instead of DB (DB might have warmup-modified inputs)
                    inputs_obj = command.get("newInputs")
                    logger.info(f"[WEEKLY] Using fresh inputs from command for day caps (method={method_local}, target={target_cap})")

                    if inputs_obj is not None:
                        adjusted = copy.deepcopy(inputs_obj)
                        is_dict_wrapper_job = False  # Track if we need to re-wrap

                        def set_follow_daily(block):
                            if isinstance(block, dict):
                                if "minFollowsDaily" in block:
                                    block["minFollowsDaily"] = max(1, target_cap - 1)  # min = max - 1
                                if "maxFollowsDaily" in block:
                                    block["maxFollowsDaily"] = target_cap
                                # Calculate dynamic hourly limits based on daily target
                                # Formula: min = daily * 0.2, max = daily * 0.5 (allows reaching target in 2-5 hours)
                                min_hourly = max(1, int(target_cap * 0.2))
                                max_hourly = max(min_hourly + 1, int(target_cap * 0.5))
                                if "minFollowsPerHour" in block:
                                    block["minFollowsPerHour"] = min_hourly
                                if "maxFollowsPerHour" in block:
                                    block["maxFollowsPerHour"] = max_hourly

                        def set_unfollow_daily(block):
                            if isinstance(block, dict):
                                # ⚠️ CRITICAL: Android expects "minFollowsDaily" NOT "minUnFollowsDaily"
                                # Android reuses follow field names for unfollowing (quirk of the codebase)
                                block["minFollowsDaily"] = max(1, target_cap - 1)  # min = max - 1
                                block["maxFollowsDaily"] = target_cap
                                # Calculate dynamic hourly limits based on daily target
                                # Formula: min = daily * 0.2, max = daily * 0.5 (allows reaching target in 2-5 hours)
                                min_hourly = max(1, int(target_cap * 0.2))
                                max_hourly = max(min_hourly + 1, int(target_cap * 0.5))
                                block["minFollowsPerHour"] = min_hourly
                                block["maxFollowsPerHour"] = max_hourly

                        # Handle dict structure (new format from command.newInputs)
                        if isinstance(adjusted, dict) and "inputs" in adjusted:
                            is_dict_wrapper_job = True
                            inputs_array_job = adjusted["inputs"]  # Work with the list
                        else:
                            inputs_array_job = adjusted

                        # Handle both legacy and account-wise structures
                        if isinstance(inputs_array_job, list):
                            for entry in inputs_array_job:
                                inner = entry.get("inputs")
                                if isinstance(inner, list):
                                    for node in inner:
                                        # Check if this is the account-wise structure
                                        if node.get("type") == "instagrmFollowerBotAcountWise" and isinstance(node.get("Accounts"), list):
                                            # Navigate into Accounts array
                                            for acc in node["Accounts"]:
                                                acc_inputs = acc.get("inputs")
                                                if isinstance(acc_inputs, list):
                                                    for block in acc_inputs:
                                                        name = block.get("name") or ""
                                                        enabled = block.get("input") is True
                                                        if enabled:
                                                            # Method 4 = Unfollow Non-Followers
                                                            if method_local == 4 and name == "Unfollow Non-Followers":
                                                                set_unfollow_daily(block)
                                                            # Methods 1, 2, 3, 6 = Follow methods
                                                            elif method_local in [1, 2, 3, 6] and name in (
                                                                "Follow from Notification Suggestions",
                                                                "Follow from Profile Followers List",
                                                                "Follow from Post",
                                                                "Follow from Profile Posts",
                                                            ):
                                                                set_follow_daily(block)
                                        else:
                                            # Legacy structure: blocks directly in inputs
                                            block = node
                                            name = block.get("name") or ""
                                            enabled_toggle = (
                                                block.get("input") is True
                                                or block.get("Follow from Notification Suggestions") is True
                                            )
                                            if enabled_toggle:
                                                # Method 4 = Unfollow Non-Followers
                                                if method_local == 4 and name == "Unfollow Non-Followers":
                                                    set_unfollow_daily(block)
                                                # Methods 1, 2, 3, 6 = Follow methods
                                                elif method_local in [1, 2, 3, 6] and name in (
                                                    "Follow from Notification Suggestions",
                                                    "Follow from Profile Followers List",
                                                    "Follow from Post",
                                                    "Follow from Profile Posts",
                                                ):
                                                    set_follow_daily(block)

                        # Re-wrap if it was originally a dict
                        if is_dict_wrapper_job:
                            adjusted["inputs"] = inputs_array_job  # Put modified list back in wrapper
                        else:
                            adjusted = inputs_array_job  # Use list directly

                        # Persist adjusted inputs
                        await asyncio.to_thread(
                            tasks_collection.update_one,
                            {"id": task_id_local},
                            {"$set": {"inputs": adjusted}}
                        )
        except Exception as _cap_err:
            logger.warning(f"[WEEKLY] Failed to persist per-day caps: {_cap_err}")
        logger.info(f"Retrieving task with ID: {task_id}")

        # Use asyncio.to_thread for blocking database operations

        task = await asyncio.to_thread(

            tasks_collection.find_one,

            {"id": task_id},

            {"serverId": 1, "channelId": 1, "_id": 0, "activeJobs": 1},

        )



        if not task:

            logger.warning(f"Task {task_id} not found")

            return



        # Check if job_id exists in activeJobs

        active_jobs = task.get("activeJobs", [])

        job_exists = any(job.get("job_id") == job_id for job in active_jobs)

        if not job_exists:

            logger.warning(f"Job {job_id} not found in activeJobs for task {task_id}")

            return



        logger.info(f"Task {task_id} found. Extracting server and channel IDs.")

        server_id = (

            int(task["serverId"])

            if isinstance(task["serverId"], str) and task["serverId"].isdigit()

            else task["serverId"]

        )



        channel_id = (

            int(task["channelId"])

            if isinstance(task["channelId"], str) and task["channelId"].isdigit()

            else task["channelId"]

        )



        # Fetch device names asynchronously

        device_name_map = {}

        if device_ids:

            logger.info(f"Fetching device names for device IDs: {device_ids}")

            device_docs = await asyncio.to_thread(

                list,

                device_collection.find(

                    {"deviceId": {"$in": device_ids}},

                    {"deviceId": 1, "deviceName": 1, "_id": 0},

                ),

            )



            device_name_map = {

                doc.get("deviceId"): doc.get("deviceName", "Unknown Device")

                for doc in device_docs

            }



        # Send commands

        logger.info(f"Sending commands to devices: {device_ids}")
        logger.info(f"[WEEKLY-DEBUG] Command about to send - method: {command.get('method')}, dailyTarget: {command.get('dailyTarget')}, dayIndex: {command.get('dayIndex')}")

        results = await send_commands_to_devices(device_ids, command)

        # Update delivery status based on results
        job_id = command.get("job_id")
        task_id = command.get("task_id")
        
        if job_id and task_id:
            # Update successful deliveries
            if results["success"]:
                await asyncio.to_thread(
                    tasks_collection.update_one,
                    {"id": task_id, "activeJobs.job_id": job_id},
                    {
                        "$set": {
                            "activeJobs.$.deliveryStatus": "delivered",
                            "activeJobs.$.deliveredAt": datetime.now(pytz.UTC)
                        },
                        "$inc": {"activeJobs.$.deliveryAttempts": 1}
                    }
                )
                logger.info(f"[DELIVERY] Job {job_id} delivered to {len(results['success'])} devices")
                
                # Check if this is a weekly plan that needs auto-renewal
                # Don't remove jobs immediately for weekly plans to allow renewal logic to work
                duration_type = command.get("durationType", "")
                is_weekly_plan = duration_type == "WeeklyRandomizedPlan"
                
                if not is_weekly_plan:
                    # Remove successful job from activeJobs after a short delay for non-weekly plans
                    # This prevents immediate removal while still cleaning up completed jobs
                    await asyncio.sleep(5)  # 5 second delay
                    await asyncio.to_thread(
                        tasks_collection.update_one,
                        {"id": task_id},
                        {"$pull": {"activeJobs": {"job_id": job_id}}}
                    )
                    logger.info(f"[CLEANUP] Removed completed job {job_id} from activeJobs (non-weekly plan)")
                else:
                    logger.info(f"[CLEANUP] Preserving job {job_id} in activeJobs for weekly auto-renewal logic")
            
            # Update failed deliveries
            if results["failed"]:
                await asyncio.to_thread(
                    tasks_collection.update_one,
                    {"id": task_id, "activeJobs.job_id": job_id},
                    {
                        "$set": {
                            "activeJobs.$.deliveryStatus": "failed",
                            "activeJobs.$.failedAt": datetime.now(pytz.UTC),
                            "activeJobs.$.failureReason": f"Devices not responding: {results['failed']}"
                        },
                        "$inc": {"activeJobs.$.deliveryAttempts": 1}
                    }
                )
                logger.warning(f"[DELIVERY] Job {job_id} failed for {len(results['failed'])} devices")
            
            # Handle mixed results (some success, some failure)
            if results["success"] and results["failed"]:
                logger.warning(f"[DELIVERY] Job {job_id} partial delivery: {len(results['success'])} success, {len(results['failed'])} failed")



        if results["success"]:

            logger.info("Command successfully executed. Updating task status.")
            
            # ========== WEEKLY AUTO-RENEWAL LOGIC ==========
            # Check if this is the last scheduled day of a weekly plan and auto-schedule next week
            day_index_raw = command.get("dayIndex")
            try:
                day_index_int = int(day_index_raw) if day_index_raw is not None else None
            except (TypeError, ValueError):
                day_index_int = None

            renewal_trigger = bool(command.get("weeklyRenewalTrigger"))
            last_day_raw = command.get("weeklyLastDayIndex")
            try:
                last_day_int = int(last_day_raw) if last_day_raw is not None else None
            except (TypeError, ValueError):
                last_day_int = None

            if not renewal_trigger and day_index_int is not None and last_day_int is not None:
                renewal_trigger = day_index_int == last_day_int

            # Backward compatibility: fallback to Sunday check if metadata missing
            if not renewal_trigger and day_index_int == 6:
                renewal_trigger = True

            if renewal_trigger:
                # Fetch full task to check if it's a weekly plan
                weekly_check_task = await asyncio.to_thread(
                    tasks_collection.find_one,
                    {"id": task_id},
                    {"durationType": 1, "schedules": 1, "inputs": 1, "timeZone": 1, "email": 1, "deviceIds": 1}
                )
                
                if weekly_check_task and weekly_check_task.get("durationType") == "WeeklyRandomizedPlan":
                    logger.info(
                        f"[WEEKLY-RENEWAL] Last scheduled day (index {day_index_int}) completed for task {task_id}. Auto-scheduling next week..."
                    )
                    
                    try:
                        # Extract weekly parameters from the task's schedules
                        schedules = weekly_check_task.get("schedules", {})
                        if isinstance(schedules, dict) and "inputs" in schedules:
                            schedules_inputs = schedules.get("inputs", [])
                            if len(schedules_inputs) >= 3 and isinstance(schedules_inputs[2], dict):
                                weekly_entry = schedules_inputs[2]
                                weekly_data = weekly_entry.get("weeklyData", {})
                                
                                if weekly_data:
                                    # Calculate next Monday (7 days from the original week_start)
                                    current_week_start_str = weekly_data.get("week_start")
                                    time_zone = weekly_check_task.get("timeZone", "UTC")
                                    user_tz = pytz.timezone(time_zone)
                                    
                                    if current_week_start_str:
                                        current_week_start = datetime.fromisoformat(current_week_start_str.replace("Z", "+00:00"))
                                        if current_week_start.tzinfo is None:
                                            current_week_start = user_tz.localize(current_week_start)
                                        else:
                                            current_week_start = current_week_start.astimezone(user_tz)
                                        
                                        # Next week is 7 days later
                                        next_week_start = current_week_start + timedelta(days=7)
                                        
                                        # Generate new weekly schedule
                                        follow_weekly_range = tuple(weekly_data.get("followWeeklyRange", [50, 100]))
                                        rest_days_range = tuple(weekly_data.get("restDaysRange", [1, 2]))
                                        no_two_high_rule = tuple(weekly_data.get("noTwoHighRule", [27, 23]))
                                        off_days_range_meta = tuple(weekly_data.get("offDaysRange", [1, 1]))
                                        if len(off_days_range_meta) != 2:
                                            off_days_range_meta = (1, 1)
                                        method = int(weekly_data.get("method", 1))
                                        
                                        # Use same default ranges for auto-renewal
                                        rest_day_likes_range = (4, 10)  # 4-10 likes per rest day
                                        rest_day_comments_range = (1, 3)  # 1-3 comments per rest day
                                        rest_day_duration_range = (30, 120)  # 30-120 minutes per rest day
                                        
                                        previous_active_days = None
                                        prev_generated_schedule = weekly_entry.get("generatedSchedule") or []
                                        if isinstance(prev_generated_schedule, list):
                                            prev_active_set = {
                                                int(day.get("dayIndex"))
                                                for day in prev_generated_schedule
                                                if isinstance(day, dict)
                                                and not day.get("isRest")
                                                and not day.get("isOff")
                                                and day.get("dayIndex") is not None
                                            }
                                            if prev_active_set:
                                                previous_active_days = prev_active_set

                                        generated = generate_weekly_targets(
                                            next_week_start,
                                            (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                                            (int(rest_days_range[0]), int(rest_days_range[1])),
                                            (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                                            method,
                                            rest_day_likes_range,
                                            rest_day_comments_range,
                                            rest_day_duration_range,
                                            (int(off_days_range_meta[0]), int(off_days_range_meta[1])),
                                            previous_active_days=previous_active_days,
                                        )
                                        validate_weekly_plan(
                                            generated,
                                            (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                                            (int(rest_days_range[0]), int(rest_days_range[1])),
                                            (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                                            (int(off_days_range_meta[0]), int(off_days_range_meta[1])),
                                        )
                                        
                                        logger.info(f"[WEEKLY-RENEWAL] Generated next week starting {next_week_start.isoformat()}")
                                        
                                        # Update DB with new schedule
                                        new_weekly_data = {
                                            "week_start": next_week_start.replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),
                                            "followWeeklyRange": [int(follow_weekly_range[0]), int(follow_weekly_range[1])],
                                            "restDaysRange": [int(rest_days_range[0]), int(rest_days_range[1])],
                                            "noTwoHighRule": [int(no_two_high_rule[0]), int(no_two_high_rule[1])],
                                            "offDaysRange": [int(off_days_range_meta[0]), int(off_days_range_meta[1])],
                                            "method": method,
                                        }
                                        new_weekly_entry = {
                                            "type": "WeeklyRandomizedPlan",
                                            "weeklyData": new_weekly_data,
                                            "generatedSchedule": generated,
                                        }
                                        schedules_inputs[2] = new_weekly_entry
                                        schedules["inputs"] = schedules_inputs
                                        
                                        await asyncio.to_thread(
                                            tasks_collection.update_one,
                                            {"id": task_id},
                                            {"$set": {"schedules": schedules}}
                                        )
                                        
                                        # Clear old jobs and schedule new week
                                        old_task = await asyncio.to_thread(
                                            tasks_collection.find_one,
                                            {"id": task_id},
                                            {"activeJobs": 1}
                                        )
                                        if old_task and old_task.get("activeJobs"):
                                            for old_job in old_task["activeJobs"]:
                                                old_job_id = old_job.get("job_id")
                                                if old_job_id:
                                                    try:
                                                        scheduler.remove_job(old_job_id)
                                                    except Exception:
                                                        pass
                                        
                                        await asyncio.to_thread(
                                            tasks_collection.update_one,
                                            {"id": task_id},
                                            {"$set": {"activeJobs": []}}
                                        )
                                        
                                        # Schedule new 7-day jobs
                                        device_ids_for_renewal = weekly_check_task.get("deviceIds", [])
                                        inputs_for_renewal = weekly_check_task.get("inputs")
                                        renewal_accounts: List[str] = []
                                        if isinstance(inputs_for_renewal, dict):
                                            for wrap in (inputs_for_renewal.get("inputs") or []):
                                                for node in (wrap.get("inputs") or []):
                                                    if isinstance(node, dict) and node.get("type") == "instagrmFollowerBotAcountWise":
                                                        for acc in (node.get("Accounts") or []):
                                                            uname = acc.get("username")
                                                            if isinstance(uname, str):
                                                                renewal_accounts.append(uname)
                                        per_account_plans_renewal: Dict[str, List[int]] = {}
                                        if renewal_accounts:
                                            try:
                                                from utils.weekly_scheduler import generate_per_account_plans
                                                per_account_plans_renewal = generate_per_account_plans(
                                                    generated,
                                                    renewal_accounts,
                                                    (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                                                    (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                                                )
                                            except Exception as renewal_plan_err:
                                                logger.warning(f"[WEEKLY-RENEWAL] Failed to compute per-account plans: {renewal_plan_err}")
                                        
                                        scheduled_indices_new = sorted(
                                            int(day.get("dayIndex", 0))
                                            for day in generated
                                            if not day.get("isOff", False)
                                        )
                                        last_index_new = scheduled_indices_new[-1] if scheduled_indices_new else None
                                        first_index_new = scheduled_indices_new[0] if scheduled_indices_new else None

                                        # Track start times for renewal summary (only used for warmup-only rest day messaging)
                                        renewal_start_times = {}
                                        first_renewal_job_scheduled = False
                                        for day in generated:
                                            day_index_new = int(day.get("dayIndex", 0))
                                            target_count = int(day.get("target", 0))
                                            is_rest = bool(day.get("isRest", False))
                                            is_off = bool(day.get("isOff", False))
                                            day_method = int(day.get("method", 0 if is_off else (9 if is_rest else 1)))
                                            
                                            # Skip off days - no tasks scheduled
                                            if is_off or day_method == 0:
                                                continue
                                            
                                            day_local = next_week_start + timedelta(days=day_index_new)
                                            start_window_start = day_local.replace(hour=11, minute=0, second=0, microsecond=0)
                                            start_window_end = day_local.replace(hour=22, minute=0, second=0, microsecond=0)
                                            window_minutes = int((start_window_end - start_window_start).total_seconds() // 60)
                                            
                                            if window_minutes > 0:
                                                random_offset = random.randint(0, window_minutes - 1)
                                                start_time_local = start_window_start + timedelta(minutes=random_offset)
                                                end_time_local = start_window_end
                                                
                                                start_time_utc = start_time_local.astimezone(pytz.UTC)
                                                end_time_utc = end_time_local.astimezone(pytz.UTC)
                                                
                                                job_id_new = f"cmd_{uuid.uuid4()}"
                                                job_command_new = {
                                                    "task_id": task_id,
                                                    "job_id": job_id_new,
                                                    "dayIndex": day_index_new,
                                                    "weeklyLastDayIndex": last_index_new,
                                                    "weeklyRenewalTrigger": bool(
                                                        last_index_new is not None and day_index_new == last_index_new
                                                    ),
                                                    # Device should rely on per-account caps only
                                                    "dailyTarget": 0,
                                                    "method": day_method,
                                                    "inputs": inputs_for_renewal,
                                                    "newInputs": inputs_for_renewal,
                                                    "timeZone": time_zone,
                                                    "appName": "Instagram Followers Bot",
                                                }
                                                # If warmup day, use the new daily warmup scheduler command type
                                                if day_method == 9 and is_rest:
                                                    job_command_new["type"] = "WARMUPS_DAILY_START"
                                                
                                                warmup_summary_text = "Warmup"
                                                if day_method == 9 and is_rest:
                                                    max_likes = day.get("maxLikes", 10)
                                                    max_comments = day.get("maxComments", 5)
                                                    warmup_duration = day.get("warmupDuration", 60)
                                                    job_command_new.update({
                                                        "maxLikes": max_likes,
                                                        "maxComments": max_comments,
                                                        "warmupDuration": warmup_duration
                                                    })
                                                    warmup_summary_text = f"Warmup - {max_likes} likes, {max_comments} comments, {warmup_duration}min"

                                                account_method_map_renewal: Dict[str, int] = {}
                                                if renewal_accounts:
                                                    for uname in renewal_accounts:
                                                        plan = per_account_plans_renewal.get(uname) or [0] * 7
                                                        day_target_val = plan[day_index_new] if 0 <= day_index_new < len(plan) else 0
                                                        if day_method == 9:
                                                            account_method_map_renewal[uname] = 9
                                                        elif day_target_val > 0:
                                                            account_method_map_renewal[uname] = day_method

                                                schedule_lines_renewal = format_daily_schedule_lines(
                                                    day_index_new,
                                                    renewal_accounts,
                                                    account_method_map_renewal,
                                                    per_account_plans_renewal,
                                                    warmup_summary_text,
                                                )
                                                
                                                # Use helper function to schedule or queue
                                                schedule_or_queue_job(
                                                    job_id=job_id_new,
                                                    trigger_time_utc=start_time_utc,
                                                    device_ids=device_ids_for_renewal,
                                                    command=job_command_new,
                                                    job_name=f"Weekly day {day_index_new} for devices {device_ids_for_renewal}",
                                                    job_type="command"
                                                )
                                                
                                                job_instance = {
                                                    "job_id": job_id_new,
                                                    "startTime": start_time_utc,
                                                    "endTime": end_time_utc,
                                                    "device_ids": device_ids_for_renewal,
                                                    "deliveryStatus": "pending",
                                                    "deliveryAttempts": 0,
                                                    "method": day_method,
                                                    "dayIndex": day_index_new,
                                                    "durationType": "WeeklyRandomizedPlan",
                                                }
                                                await asyncio.to_thread(
                                                    tasks_collection.update_one,
                                                    {"id": task_id},
                                                    {"$push": {"activeJobs": job_instance}}
                                                )
                                                
                                                # Record start time for summary use
                                                try:
                                                    renewal_start_times[day_index_new] = start_time_local
                                                except Exception:
                                                    pass

                                                # Log with actual calendar date
                                                day_names_log = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                                                actual_day_log = day_names_log[start_time_local.weekday()]
                                                logger.info(f"[WEEKLY-RENEWAL] Scheduled Day {day_index_new} ({actual_day_log} {start_time_local.strftime('%b %d')}): {start_time_local.strftime('%H:%M')} (method {day_method})")
                                                
                                                # Schedule reminder for renewed week
                                                reminder_job_id_renewal = f"reminder_{job_id_new}"
                                                schedule_lines_payload_renewal = schedule_lines_renewal or [
                                                    f"Device target: {target_count} {get_method_unit(day_method)}"
                                                ]
                                                
                                                # Send first day reminder immediately when renewed week is created (async to avoid blocking)
                                                if day_index_new == first_index_new and not first_renewal_job_scheduled:
                                                    try:
                                                        # Run reminder in background thread to avoid blocking the event loop
                                                        # Call async version directly since we're in an async context
                                                        asyncio.create_task(
                                                            _send_weekly_reminder_async(
                                                                task_id,
                                                                actual_day_log,
                                                                start_time_local.strftime('%Y-%m-%d %H:%M'),
                                                                schedule_lines_payload_renewal,
                                                                time_zone,
                                                                False,
                                                                "Immediate (First Day Schedule)",
                                                            )
                                                        )
                                                        logger.info(f"[WEEKLY-RENEWAL] ⏰ Queued immediate daily schedule reminder for first day (Day {day_index_new})")
                                                        first_renewal_job_scheduled = True
                                                    except Exception as renewal_reminder_err:
                                                        logger.warning(f"[WEEKLY-RENEWAL] Could not queue immediate first day reminder: {renewal_reminder_err}")
                                                else:
                                                    # For other days, send reminder 5 hours before start, or immediately if less than 5 hours away
                                                    reminder_time_utc_renewal = start_time_utc - timedelta(hours=5)
                                                    now_utc_renewal = datetime.now(pytz.UTC)
                                                    
                                                    # If 5 hours before is in the past (less than 5 hours away), send immediately (async to avoid blocking)
                                                    if reminder_time_utc_renewal <= now_utc_renewal:
                                                        try:
                                                            # Run reminder in background thread to avoid blocking the event loop
                                                            # Call async version directly since we're in an async context
                                                            asyncio.create_task(
                                                                _send_weekly_reminder_async(
                                                                    task_id,
                                                                    actual_day_log,
                                                                    start_time_local.strftime('%Y-%m-%d %H:%M'),
                                                                    schedule_lines_payload_renewal,
                                                                    time_zone,
                                                                    False,
                                                                    "Immediate (Less than 5 hours away)",
                                                                )
                                                            )
                                                            logger.info(f"[WEEKLY-RENEWAL] ⏰ Queued immediate daily schedule reminder for Day {day_index_new} (less than 5 hours away)")
                                                        except Exception as renewal_reminder_err:
                                                            logger.warning(f"[WEEKLY-RENEWAL] Could not queue immediate reminder: {renewal_reminder_err}")
                                                    else:
                                                        # Schedule for 5 hours before start
                                                        try:
                                                            # Use helper function to schedule or queue reminder
                                                            reminder_command = {
                                                                "reminder_type": "weekly_schedule",
                                                                "task_id": task_id,
                                                                "day_name": actual_day_log,
                                                                "start_time": start_time_local.strftime('%Y-%m-%d %H:%M'),
                                                                "schedule_lines": schedule_lines_payload_renewal,
                                                                "time_zone": time_zone,
                                                            }
                                                            schedule_or_queue_job(
                                                                job_id=reminder_job_id_renewal,
                                                                trigger_time_utc=reminder_time_utc_renewal,
                                                                device_ids=[],
                                                                command=reminder_command,
                                                                job_name=f"Reminder for renewed day {day_index_new} of task {task_id}",
                                                                job_type="weekly_reminder"
                                                            )
                                                            logger.info(f"[WEEKLY-RENEWAL] ⏰ Scheduled reminder for renewed Day {day_index_new}: 5 hours before start")
                                                        except Exception as renewal_reminder_err:
                                                            logger.warning(f"[WEEKLY-RENEWAL] Could not schedule reminder: {renewal_reminder_err}")
                                        
                                        logger.info(f"[WEEKLY-RENEWAL] ✅ Next week auto-scheduled for task {task_id}")
                                        
                                        # Send Discord notification
                                        try:
                                            task_meta = await asyncio.to_thread(
                                                tasks_collection.find_one,
                                                {"id": task_id},
                                                {"taskName": 1, "serverId": 1, "channelId": 1}
                                            )
                                            if task_meta:
                                                server_id_notify = task_meta.get("serverId")
                                                channel_id_notify = task_meta.get("channelId")
                                                
                                                if server_id_notify and channel_id_notify:
                                                    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                                                    method_names = {0: "Off Day", 1: "Follow Suggestions", 4: "Unfollow Non-Followers", 9: "Warmup"}
                                                    lines = []
                                                    # Determine if this is a warmup-only plan (weekly target range 0–0)
                                                    try:
                                                        is_warmup_only_plan = int(follow_weekly_range[0]) == 0 and int(follow_weekly_range[1]) == 0
                                                    except Exception:
                                                        is_warmup_only_plan = False
                                                    for d in generated:
                                                        idx = int(d.get("dayIndex", 0))
                                                        is_rest_d = bool(d.get("isRest", False))
                                                        is_off_d = bool(d.get("isOff", False))
                                                        target = int(d.get("target", 0))
                                                        day_method_d = int(d.get("method", 0 if is_off_d else (9 if is_rest_d else 1)))
                                                        method_label = method_names.get(day_method_d, f"method {day_method_d}")
                                                        
                                                        # Calculate actual calendar date for this day
                                                        actual_date = next_week_start + timedelta(days=idx)
                                                        actual_day_name = day_names[actual_date.weekday()]
                                                        date_str = actual_date.strftime("%b %d")  # e.g., "Oct 14"
                                                        
                                                        if is_off_d:
                                                            # For warmup-only plans, treat off-days as warmup-only in the summary
                                                            if is_warmup_only_plan:
                                                                try:
                                                                    # Use a consistent display time window start (11:00)
                                                                    display_time = (next_week_start + timedelta(days=idx)).replace(hour=11, minute=0, second=0, microsecond=0)
                                                                    time_str = display_time.strftime("%H:%M")
                                                                except Exception:
                                                                    time_str = ""
                                                                # Low randomized ranges for display
                                                                try:
                                                                    rand_likes = random.randint(0, 5)
                                                                    rand_comments = random.randint(0, 2)
                                                                    rand_duration = random.randint(5, 10)
                                                                except Exception:
                                                                    rand_likes, rand_comments, rand_duration = 0, 0, 5
                                                                if time_str:
                                                                    label = f"{actual_day_name} {date_str} {time_str}: Off day Warmup - {rand_likes} likes, {rand_comments} comments, {rand_duration}min"
                                                                else:
                                                                    label = f"{actual_day_name} {date_str}: Off day Warmup - {rand_likes} likes, {rand_comments} comments, {rand_duration}min"
                                                            else:
                                                                label = f"{actual_day_name} {date_str}: Off day - No tasks scheduled"
                                                        elif is_rest_d:
                                                            # Keep rest day behavior unchanged for renewal summary
                                                            label = f"{actual_day_name} {date_str}: {method_label}"
                                                        else:
                                                            # Per-account breakdown for renewal summary
                                                            per_acc_parts = []
                                                            try:
                                                                # Build accounts list similar to above (inputs_for_renewal)
                                                                accounts_notify = []
                                                                new_inputs_notify = inputs_for_renewal
                                                                if isinstance(new_inputs_notify, dict):
                                                                    wrap_notify = new_inputs_notify.get("inputs", [])
                                                                    for wrap_entry in (wrap_notify or []):
                                                                        inner_notify = wrap_entry.get("inputs", [])
                                                                        for node_n in (inner_notify or []):
                                                                            if isinstance(node_n, dict) and node_n.get("type") == "instagrmFollowerBotAcountWise":
                                                                                for acc_n in (node_n.get("Accounts") or []):
                                                                                    uname_n = acc_n.get("username")
                                                                                    # Include all accounts - enabled flag is for per-day control
                                                                                    if isinstance(uname_n, str):
                                                                                        accounts_notify.append(uname_n)
                                                                if accounts_notify:
                                                                    from utils.weekly_scheduler import generate_per_account_plans
                                                                    per_acc_notify = generate_per_account_plans(
                                                                        generated,
                                                                        accounts_notify,
                                                                        (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                                                                        (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                                                                    )
                                                                    for uname in accounts_notify:
                                                                        v = (per_acc_notify.get(uname) or [0]*7)[idx]
                                                                        per_acc_parts.append(f"{uname}: {v}")
                                                            except Exception:
                                                                pass
                                                            label = f"{actual_day_name} {date_str}: {method_label}"
                                                            if per_acc_parts:
                                                                label += f" | Accounts: {', '.join(per_acc_parts)}"
                                                        lines.append(label)
                                                    
                                                    # Build per-account breakdown for renewal (reuse same computed plan)
                                                    per_account_lines = []
                                                    try:
                                                        # Use the same account list and per-account plan if available
                                                        accounts_notify = []
                                                        per_acc_notify = {}
                                                        # account_usernames / per_account_plans may not be in scope; try to extract from inputs_for_renewal
                                                        if not accounts_notify:
                                                            try:
                                                                tmp_accounts = []
                                                                new_inputs_notify2 = inputs_for_renewal
                                                                if isinstance(new_inputs_notify2, dict):
                                                                    wrap2 = new_inputs_notify2.get("inputs", [])
                                                                    for wrap_entry2 in (wrap2 or []):
                                                                        inner2 = wrap_entry2.get("inputs", [])
                                                                        for node2 in (inner2 or []):
                                                                            if isinstance(node2, dict) and node2.get("type") == "instagrmFollowerBotAcountWise":
                                                                                for acc2 in (node2.get("Accounts") or []):
                                                                                    uname2 = acc2.get("username")
                                                                                    # Include all accounts - enabled flag is for per-day control
                                                                                    if isinstance(uname2, str):
                                                                                        tmp_accounts.append(uname2)
                                                                accounts_notify = tmp_accounts
                                                            except Exception:
                                                                accounts_notify = []
                                                        if not per_acc_notify and accounts_notify:
                                                            try:
                                                                from utils.weekly_scheduler import generate_per_account_plans
                                                                per_acc_notify = generate_per_account_plans(
                                                                    generated,
                                                                    accounts_notify,
                                                                    (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                                                                    (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                                                                )
                                                            except Exception:
                                                                per_acc_notify = {}
                                                        if accounts_notify and per_acc_notify:
                                                            for uname in accounts_notify:
                                                                plan = per_acc_notify.get(uname) or [0]*7
                                                                per_account_lines.append(f"{uname}: {sum(plan)} follows/week")
                                                    except Exception:
                                                        per_account_lines = []

                                                    summary = (
                                                        f"🔄 Weekly plan AUTO-RENEWED: {task_id}\n"
                                                        f"🧩 Task: {task_meta.get('taskName', 'Unknown Task')}\n"
                                                        f"🕰️ Next week start: {next_week_start.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()}\n\n"
                                                        + "\n".join(lines)
                                                        + ("\n\nAccounts:\n" + "\n".join(per_account_lines) if per_account_lines else "")
                                                    )
                                                    
                                                    renewal_chunks = chunk_text_for_discord(summary)
                                                    for idx, chunk in enumerate(renewal_chunks, start=1):
                                                        prefix = "" if idx == 1 else f"(Part {idx})\n"
                                                        await bot_instance.send_message({
                                                            "message": f"{prefix}{chunk}",
                                                            "task_id": task_id,
                                                            "job_id": f"weekly_renewal_{task_id}_part{idx}",
                                                            "server_id": int(server_id_notify) if isinstance(server_id_notify, str) and server_id_notify.isdigit() else server_id_notify,
                                                            "channel_id": int(channel_id_notify) if isinstance(channel_id_notify, str) and channel_id_notify.isdigit() else channel_id_notify,
                                                            "type": "info",
                                                        })
                                        except Exception as notify_err:
                                            logger.warning(f"[WEEKLY-RENEWAL] Failed to send Discord notification: {notify_err}")
                                        
                    except Exception as renewal_err:
                        logger.error(f"[WEEKLY-RENEWAL] Failed to auto-schedule next week: {renewal_err}", exc_info=True)
            # ========== END WEEKLY AUTO-RENEWAL ==========

            # Check if this is a recurring task (daily or weekly) before clearing scheduledTime
            task_info = await asyncio.to_thread(

                tasks_collection.find_one,

                {"id": task_id},

                {"durationType": 1, "scheduledTime": 1}

            )

            

            if task_info and task_info.get("durationType") in ["EveryDayAutomaticRun", "WeeklyRandomizedPlan"]:
                # For recurring tasks (daily/weekly), DON'T clear scheduledTime
                result = await asyncio.to_thread(

                    tasks_collection.update_one,

                    {"id": task_id},

                    {"$set": {"status": "running"}}

                )

            else:

                # For one-time tasks (fixed-time), clear scheduledTime
                result = await asyncio.to_thread(

                    tasks_collection.update_one,

                    {"id": task_id},

                    {

                        "$set": {"status": "running"},

                        "$unset": {"scheduledTime": ""}

                    }

                )
            
            

            if result.modified_count > 0:

                logger.info("Task status updated successfully.")

                # Force immediate cache refresh for all related endpoints

                try:

                    from routes.tasks import invalidate_user_tasks_cache

                    # Get user email from the task

                    task_info = await asyncio.to_thread(

                        tasks_collection.find_one,

                        {"id": task_id},

                        {"email": 1}

                    )

                    if task_info and task_info.get("email"):

                        user_email = task_info["email"]

                        # Clear all related caches immediately

                        invalidate_user_tasks_cache(user_email)

                        
                        
                        # Also clear specific endpoint caches for immediate refresh

                        redis_client.delete(f"tasks:list:{user_email}:*")

                        redis_client.delete(f"tasks:scheduled:{user_email}")

                        redis_client.delete(f"tasks:running:{user_email}")

                        
                        
                        logger.info(f"All caches cleared for user {user_email}")

                except Exception as cache_error:

                    logger.warning(f"Cache clearing failed: {cache_error}")

            else:

                logger.warning("Task status update failed.")



        # Handle failed devices

        if results["failed"]:

            failed_names = [

                device_name_map.get(d_id, "Unknown Device")

                for d_id in results["failed"]

            ]

            error_message = f"Error: The following devices are not connected: {', '.join(failed_names)}"

            logger.warning(error_message)



            try:

                logger.info("Creating a new task to send error message")



                # Use async send_message in async context

                await bot_instance.send_message(

                    {

                        "message": error_message,

                        "task_id": task_id,

                        "job_id": job_id,

                        "server_id": server_id,

                        "channel_id": channel_id,

                        "type": "error",

                    }

                )

                logger.info("Error message queued successfully")



            except Exception as e:

                logger.error(f"Error sending message: {e}")



            # Update database to remove failed devices

            logger.info("Removing failed devices from active jobs.")

            result = await asyncio.to_thread(

                tasks_collection.update_one,

                {"id": task_id, "activeJobs.job_id": job_id},

                {"$pull": {"activeJobs.$.device_ids": {"$in": results["failed"]}}},

            )

            if result.modified_count > 0:

                logger.info("Active jobs updated successfully.")

            else:

                logger.warning("No active jobs were updated.")



        if len(results["failed"]) == len(device_ids):

            logger.info("All devices failed. Removing job from active jobs.")

            # Remove job from active jobs

            result = await asyncio.to_thread(

                tasks_collection.update_one,

                {"id": task_id},

                {"$pull": {"activeJobs": {"job_id": job_id}}},

            )



            if result.modified_count > 0:

                logger.info(f"Job {job_id} successfully removed from active jobs.")



                # Get the updated task to check remaining active jobs

                task_data = await asyncio.to_thread(

                    tasks_collection.find_one, {"id": task_id}, {"activeJobs": 1}

                )



                # Update status based on remaining active jobs

                status = (

                    "awaiting"

                    if not task_data.get("activeJobs")

                    or len(task_data["activeJobs"]) == 0

                    else "scheduled"

                )



                # Update the task status

                await asyncio.to_thread(

                    tasks_collection.update_one,

                    {"id": task_id},

                    {"$set": {"status": status}},

                )



                logger.info(f"Task status updated to: {status}")

            else:

                logger.warning(f"Job {job_id} removal failed.")



            # Send all devices disconnected message

            error_message_all_devices_not_connected = (

                "Task cannot be executed. All target devices are disconnected."

            )



            try:

                # Use async send_message in async context

                await bot_instance.send_message(

                    {

                        "message": error_message_all_devices_not_connected,

                        "task_id": task_id,

                        "job_id": job_id,

                        "server_id": server_id,

                        "channel_id": channel_id,

                        "type": "error",

                    }

                )

                logger.info("All devices disconnected message queued successfully")



            except Exception as e:

                logger.error(f"Error sending all devices disconnected message: {e}")



            logger.info(

                f"Job {job_id} is no longer active as no devices are connected."

            )



        # Handle recurring job if applicable

        if is_recurring:

            logger.info(f"Scheduling recurring job for task {task_id}.")

            # Use executor to run blocking code

            loop = asyncio.get_event_loop()

            await loop.run_in_executor(

                None, schedule_recurring_job, command, device_ids

            )



        return results



    except Exception as e:

        logger.error(f"Error in send_command_to_devices: {e}")

        import traceback



        traceback.print_exc()

        return None

















def schedule_split_jobs(

    start_times: List[datetime],

    random_durations: List[int],

    device_ids: List[str],

    command: dict,

    task_id: str,

) -> None:

    """Schedule multiple jobs based on random start times and durations."""

    job_instances = []

    scheduled_jobs = []



    for i, (start_time, duration) in enumerate(zip(start_times, random_durations)):

        job_id = f"cmd_{uuid.uuid4()}"

        end_time = start_time + timedelta(minutes=duration)

        start_time_utc = start_time.astimezone(pytz.UTC)

        end_time_utc = end_time.astimezone(pytz.UTC)

        jobInstance = {

            "job_id": job_id,

            "startTime": start_time_utc,

            "endTime": end_time_utc,

            "device_ids": device_ids,
            "deliveryStatus": "pending",
            "deliveryAttempts": 0,
            "method": command.get("method", 1),
            "durationType": command.get("durationType", "FixedTime"),

        }

        job_instances.append(jobInstance)



        modified_command = {**command, "duration": duration, "job_id": job_id}

        try:
            # Use helper function to schedule or queue
            schedule_or_queue_job(
                job_id=job_id,
                trigger_time_utc=start_time_utc,
                device_ids=device_ids,
                command=modified_command,
                job_name=f"DurationWithTimeWindow {i+1} for devices {device_ids}",
                job_type="command"
            )

            scheduled_jobs.append(job_id)



        except Exception as e:

            print(f"Failed to schedule split job {i + 1}: {str(e)}")

            # Remove previously scheduled jobs if any fail

            for scheduled_job_id in scheduled_jobs:

                try:

                    scheduler.remove_job(scheduled_job_id)

                except:  # noqa: E722

                    pass

            raise HTTPException(

                status_code=500, detail=f"Failed to schedule job: {str(e)}"

            )



    # Only update database if all jobs were scheduled successfully

    if scheduled_jobs:

        # Get the earliest start time for nextRunTime

        earliest_start_time = min(job_instances, key=lambda x: x["startTime"])["startTime"]

        
        # Update status only if it's not 'running'

        tasks_collection.update_one(

            {"id": task_id, "status": {"$ne": "running"}},

            {

                "$set": {
                    "status": "scheduled",
                    "nextRunTime": earliest_start_time.isoformat()  # Add next run time
                },

                "$unset": {"scheduledTime": ""}  # Clear old scheduledTime

            },

        )

        
        
        # Invalidate cache to ensure consistency

        try:

            from routes.tasks import invalidate_user_tasks_cache

            # Get user email from the task

            task_info = tasks_collection.find_one(

                {"id": task_id},

                {"email": 1}

            )

            if task_info and task_info.get("email"):

                user_email = task_info["email"]

                # Clear all related caches immediately

                invalidate_user_tasks_cache(user_email)

                
                
                # Also clear specific endpoint caches for immediate refresh

                redis_client.delete(f"tasks:list:{user_email}:*")

                redis_client.delete(f"tasks:scheduled:{user_email}")

                redis_client.delete(f"tasks:running:{user_email}")

                
                
                print(f"[CACHE] All caches cleared after split schedule update for task ID {task_id}")

        except Exception as cache_error:

            print(f"[WARNING] Cache invalidation failed: {cache_error}")



        # Always update activeJobs - push ALL job instances, not just one

        tasks_collection.update_one(

            {"id": task_id}, {"$push": {"activeJobs": {"$each": job_instances}}}

        )



        # tasks_collection.update_one(

        #     {"id": task_id},

        #     {"$set": {"status": "scheduled"},

        #      "$push": {"activeJobs": {"$each": job_instances}}}

        # )



        # Get device names for notification

        device_docs = list(

            device_collection.find(

                {"deviceId": {"$in": device_ids}},

                {"deviceId": 1, "deviceName": 1, "_id": 0},

            )

        )

        device_names = [doc.get("deviceName", "Unknown Device") for doc in device_docs]



        # Get task details for notification

        task = tasks_collection.find_one({"id": task_id})

        time_zone = command.get("timeZone", "UTC")



        # Send split schedule notification asynchronously

        asyncio.create_task(

            send_split_schedule_notification(

                task, device_names, start_times, random_durations, time_zone

            )

        )





def schedule_notification(task, device_names, start_time, end_time, time_zone, job_id):

    """Thread-safe function to send a notification about a scheduled task."""

    if not task or not task.get("serverId") or not task.get("channelId"):

        print("Skipping notification. Missing serverId/channelId for task")

        return



    try:

        # Import here to avoid circular imports

        from Bot.discord_bot import get_bot_instance



        user_tz = pytz.timezone(time_zone)

        local_start_time = start_time.astimezone(user_tz)

        # Format times in user's timezone

        formatted_start = local_start_time.strftime("%Y-%m-%d %H:%M")

        task_name = task.get("taskName", "Unknown Task")



        # Create device list string

        device_list = ", ".join(device_names) if device_names else "No devices"



        # Create notification message

        message = (

            f"📅 **Task Scheduled**: {task_name}\n"

            f"⏰ **Start Time**: {formatted_start} ({time_zone})\n"

            f"🔌 **Devices**: {device_list}"

        )



        server_id = (

            int(task["serverId"])

            if isinstance(task["serverId"], str) and task["serverId"].isdigit()

            else task["serverId"]

        )

        channel_id = (

            int(task["channelId"])

            if isinstance(task["channelId"], str) and task["channelId"].isdigit()

            else task["channelId"]

        )



        # Get bot instance and use thread-safe method

        bot = get_bot_instance()

        bot.send_message_sync(

            {

                "message": message,

                "task_id": task.get("id"),

                "job_id": job_id,

                "server_id": server_id,

                "channel_id": channel_id,

                "type": "info",

            }

        )

        print(f"Schedule notification queued for task {task.get('id')}")



    except Exception as e:

        print(f"Error sending schedule notification: {str(e)}")





async def send_schedule_notification(

    task, device_names, start_time, end_time, time_zone, job_id

):

    """Async version that now uses the thread-safe notification sender"""

    schedule_notification(task, device_names, start_time, end_time, time_zone, job_id)





def schedule_recurring_job(

    command: dict, device_ids: List[str], main_loop=None

) -> None:

    """Schedule the next day's task within the specified time window"""

    task_id = command.get("task_id")

    time_zone = command.get("timeZone", "UTC")

    user_tz = pytz.timezone(time_zone)



    # Get tomorrow's date

    now = datetime.now(user_tz)



    # Parse start and end times

    start_time_str = command.get("startInput")

    end_time_str = command.get("endInput")

    start_hour, start_minute = parse_time(start_time_str)

    end_hour, end_minute = parse_time(end_time_str)



    start_time = now.replace(

        hour=start_hour, minute=start_minute, second=0, microsecond=0

    )

    end_time = now.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)



    if start_time < now:

        start_time += timedelta(days=1)



    if end_time < start_time:

        end_time += timedelta(days=1)



    time_window_minutes = int((end_time - start_time).total_seconds() / 60)

    duration = int(command.get("duration", 0))



    # Ensure we don't schedule if the duration exceeds available time

    if duration > time_window_minutes:

        print(

            f"Duration ({duration}) exceeds available time window ({time_window_minutes})"

        )

        return



    random_minutes = random.randint(0, time_window_minutes - duration)

    random_start_time = start_time + timedelta(minutes=random_minutes)

    random_end_time = random_start_time + timedelta(minutes=duration)



    start_time_utc = random_start_time.astimezone(pytz.UTC)

    end_time_utc = random_end_time.astimezone(pytz.UTC)



    new_job_id = f"cmd_{uuid.uuid4()}"

    modified_command = {**command, "job_id": new_job_id, "isRecurring": True}



    jobInstance = {

        "job_id": new_job_id,

        "startTime": start_time_utc,

        "endTime": end_time_utc,

        "device_ids": device_ids,
        "deliveryStatus": "pending",
        "deliveryAttempts": 0,
        "method": command.get("method", 1),
        "durationType": command.get("durationType", "EveryDayAutomaticRun"),

    }



    try:
        # Use helper function to schedule or queue
        schedule_or_queue_job(
            job_id=new_job_id,
            trigger_time_utc=start_time_utc,
            device_ids=device_ids,
            command=modified_command,
            job_name=f"Recurring random-time command for devices {device_ids}",
            job_type="command"
        )

        # First, get current task status

        task = tasks_collection.find_one({"id": task_id}, {"status": 1})



        if task and task.get("status") == "awaiting":

            # Update both status and activeJobs if status is "awaiting"

            update_operation = {

                "$set": {
                    "status": "scheduled",
                    "nextRunTime": start_time_utc.isoformat()  # Add next run time
                },

                "$push": {"activeJobs": jobInstance}

                # Remove the "$unset": {"scheduledTime": ""} line for daily tasks

            }

        else:

            # Only update activeJobs if status is not "awaiting"

            update_operation = {

                "$push": {"activeJobs": jobInstance},
                "$set": {"nextRunTime": start_time_utc.isoformat()}  # Add next run time even if not awaiting

                # Remove the "$unset": {"scheduledTime": ""} line for daily tasks

            }



        # Update the task in the database

        tasks_collection.update_one({"id": task_id}, update_operation)

        
        
        # Invalidate cache to ensure consistency

        try:

            from routes.tasks import invalidate_user_tasks_cache

            # Get user email from the task

            task_info = tasks_collection.find_one(

                {"id": task_id},

                {"email": 1}

            )

            if task_info and task_info.get("email"):

                user_email = task_info["email"]

                # Clear all related caches immediately

                invalidate_user_tasks_cache(user_email)

                
                
                # Also clear specific endpoint caches for immediate refresh

                redis_client.delete(f"tasks:list:{user_email}:*")

                redis_client.delete(f"tasks:scheduled:{user_email}")

                redis_client.delete(f"tasks:running:{user_email}")

                
                
                print(f"[CACHE] All caches cleared after recurring schedule update for task ID {task_id}")

        except Exception as cache_error:

            print(f"[WARNING] Cache invalidation failed: {cache_error}")



        # Get device names for notification

        device_docs = list(

            device_collection.find(

                {"deviceId": {"$in": device_ids}},

                {"deviceId": 1, "deviceName": 1, "_id": 0},

            )

        )

        device_names = [doc.get("deviceName", "Unknown Device") for doc in device_docs]



        # Get updated task details for notification

        task = tasks_collection.find_one({"id": task_id})

        # asyncio.create_task(

        schedule_notification(

            task,

            device_names,

            random_start_time,

            random_end_time,

            time_zone,

            new_job_id,

        )

        # )



        print(f"Scheduled next day's task for {random_start_time} ({time_zone})")



    except Exception as e:

        print(f"Failed to schedule next day's job: {str(e)}")

        raise HTTPException(

            status_code=500, detail=f"Failed to schedule next day's job: {str(e)}"

        )





def schedule_single_job(

    start_time, end_time, device_ids, command, job_id: str, task_id: str

) -> None:

    """Schedule a single job with a defined start and end time."""



    start_time_utc = start_time.astimezone(pytz.UTC)

    end_time_utc = end_time.astimezone(pytz.UTC)



    jobInstance = {

        "job_id": job_id,

        "startTime": start_time_utc,

        "endTime": end_time_utc,

        "device_ids": device_ids,
        "deliveryStatus": "pending",
        "deliveryAttempts": 0,
        "method": command.get("method", 1),
        "durationType": command.get("durationType", "FixedTime"),

    }



    try:
        # Use helper function to schedule or queue
        schedule_or_queue_job(
            job_id=job_id,
            trigger_time_utc=start_time_utc,
            device_ids=device_ids,
            command={**command, "duration": int(command.get("duration", 0))},
            job_name=f"Single session command for devices {device_ids}",
            job_type="command"
        )



        # Update status only if it's not 'running' AND clear scheduledTime

        tasks_collection.update_one(

            {"id": task_id, "status": {"$ne": "running"}},

            {

                "$set": {
                    "status": "scheduled",
                    "nextRunTime": start_time_utc.isoformat()  # Add next run time
                },

                "$unset": {"scheduledTime": ""}  # Clear old scheduledTime

            },

        )

        
        
        # Invalidate cache to ensure consistency

        try:

            from routes.tasks import invalidate_user_tasks_cache

            # Get user email from the task

            task_info = tasks_collection.find_one(

                {"id": task_id},

                {"email": 1}

            )

            if task_info and task_info.get("email"):

                user_email = task_info["email"]

                # Clear all related caches immediately

                invalidate_user_tasks_cache(user_email)

                
                
                # Also clear specific endpoint caches for immediate refresh

                redis_client.delete(f"tasks:list:{user_email}:*")

                redis_client.delete(f"tasks:scheduled:{user_email}")

                redis_client.delete(f"tasks:running:{user_email}")

                
                
                print(f"[CACHE] All caches cleared after schedule update for task ID {task_id}")

        except Exception as cache_error:

            print(f"[WARNING] Cache clearing failed: {cache_error}")



        # Always update activeJobs

        tasks_collection.update_one(

            {"id": task_id}, {"$push": {"activeJobs": jobInstance}}

        )



        # Get device names for notification

        device_docs = list(

            device_collection.find(

                {"deviceId": {"$in": device_ids}},

                {"deviceId": 1, "deviceName": 1, "_id": 0},

            )

        )

        device_names = [doc.get("deviceName", "Unknown Device") for doc in device_docs]



        # Get task details for notification

        task = tasks_collection.find_one({"id": task_id})

        time_zone = command.get("timeZone", "UTC")



        # Send schedule notification asynchronously

        asyncio.create_task(

            send_schedule_notification(

                task, device_names, start_time, end_time, time_zone, job_id

            )

        )



        # schedule_notification(

        #         task, device_names, start_time, end_time, time_zone, job_id

        #     )



    except Exception as e:

        print(f"Failed to schedule single job: {str(e)}")

        raise HTTPException(status_code=500, detail=f"Failed to schedule job: {str(e)}")

