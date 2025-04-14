import asyncio
import time
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Depends
from pymongo import MongoClient, UpdateOne
from pydantic import BaseModel
from typing import List
import random
from datetime import datetime, timedelta
from utils.utils import get_current_user, check_for_Job_clashes, split_message
from models.tasks import tasks_collection
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
# from Bot.discord_bot import get_bot_instance

from redis_client import get_redis_client
from logger import logger


redis_client = get_redis_client()
main_event_loop = asyncio.get_event_loop()

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


@device_router.post("/register_device")
async def register_device_endpoint(device_data: DeviceRegistration):
    return register_device(device_data)


@device_router.get("/device_status/{device_id}")
async def check_device_status(device_id: str):
    device = device_collection.find_one({"deviceId": device_id})
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    return {"device_id": device_id, "status": device["status"]}


@device_router.put("/update_status/{device_id}")
async def update_device_status(device_id: str, status: bool):
    device = device_collection.find_one({"deviceId": device_id})
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    device_collection.update_one({"deviceId": device_id}, {"$set": {"status": status}})
    return {"message": f"Device {device_id} status updated to {status}"}


@device_router.get("/device_registration/{device_id}")
async def check_device_registration(device_id: str):
    device = device_collection.find_one({"deviceId": device_id})
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    if not device["status"]:
        device_collection.update_one(
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
        tasks = list(tasks_collection.find({"id": {"$in": task_ids}}))

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
            # websocket = device_connections.get(device_id)
            check = is_device_connected(device_id)
            device_name = device_names.get(device_id, device_id)

            if check:
                print(f"[LOG] Device {device_id} ({device_name}) is connected.")
                connected_devices.append(device_id)
            else:
                print(f"[LOG] Device {device_id} ({device_name}) is NOT connected.")
                not_connected_devices.add(device_id)

        # If no devices are connected
        if not connected_devices:
            print("[LOG] No connected devices found for the old jobs.")
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
            result = tasks_collection.bulk_write(bulk_operations)
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


@device_router.websocket("/ws/{device_id}")
async def websocket_endpoint(websocket: WebSocket, device_id: str):
    await websocket.accept()
    reconnection_count = track_reconnection(device_id)
    if reconnection_count > 10:
        print(
            f"Warning: Device {device_id} has reconnected {reconnection_count} times in the last hour"
        )

    device_connections[device_id] = websocket
    active_connections.append(websocket)

    # Register in Redis
    register_device_connection(device_id)

    # Update MongoDB status
    device_collection.update_one({"deviceId": device_id}, {"$set": {"status": True}})

    print(f"Device {device_id} connected to worker {WORKER_ID}")
    log_all_connected_devices()

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

                device_info = device_collection.find_one({"deviceId": device_id})
                device_name = (
                    device_info.get("deviceName", device_id)
                    if device_info
                    else device_id
                )
                if message:
                    message = f"Device Name: {device_name}\n\n{message}"

                if message_type == "ping":
                    pong_response = {
                        "type": "pong",
                        "timestamp": payload.get("timestamp", int(time.time() * 1000)),
                    }
                    await websocket.send_text(json.dumps(pong_response))
                    print(f"Sent pong response to ({device_id})")
                    continue

                taskData = tasks_collection.find_one(
                    {"id": task_id}, {"serverId": 1, "channelId": 1, "_id": 0}
                )

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

                    # Check if task is still active and update status (only for non-update messages)
                    task = tasks_collection.find_one({"id": task_id})
                    if task:
                        status = (
                            "awaiting"
                            if len(task.get("activeJobs", [])) == 0
                            else "scheduled"
                        )
                        tasks_collection.update_one(
                            {"id": task_id}, {"$set": {"status": status}}
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
        device_collection.update_one(
            {"deviceId": device_id}, {"$set": {"status": False}}
        )

        # Clean up local connections
        active_connections.remove(websocket)
        device_connections.pop(device_id, None)

        # Unregister from Redis
        unregister_device_connection(device_id)


@device_router.post("/send_command")
async def send_command(
    request: CommandRequest, current_user: dict = Depends(get_current_user)
):
    task_id = request.command.get("task_id")
    command = request.command
    print(command)
    device_ids = request.device_ids
    duration = int(command.get("duration", 0))
    durationType = request.command.get("durationType")
    time_zone = request.command.get("timeZone", "UTC")
    newInputs = request.command.get("newInputs")
    newSchedules = request.command.get("newSchecdules")
    tasks_collection.update_one(
        {"id": task_id},
        {
            "$set": {
                "inputs": newInputs,
                "deviceIds": device_ids,
                "schedules": newSchedules,
            }
        },
    )

    try:
        user_tz = pytz.timezone(time_zone)
        now = datetime.now(user_tz)
        print(f"Current time in {time_zone}: {now}")

        if durationType in ["DurationWithExactStartTime", "ExactStartTime"]:
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

            if check_for_Job_clashes(
                target_time_utc, target_end_time_utc, task_id, device_ids
            ):
                return JSONResponse(
                    content={"message": "Task already Scheduled on this time"},
                    status_code=400,
                )

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

            if check_for_Job_clashes(start_time, end_time, task_id, device_ids):
                return JSONResponse(
                    content={"message": "Task already Scheduled on this time"},
                    status_code=400,
                )

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
            schedule_recurring_job(command, device_ids)

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


def wrapper_for_send_command(device_ids, command):
    """
    Thread-safe wrapper for sending commands to devices
    """
    try:
        # No need to pass bot instance or manage event loops for messaging
        # Check if we're already in an event loop for the command processing
        try:
            current_loop = asyncio.get_event_loop()
            if current_loop.is_running():
                # We're already in a running event loop, create a new one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(
                    send_command_to_devices(device_ids, command)
                )
            else:
                # We have a loop but it's not running, use it
                return current_loop.run_until_complete(
                    send_command_to_devices(device_ids, command)
                )
        except RuntimeError:
            # No event loop exists, create one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(send_command_to_devices(device_ids, command))
    finally:
        # Only close the loop if we created a new one
        if "loop" in locals() and loop is not asyncio.get_event_loop():
            loop.close()


async def send_command_to_devices(device_ids, command):
    """Send command to devices with improved async handling"""
    logger.info(f"Executing command for devices: {device_ids}")
    
    # Import here to avoid circular imports
    from Bot.discord_bot import get_bot_instance
    
    task_id = command.get("task_id")
    job_id = command.get("job_id")
    is_recurring = command.get("isRecurring", False)

    try:
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
        
        activeJobs = task["activeJobs"]
        jobFound = False
        for job in activeJobs:
            if job["job_id"] == job_id:
                jobFound = True
                break
            
        if not jobFound:
            logger.warning(f"Job {job_id} in task {task_id} not found")
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
        results = await send_commands_to_devices(device_ids, command)

        if results["success"]:
            logger.info("Command successfully executed. Updating task status.")
            # Update task status
            result = await asyncio.to_thread(
                tasks_collection.update_one,
                {"id": task_id},
                {"$set": {"status": "running"}},
            )
            if result.modified_count > 0:
                logger.info("Task status updated successfully.")
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
                
                # Get the bot instance and use the thread-safe method
                bot = get_bot_instance()
                bot.send_message_sync({
                    "message": error_message,
                    "task_id": task_id,
                    "job_id": job_id,
                    "server_id": server_id,
                    "channel_id": channel_id,
                    "type": "error",
                })
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
                    tasks_collection.find_one,
                    {"id": task_id},
                    {"activeJobs": 1}
                )
                
                # Update status based on remaining active jobs
                status = "awaiting" if not task_data.get("activeJobs") or len(task_data["activeJobs"]) == 0 else "scheduled"
                
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
                # Get the bot instance and use the thread-safe method
                bot = get_bot_instance()
                bot.send_message_sync({
                    "message": error_message_all_devices_not_connected,
                    "task_id": task_id,
                    "job_id": job_id,
                    "server_id": server_id,
                    "channel_id": channel_id,
                    "type": "error",
                })
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


def parse_time(time_str: str) -> tuple:
    """Parse time string in 'HH:MM' format to a tuple of integers (hour, minute)."""
    hour, minute = map(int, time_str.split(":"))
    return hour, minute


def generate_random_durations_and_start_times(
    duration: int, start_time: datetime, end_time: datetime
) -> tuple:
    """Generate random durations and start times for split jobs."""
    random_durations = generate_random_durations(duration)
    start_times = get_random_start_times(start_time, end_time, random_durations)
    return random_durations, start_times


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
        }
        job_instances.append(jobInstance)

        modified_command = {**command, "duration": duration, "job_id": job_id}
        try:
            scheduler.add_job(
                wrapper_for_send_command,
                trigger=DateTrigger(
                    run_date=start_time.astimezone(pytz.UTC), timezone=pytz.UTC
                ),
                args=[device_ids, modified_command],
                id=job_id,
                name=f"Part {i + 1} of split command for devices {device_ids}",
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
        # Update status only if it's not 'running'
        tasks_collection.update_one(
            {"id": task_id, "status": {"$ne": "running"}},
            {"$set": {"status": "scheduled"}},
        )

        # Always update activeJobs
        tasks_collection.update_one(
            {"id": task_id}, {"$push": {"activeJobs": jobInstance}}
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


async def send_schedule_notification(task, device_names, start_time, end_time, time_zone, job_id):
    """Async version that now uses the thread-safe notification sender"""
    schedule_notification(task, device_names, start_time, end_time, time_zone, job_id)


def schedule_recurring_job(command: dict, device_ids: List[str], main_loop=None) -> None:
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
    }

    try:
        scheduler.add_job(
            wrapper_for_send_command,
            trigger=DateTrigger(run_date=start_time_utc, timezone=pytz.UTC),
            args=[device_ids, modified_command],
            id=new_job_id,
            name=f"Recurring random-time command for devices {device_ids}",
        )

        # First, get current task status
        task = tasks_collection.find_one({"id": task_id}, {"status": 1})

        if task and task.get("status") == "awaiting":
            # Update both status and activeJobs if status is "awaiting"
            update_operation = {
                "$set": {"status": "scheduled"},
                "$push": {"activeJobs": jobInstance},
            }
        else:
            # Only update activeJobs if status is not "awaiting"
            update_operation = {"$push": {"activeJobs": jobInstance}}

        # Update the task in the database
        tasks_collection.update_one({"id": task_id}, update_operation)

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
        schedule_notification(task, device_names, random_start_time, random_end_time, time_zone, new_job_id)
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
    }

    try:
        scheduler.add_job(
            wrapper_for_send_command,
            trigger=DateTrigger(
                run_date=start_time.astimezone(pytz.UTC), timezone=pytz.UTC
            ),
            args=[device_ids, {**command, "duration": int(command.get("duration", 0))}],
            id=job_id,
            name=f"Single session command for devices {device_ids}",
        )
        

        # tasks_collection.update_one(
        #     {"id": task_id},
        #     {"$set": {"status": "scheduled"},
        #      "$push": {"activeJobs": jobInstance}}
        # )

        # Update status only if it's not 'running'
        tasks_collection.update_one(
            {"id": task_id, "status": {"$ne": "running"}},
            {"$set": {"status": "scheduled"}},
        )

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


def generate_random_durations(total_duration: int, min_duration: int = 30) -> List[int]:
    """
    Generate 2 to 4 random durations that sum up to the total duration.
    Each duration will be at least min_duration minutes.
    """
    num_durations = random.randint(2, 4)  # Limit the number of partitions to 2, 3, or 4

    if total_duration <= min_duration:
        return [total_duration]

    durations = []
    remaining = total_duration

    for _ in range(num_durations - 1):
        max_possible = min(
            remaining - min_duration, remaining // (num_durations - len(durations))
        )
        if max_possible <= min_duration:
            break
        duration = random.randint(min_duration, max_possible)
        durations.append(duration)
        remaining -= duration

    durations.append(remaining)  # Add the remaining time to the last partition

    return durations


def get_random_start_times(
    start_time: datetime, end_time: datetime, durations: List[int], min_gap: float = 1.5
) -> List[datetime]:
    """
    Generate random start times for each duration, ensuring minimum gap between sessions.
    If end_time is less than or equal to start_time, end_time is considered the next day.
    Returns list of start times in chronological order.
    """

    # If end_time is less than or equal to start_time, treat it as next day's time
    if end_time <= start_time:
        end_time += timedelta(days=1)

    total_time_needed = sum(durations) + (len(durations) - 1) * min_gap
    available_time = (end_time - start_time).total_seconds() / 60  # Convert to minutes

    # Check if there is enough time in the window
    if total_time_needed > available_time:
        raise ValueError(
            "Not enough time in the window for all sessions with minimum gaps"
        )

    start_times = []
    current_time = start_time

    # Calculate maximum gap possible
    remaining_gaps = len(durations) - 1
    for i, duration in enumerate(durations):
        start_times.append(current_time)

        if remaining_gaps > 0:
            # Calculate the remaining time and the maximum possible gap
            time_left = (end_time - current_time).total_seconds() / 60
            time_needed = sum(durations[i + 1 :]) + remaining_gaps * min_gap
            max_gap = (time_left - time_needed) / remaining_gaps

            # Add a random gap after the session, but ensure it's at least min_gap
            gap = random.uniform(min_gap, max_gap) if max_gap > min_gap else min_gap
            current_time += timedelta(minutes=duration + gap)
            remaining_gaps -= 1
        else:
            # No more gaps to add, just adjust the current time
            current_time += timedelta(minutes=duration)

    return start_times


async def send_split_schedule_notification(
    task, device_names, start_times, durations, time_zone
):
    """Send a notification to Discord about split scheduled tasks."""
    if not task or not task.get("serverId") or not task.get("channelId"):
        print("Skipping notification. Missing serverId/channelId for task")
        return

    try:
        task_name = task.get("taskName", "Unknown Task")
        device_list = ", ".join(device_names) if device_names else "No devices"

        # Create summary of split sessions
        total_duration = sum(durations)
        session_count = len(start_times)
        timespan_start = min(start_times).strftime("%Y-%m-%d %H:%M")
        timespan_end = (max(start_times) + timedelta(minutes=durations[-1])).strftime(
            "%Y-%m-%d %H:%M"
        )

        message = (
            f"📅 **Split Task Scheduled**: {task_name}\n"
            f"⏰ **Timespan**: {timespan_start} to {timespan_end} ({time_zone})\n"
            f"🔢 **Sessions**: {session_count} sessions (total {total_duration} minutes)\n"
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

        # Send message to Discord
        await bot_instance.send_message(
            {
                "message": message,
                "task_id": task.get("id"),
                "job_id": f"split_{uuid.uuid4()}",  # Generate a unique ID for this notification
                "server_id": server_id,
                "channel_id": channel_id,
                "type": "info",
            }
        )
    except Exception as e:
        print(f"Error sending split schedule notification: {str(e)}")
