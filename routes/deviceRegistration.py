import asyncio

import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Depends

from pymongo import MongoClient, UpdateOne

from pydantic import BaseModel

from typing import List

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
)
import copy




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


class PauseTaskCommandRequest(BaseModel):

    command: dict

    Task_ids: List[str]


class ResumeTaskCommandRequest(BaseModel):

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

        tasks = list(tasks_collection.find({"id": {"$in": task_ids}}))



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

        tasks = list(tasks_collection.find({"id": {"$in": task_ids}}))



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



                    # Check if task is still active and update status

                    task = tasks_collection.find_one({"id": task_id})

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

                                    tasks_collection.update_one(

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

                                tasks_collection.update_one(

                                    {"id": task_id}, {"$set": {"status": status}}

                                )

                        else:

                            # Normal status update

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

    # Update only non-null inputs/schedules; never set to null
    update_fields = {"deviceIds": device_ids}
    if newInputs is not None:
        update_fields["inputs"] = newInputs
    if newSchedules is not None:
        update_fields["schedules"] = newSchedules
    if update_fields:
        tasks_collection.update_one({"id": task_id}, {"$set": update_fields})


    try:

        user_tz = pytz.timezone(time_zone)

        now = datetime.now(user_tz)

        print(f"Current time in {time_zone}: {now}")



        if durationType in ["DurationWithExactStartTime", "ExactStartTime"]:

            # Clear old scheduled jobs before scheduling new fixed start time

            print(f"[LOG] Clearing old fixed start time jobs for task {task_id}")

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

            tasks_collection.update_one(

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

            tasks_collection.update_one(

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

            task = tasks_collection.find_one({"id": task_id}, {"activeJobs": 1})

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

            tasks_collection.update_one(

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
                task_doc = tasks_collection.find_one({"id": task_id}) or {}
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
                        bot_doc = bots_collection.find_one({"id": bot_id}, {"schedules": 1}) or {}
                        schedules_payload = bot_doc.get("schedules", [])

                # Parse weekly-specific fields from command-level
                week_start_raw = command.get("week_start")
                follow_weekly_range = tuple(command.get("followWeeklyRange", []))
                rest_days_range = tuple(command.get("restDaysRange", []))
                no_two_high_rule_raw = command.get("noTwoHighRule")
                test_mode = bool(command.get("testMode", False))  # Test mode: schedule with 10-min gaps
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
                    logger.warning(f"[WEEKLY] ⚠️ TEST MODE ENABLED - Scheduling with 10-minute gaps starting NOW")

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
                        raise ValueError("Invalid week_start")

                    user_tz = pytz.timezone(time_zone)
                    if local_dt.tzinfo is None:
                        local_dt = user_tz.localize(local_dt)
                    else:
                        local_dt = local_dt.astimezone(user_tz)

                    logger.info(f"[WEEKLY] Parsing week_start: '{week_start_raw}' → '{local_dt.isoformat()}'")
                except Exception:
                    raise HTTPException(status_code=400, detail="Invalid week_start format")

                # Monday check (warn-only)
                if local_dt.weekday() != 0:
                    logger.warning("[WEEKLY] week_start is not Monday; proceeding anyway")

                # Test mode: override week_start to use current time for immediate scheduling
                if test_mode:
                    local_dt = datetime.now(user_tz)
                    logger.warning(f"[WEEKLY] Test mode: Overriding week_start to NOW: {local_dt.isoformat()}")

                # Calculate and log daily bounds
                bounds = calculate_daily_bounds(
                    (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                    (int(rest_days_range[0]), int(rest_days_range[1])),
                    (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                )
                logger.info(
                    f"[WEEKLY] Weekly Constraints: Total range: {follow_weekly_range[0]}-{follow_weekly_range[1]} follows/week | Rest days: {rest_days_range[0]}-{rest_days_range[1]} | NoTwoHighRule: {no_two_high_rule} | Calculated daily bounds: {bounds['daily_min']}-{bounds['daily_max']} follows/day"
                )

                # Generate and validate schedule
                # Default ranges for rest day likes, comments, and duration (can be made configurable later)
                rest_day_likes_range = (4, 10)  # 4-10 likes per rest day
                rest_day_comments_range = (1, 3)  # 1-3 comments per rest day
                rest_day_duration_range = (30, 120)  # 30-120 minutes per rest day
                
                generated = generate_weekly_targets(
                    local_dt,
                    (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                    (int(rest_days_range[0]), int(rest_days_range[1])),
                    (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                    method,
                    rest_day_likes_range,
                    rest_day_comments_range,
                    rest_day_duration_range,
                )
                validate_weekly_plan(
                    generated,
                    (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                    (int(rest_days_range[0]), int(rest_days_range[1])),
                    (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                )

                logger.info(f"[WEEKLY] Generated {len(generated)} days")

                # --- BEGIN full weekly schedule Discord summary ---
                try:
                    task_meta = tasks_collection.find_one(
                        {"id": task_id}, {"_id": 0, "taskName": 1, "serverId": 1, "channelId": 1}
                    ) or {}

                    server_id = task_meta.get("serverId")
                    channel_id = task_meta.get("channelId")

                    if server_id and channel_id:
                        # Build readable schedule table with actual calendar dates
                        day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                        method_names = {
                            0: "Off Day",
                            1: "Method 1: Follow Suggestions",
                            4: "Method 4: Unfollow Non-Followers",
                            9: "Method 9: Warmup"
                        }
                        
                        # Compute planned start/end times once and reuse for summary and scheduling
                        planned_schedule = []
                        for d in generated:
                            idx = int(d.get("dayIndex", 0))
                            is_rest = bool(d.get("isRest", False))
                            is_off = bool(d.get("isOff", False))
                            target = int(d.get("target", 0))
                            day_method = int(d.get("method", 0 if is_off else (9 if is_rest else 1)))
                            method_label = method_names.get(day_method, f"method {day_method}")
                            
                            if test_mode:
                                start_time_local = local_dt + timedelta(minutes=idx * 10)
                                end_time_local = start_time_local + timedelta(hours=11)
                            else:
                                day_local = local_dt + timedelta(days=idx)
                                start_window_start = day_local.replace(hour=11, minute=0, second=0, microsecond=0)
                                start_window_end = day_local.replace(hour=22, minute=0, second=0, microsecond=0)
                                window_minutes = int((start_window_end - start_window_start).total_seconds() // 60)
                                if window_minutes <= 0:
                                    # Skip invalid window day
                                    continue
                                random_offset = random.randint(0, window_minutes - 1)
                                start_time_local = start_window_start + timedelta(minutes=random_offset)
                                end_time_local = start_window_end
                            
                            entry = {
                                "dayIndex": idx,
                                "isRest": is_rest,
                                "isOff": is_off,
                                "target": target,
                                "method": day_method,
                                "methodLabel": method_label,
                                "start_local": start_time_local,
                                "end_local": end_time_local,
                            }
                            if is_rest:
                                entry["maxLikes"] = int(d.get("maxLikes", 10))
                                entry["maxComments"] = int(d.get("maxComments", 5))
                                entry["warmupDuration"] = int(d.get("warmupDuration", 60))
                            planned_schedule.append(entry)
                        
                        # --- Per-account weekly plans (randomized per account) ---
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
                                                if isinstance(uname, str):
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

                        # Build summary lines with planned start times (show per-account caps on active days; no global headline)
                        lines = []
                        for p in planned_schedule:
                            actual_day_name = day_names[p["start_local"].weekday()]
                            date_str = p["start_local"].strftime("%b %d")
                            time_str = p["start_local"].strftime("%H:%M")
                            if p["isOff"]:
                                label = f"{actual_day_name} {date_str}: Off day - No tasks scheduled"
                            elif p["isRest"]:
                                label = f"{actual_day_name} {date_str} {time_str}: {p['methodLabel']} - {p.get('maxLikes', 10)} likes, {p.get('maxComments', 5)} comments, {p.get('warmupDuration', 60)}min"
                            else:
                                # Per-account breakdown
                                per_account_str = ""
                                if per_account_plans and account_usernames:
                                    di = int(p.get("dayIndex", 0))
                                    parts = []
                                    for uname in account_usernames:
                                        v = (per_account_plans.get(uname) or [0]*7)[di]
                                        parts.append(f"{uname}: {v}")
                                    per_account_str = f" | Accounts: " + ", ".join(parts)
                                label = f"{actual_day_name} {date_str} {time_str}: {p['methodLabel']}{per_account_str}"
                            lines.append(label)
                        
                        test_mode_indicator = "TEST MODE (10-min gaps)\n" if test_mode else ""
                        summary = (
                            f"Weekly plan scheduled: {task_meta.get('taskName', 'Unknown Task')}\n"
                            f"{test_mode_indicator}"
                            f"Week start: {local_dt.strftime('%a %b %d')}\n"
                            f"Weekly target range: {int(follow_weekly_range[0])}–{int(follow_weekly_range[1])}\n"
                            f"Rest days range: {int(rest_days_range[0])}–{int(rest_days_range[1])}\n\n" + "\n".join(lines)
                        )

                        from Bot.discord_bot import get_bot_instance
                        bot = get_bot_instance()
                        bot.send_message_sync(
                            {
                                "message": summary,
                                "task_id": task_id,
                                "job_id": f"weekly_summary_{task_id}",
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
                    "method": method,
                }

                weekly_entry = {
                    "type": "WeeklyRandomizedPlan",
                    "weeklyData": weekly_data,
                    "generatedSchedule": generated,
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

                # Ensure index 2 is WeeklyRandomizedPlan
                while len(schedules_inputs) < 3:
                    schedules_inputs.append({})
                schedules_inputs[2] = weekly_entry
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
                tasks_collection.update_one({"id": task_id}, {"$set": persist_update})
                logger.info(f"[WEEKLY] Persisted to DB: inputs={new_inputs_from_command is not None}, schedules={schedules_payload is not None}")

                # NOTE: Instant caps logic DISABLED - each job applies its own caps when it runs
                # Applying instant caps breaks rest days (Day 0 would get Day 1's caps)
                # The per-job mutation logic in send_command_to_devices handles this correctly
                logger.info("[WEEKLY] Skipping instant caps - will be applied per-job when each day runs")

                # Clear old scheduled jobs before scheduling weekly plan
                print(f"[WEEKLY] Clearing old jobs for task {task_id}")
                task = tasks_collection.find_one({"id": task_id}, {"activeJobs": 1})
                if task and task.get("activeJobs"):
                    for old_job in task["activeJobs"]:
                        old_job_id = old_job.get("job_id")
                        if old_job_id:
                            try:
                                scheduler.remove_job(old_job_id)
                            except Exception:
                                pass
                tasks_collection.update_one({"id": task_id}, {"$set": {"activeJobs": []}})

                # Schedule 7 jobs with random local start time 11:00–22:00, end at 22:00
                # Also schedule "1 hour before" reminder notifications
                scheduled_job_ids = []
                notify_daily = bool(command.get("notifyDaily", False))
                total_target = 0
                active_days = 0
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
                                        if isinstance(uname_caps, str):
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

                # Reuse same per-account plan if available; otherwise compute now
                if account_usernames_for_caps:
                    if 'per_account_plans' in locals() and per_account_plans:
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

                for p in planned_schedule:
                    day_index = int(p.get("dayIndex", 0))
                    target_count = int(p.get("target", 0))
                    is_rest = bool(p.get("isRest", False))
                    is_off = bool(p.get("isOff", False))
                    day_method = int(p.get("method", 0 if is_off else (9 if is_rest else 1)))
                    
                    # Skip off days - no tasks scheduled
                    if is_off or day_method == 0:
                        continue
                    
                    total_target += target_count
                    if not is_rest:
                        active_days += 1

                    # Reuse planned times
                    start_time_local = p["start_local"]
                    end_time_local = p["end_local"]

                    start_time_utc = start_time_local.astimezone(pytz.UTC)
                    end_time_utc = end_time_local.astimezone(pytz.UTC)

                    job_id = f"cmd_{uuid.uuid4()}"
                    
                    # Deep copy command to prevent shared object references between jobs
                    job_command = copy.deepcopy(command)
                    job_command.update({
                        "job_id": job_id,
                        # For device execution:
                        "dayIndex": day_index,
                        # Device should rely on per-account caps only
                        "dailyTarget": 0,
                        "method": day_method,  # Randomly 1 or 4 for active days, 9 for rest days
                    })
                    
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
                    # Note: method 1 = Follow Suggestions, 4 = Unfollow Non-Followers, 9 = Warmup

                    # Apply per-account caps into newInputs and legacy inputs for active days (method != 9)
                    if day_method != 9 and account_usernames_for_caps and per_account_plans_caps:
                        try:
                            logger.info(f"[WEEKLY-CAPS] Applying per-account caps for day {day_index}, method {day_method}")
                            logger.info(f"[WEEKLY-CAPS] Accounts: {account_usernames_for_caps}")
                            logger.info(f"[WEEKLY-CAPS] Per-account plans available: {list(per_account_plans_caps.keys())}")
                            
                            adjusted_inputs = copy.deepcopy(job_command.get("newInputs"))
                            def set_follow_daily_for_account(block, uname, v):
                                if block.get("input") is True:
                                    if "minFollowsDaily" in block:
                                        block["minFollowsDaily"] = max(1, int(v) - 1)
                                    if "maxFollowsDaily" in block:
                                        block["maxFollowsDaily"] = int(v)
                                    # Calculate dynamic hourly limits based on daily target
                                    # Formula: min = daily * 0.2, max = daily * 0.5
                                    min_hourly = max(1, int(int(v) * 0.2))
                                    max_hourly = max(min_hourly + 1, int(int(v) * 0.5))
                                    if "minFollowsPerHour" in block:
                                        block["minFollowsPerHour"] = min_hourly
                                    if "maxFollowsPerHour" in block:
                                        block["maxFollowsPerHour"] = max_hourly

                            if isinstance(adjusted_inputs, dict):
                                inputs_arr = adjusted_inputs.get("inputs", [])
                                for wrap in (inputs_arr or []):
                                    inner = wrap.get("inputs", [])
                                    for node in (inner or []):
                                        if isinstance(node, dict) and node.get("type") == "instagrmFollowerBotAcountWise":
                                            for acc in (node.get("Accounts") or []):
                                                uname = acc.get("username")
                                                if uname in account_usernames_for_caps:
                                                    v = (per_account_plans_caps.get(uname) or [0]*7)[day_index]
                                                    for blk in (acc.get("inputs") or []):
                                                        # Only adjust the block matching the day's method
                                                        name = blk.get("name") or ""
                                                        if day_method == 1:
                                                            if name in ("Follow from Notification Suggestions", "Follow from Profile Posts"):
                                                                set_follow_daily_for_account(blk, uname, v)
                                                        elif day_method == 4:
                                                            # Tolerant detection: match by type or name prefix to avoid label drift
                                                            is_unfollow_block = (
                                                                (blk.get("type") == "toggleAndUnFollowInputs")
                                                                or (name.startswith("Unfollow Non-Followers"))
                                                            )
                                                            if is_unfollow_block:
                                                                # Force enable unfollow block for this per-day method
                                                                blk["input"] = True
                                                                # ⚠️ CRITICAL: Android expects "minFollowsDaily" NOT "minUnFollowsDaily"
                                                                # Android reuses follow field names for unfollowing (quirk of the codebase)
                                                                blk["minFollowsDaily"] = max(1, int(v) - 1)
                                                                blk["maxFollowsDaily"] = int(v)
                                                                # Calculate dynamic hourly limits based on daily target
                                                                # Formula: min = daily * 0.2, max = daily * 0.5
                                                                min_hourly = max(1, int(int(v) * 0.2))
                                                                max_hourly = max(min_hourly + 1, int(int(v) * 0.5))
                                                                blk["minFollowsPerHour"] = min_hourly
                                                                blk["maxFollowsPerHour"] = max_hourly
                            job_command["newInputs"] = adjusted_inputs

                            # Also update legacy structure `inputs` so the device (which reads legacy) gets the same per-account caps
                            adjusted_legacy = copy.deepcopy(job_command.get("inputs"))
                            if isinstance(adjusted_legacy, list):
                                for acct in adjusted_legacy:
                                    uname_legacy = acct.get("username")
                                    if uname_legacy in account_usernames_for_caps:
                                        v = (per_account_plans_caps.get(uname_legacy) or [0]*7)[day_index]
                                        blocks = acct.get("inputs")
                                        if isinstance(blocks, list):
                                            for blk in blocks:
                                                # Legacy blocks have method name as key with boolean true/false and the numeric fields beside
                                                # For method 1: check if block has the key (regardless of current value)
                                                if day_method == 1 and "Follow from Notification Suggestions" in blk:
                                                    # Force enable for this method
                                                    blk["Follow from Notification Suggestions"] = True
                                                    # Apply caps
                                                    blk["minFollowsDaily"] = max(1, int(v) - 1)
                                                    blk["maxFollowsDaily"] = int(v)
                                                    # Calculate dynamic hourly limits based on daily target
                                                    # Formula: min = daily * 0.2, max = daily * 0.5
                                                    min_hourly = max(1, int(int(v) * 0.2))
                                                    max_hourly = max(min_hourly + 1, int(int(v) * 0.5))
                                                    blk["minFollowsPerHour"] = min_hourly
                                                    blk["maxFollowsPerHour"] = max_hourly
                                                # For method 4: check if block has the key (regardless of current value)
                                                elif day_method == 4 and "Unfollow Non-Followers" in blk:
                                                    # Force enable for this method
                                                    blk["Unfollow Non-Followers"] = True
                                                    # ⚠️ CRITICAL: Android expects "minFollowsDaily" NOT "minUnFollowsDaily"
                                                    # Android reuses follow field names for unfollowing (quirk of the codebase)
                                                    blk["minFollowsDaily"] = max(1, int(v) - 1)
                                                    blk["maxFollowsDaily"] = int(v)
                                                    # Calculate dynamic hourly limits based on daily target
                                                    # Formula: min = daily * 0.2, max = daily * 0.5
                                                    min_hourly = max(1, int(int(v) * 0.2))
                                                    max_hourly = max(min_hourly + 1, int(int(v) * 0.5))
                                                    blk["minFollowsPerHour"] = min_hourly
                                                    blk["maxFollowsPerHour"] = max_hourly
                                                    logger.info(f"[WEEKLY-CAPS] Set unfollow caps for {uname_legacy}: daily={int(v)}, hourly=({min_hourly}-{max_hourly})")
                                                # Extend here for other methods if needed
                            job_command["inputs"] = adjusted_legacy
                        except Exception as e:
                            logger.warning(f"[WEEKLY] Failed to inject per-account caps: {e}")

                    # Add job to scheduler
                    try:
                        scheduler.add_job(
                            wrapper_for_send_command,
                            trigger=DateTrigger(run_date=start_time_utc, timezone=pytz.UTC),
                            args=[device_ids, job_command],
                            id=job_id,
                            name=f"Weekly day {day_index} for devices {device_ids}",
                        )
                        scheduled_job_ids.append(job_id)
                        # Update DB activeJobs immediately
                        job_instance = {
                            "job_id": job_id,
                            "startTime": start_time_utc,
                            "endTime": end_time_utc,
                            "device_ids": device_ids,
                        }
                        tasks_collection.update_one(
                            {"id": task_id},
                            {
                                "$set": {"status": "scheduled"},
                                "$unset": {"scheduledTime": ""},
                                "$push": {"activeJobs": job_instance},
                            },
                        )
                        # Optional per-day notification (disabled by default for weekly plans)
                        if notify_daily:
                            task_for_notify = tasks_collection.find_one({"id": task_id})
                            device_docs = list(
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
                        
                        # Schedule reminder notification
                        # Test mode: 2 minutes before, Normal mode: 1 hour before
                        if test_mode:
                            reminder_time_utc = start_time_utc - timedelta(minutes=2)
                            reminder_label = "2 minutes"
                        else:
                            reminder_time_utc = start_time_utc - timedelta(hours=1)
                            reminder_label = "1 hour"
                        
                        if reminder_time_utc > datetime.now(pytz.UTC):
                            reminder_job_id = f"reminder_{job_id}"
                            method_names_reminder = {
                                1: "Method 1: Follow Suggestions",
                                4: "Method 4: Unfollow Non-Followers",
                                9: "Method 9: Warmup"
                            }
                            method_label_reminder = method_names_reminder.get(day_method, f"method {day_method}")
                            if is_rest:
                                # Show likes, comments, and duration for rest day reminders
                                max_likes = int(p.get("maxLikes", 10))
                                max_comments = int(p.get("maxComments", 5))
                                warmup_duration = int(p.get("warmupDuration", 60))
                                activity_text = f"{method_label_reminder} - {max_likes} likes, {max_comments} comments, {warmup_duration}min"
                            else:
                                activity_text = f"{target_count} follows ({method_label_reminder})"
                                activity_text = f"{target_count} follows ({method_label_reminder})"
                            
                            try:
                                scheduler.add_job(
                                    send_weekly_reminder,
                                    trigger=DateTrigger(run_date=reminder_time_utc, timezone=pytz.UTC),
                                    args=[task_id, actual_day_name, start_time_local.strftime('%Y-%m-%d %H:%M'), activity_text, time_zone, test_mode],
                                    id=reminder_job_id,
                                    name=f"Reminder for day {day_index} of task {task_id}",
                                )
                                logger.info(f"[WEEKLY] ⏰ Scheduled reminder for Day {day_index}: {reminder_label} before start")
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
                    f"[WEEKLY] ✅ Weekly plan scheduled: {task_id} | Total weekly target: {total_target} | Active days: {active_days} | Rest days: {7 - active_days}"
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

                        update_result = tasks_collection.update_one(

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

                                update_result = tasks_collection.update_one(

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





def send_weekly_reminder(task_id, day_name, start_time_formatted, activity_text, time_zone, test_mode=False):
    """
    Send a reminder notification before weekly tasks start
    Test mode: 2 minutes before
    Normal mode: 1 hour before
    """
    try:
        from Bot.discord_bot import get_bot_instance
        
        # Fetch task details from database
        task = tasks_collection.find_one(
            {"id": task_id},
            {"taskName": 1, "serverId": 1, "channelId": 1, "status": 1, "_id": 0}
        )
        
        # Skip if task unscheduled or missing discord config
        if not task or not task.get("serverId") or not task.get("channelId"):
            logger.warning(f"[WEEKLY-REMINDER] Cannot send reminder: task {task_id} missing Discord config")
            return
        if task.get("status") != "scheduled":
            logger.info(f"[WEEKLY-REMINDER] Skipping reminder for task {task_id}: status is '{task.get('status')}'")
            return
        
        task_name = task.get("taskName", "Weekly Task")
        server_id = task.get("serverId")
        channel_id = task.get("channelId")
        
        # Format message based on mode
        time_before = "2 Minutes" if test_mode else "1 Hour"
        message = (
            f"⚠️ **Weekly Task Starting in {time_before}**\n"
            f"📅 **Day**: {day_name}\n"
            f"⏰ **Start Time**: {start_time_formatted} ({time_zone})\n"
            f"🎯 **Activity**: {activity_text}\n"
            f"🧩 **Task**: {task_name}"
        )
        
        # Send via Discord bot
        bot = get_bot_instance()
        bot.send_message_sync({
            "message": message,
            "task_id": task_id,
            "job_id": f"reminder_{task_id}",
            "server_id": int(server_id) if isinstance(server_id, str) and server_id.isdigit() else server_id,
            "channel_id": int(channel_id) if isinstance(channel_id, str) and channel_id.isdigit() else channel_id,
            "type": "warning",  # Uses the "⚠️ Task Starting Soon" embed
        })
        
        logger.info(f"[WEEKLY-REMINDER] Sent 1-hour reminder for task {task_id}, day {day_name}")
    
    except Exception as e:
        logger.error(f"[WEEKLY-REMINDER] Failed to send reminder: {e}")


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
    if method_value in method_to_input_map:
        target_input_name = method_to_input_map[method_value]
        logger.info(f"[METHOD-ENABLE] Detected method: {method_value} for task {task_id}, enabling '{target_input_name}'")
        
        # Modify legacy inputs format
        inputs_list = command.get("inputs", [])
        if isinstance(inputs_list, list):
            for account_data in inputs_list:
                if isinstance(account_data, dict) and "inputs" in account_data:
                    account_inputs = account_data["inputs"]
                    if isinstance(account_inputs, list):
                        for block in account_inputs:
                            if isinstance(block, dict):
                                # Disable all methods
                                for method_name in method_to_input_map.values():
                                    if method_name in block:
                                        block[method_name] = False
                                # Enable the target method
                                if target_input_name in block:
                                    block[target_input_name] = True
                                    if method_value == 9:
                                        # Use randomized warmup parameters from command
                                        warmup_duration = command.get("warmupDuration", 60)
                                        max_likes = command.get("maxLikes", 10)
                                        max_comments = command.get("maxComments", 5)
                                        block["warmupDuration"] = warmup_duration
                                        block["maxLikes"] = max_likes
                                        block["maxComments"] = max_comments
                                    logger.info(f"[METHOD-ENABLE] Enabled '{target_input_name}' in legacy inputs")
        
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
                                            acc_inputs = account.get("inputs", [])
                                            if isinstance(acc_inputs, list):
                                                for block in acc_inputs:
                                                    if isinstance(block, dict):
                                                        name = block.get("name", "")
                                                        # Disable all methods
                                                        if name in method_to_input_map.values():
                                                            block["input"] = False
                                                        # Enable the target method
                                                        if name == target_input_name:
                                                            block["input"] = True
                                                            if method_value == 9:
                                                                # Use randomized warmup parameters from command
                                                                warmup_duration = command.get("warmupDuration", 60)
                                                                max_likes = command.get("maxLikes", 10)
                                                                max_comments = command.get("maxComments", 5)
                                                                block["warmupDuration"] = warmup_duration
                                                                block["maxLikes"] = max_likes
                                                                block["maxComments"] = max_comments
                                                            logger.info(f"[METHOD-ENABLE] Enabled '{target_input_name}' (account-wise)")



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



        if results["success"]:

            logger.info("Command successfully executed. Updating task status.")
            
            # ========== WEEKLY AUTO-RENEWAL LOGIC ==========
            # Check if this is the last day of a weekly plan and auto-schedule next week
            day_index = command.get("dayIndex")
            if day_index == 6:  # Day 6 = Sunday (last day of week)
                # Fetch full task to check if it's a weekly plan
                weekly_check_task = await asyncio.to_thread(
                    tasks_collection.find_one,
                    {"id": task_id},
                    {"durationType": 1, "schedules": 1, "inputs": 1, "timeZone": 1, "email": 1, "deviceIds": 1}
                )
                
                if weekly_check_task and weekly_check_task.get("durationType") == "WeeklyRandomizedPlan":
                    logger.info(f"[WEEKLY-RENEWAL] Day 6 completed for task {task_id}. Auto-scheduling next week...")
                    
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
                                        
                                        # Next Monday is 7 days later
                                        next_week_start = current_week_start + timedelta(days=7)
                                        
                                        # Generate new weekly schedule
                                        follow_weekly_range = tuple(weekly_data.get("followWeeklyRange", [50, 100]))
                                        rest_days_range = tuple(weekly_data.get("restDaysRange", [1, 2]))
                                        no_two_high_rule = tuple(weekly_data.get("noTwoHighRule", [27, 23]))
                                        method = int(weekly_data.get("method", 1))
                                        
                                        # Use same default ranges for auto-renewal
                                        rest_day_likes_range = (4, 10)  # 4-10 likes per rest day
                                        rest_day_comments_range = (1, 3)  # 1-3 comments per rest day
                                        rest_day_duration_range = (30, 120)  # 30-120 minutes per rest day
                                        
                                        generated = generate_weekly_targets(
                                            next_week_start,
                                            (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                                            (int(rest_days_range[0]), int(rest_days_range[1])),
                                            (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                                            method,
                                            rest_day_likes_range,
                                            rest_day_comments_range,
                                            rest_day_duration_range,
                                        )
                                        validate_weekly_plan(
                                            generated,
                                            (int(follow_weekly_range[0]), int(follow_weekly_range[1])),
                                            (int(rest_days_range[0]), int(rest_days_range[1])),
                                            (int(no_two_high_rule[0]), int(no_two_high_rule[1])),
                                        )
                                        
                                        logger.info(f"[WEEKLY-RENEWAL] Generated next week starting {next_week_start.isoformat()}")
                                        
                                        # Update DB with new schedule
                                        new_weekly_data = {
                                            "week_start": next_week_start.replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),
                                            "followWeeklyRange": [int(follow_weekly_range[0]), int(follow_weekly_range[1])],
                                            "restDaysRange": [int(rest_days_range[0]), int(rest_days_range[1])],
                                            "noTwoHighRule": [int(no_two_high_rule[0]), int(no_two_high_rule[1])],
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
                                                    # Device should rely on per-account caps only
                                                    "dailyTarget": 0,
                                                    "method": day_method,
                                                    "inputs": inputs_for_renewal,
                                                    "newInputs": inputs_for_renewal,
                                                    "timeZone": time_zone,
                                                    "appName": "Instagram Followers Bot",
                                                }
                                                
                                                # Add likes, comments, and duration for rest days (method 9)
                                                if day_method == 9 and is_rest:
                                                    max_likes = day.get("maxLikes", 10)
                                                    max_comments = day.get("maxComments", 5)
                                                    warmup_duration = day.get("warmupDuration", 60)
                                                    job_command_new.update({
                                                        "maxLikes": max_likes,
                                                        "maxComments": max_comments,
                                                        "warmupDuration": warmup_duration
                                                    })
                                                
                                                scheduler.add_job(
                                                    wrapper_for_send_command,
                                                    trigger=DateTrigger(run_date=start_time_utc, timezone=pytz.UTC),
                                                    args=[device_ids_for_renewal, job_command_new],
                                                    id=job_id_new,
                                                    name=f"Weekly day {day_index_new} for devices {device_ids_for_renewal}",
                                                )
                                                
                                                job_instance = {
                                                    "job_id": job_id_new,
                                                    "startTime": start_time_utc,
                                                    "endTime": end_time_utc,
                                                    "device_ids": device_ids_for_renewal,
                                                }
                                                await asyncio.to_thread(
                                                    tasks_collection.update_one,
                                                    {"id": task_id},
                                                    {"$push": {"activeJobs": job_instance}}
                                                )
                                                
                                                # Log with actual calendar date
                                                day_names_log = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                                                actual_day_log = day_names_log[start_time_local.weekday()]
                                                logger.info(f"[WEEKLY-RENEWAL] Scheduled Day {day_index_new} ({actual_day_log} {start_time_local.strftime('%b %d')}): {start_time_local.strftime('%H:%M')} (method {day_method})")
                                                
                                                # Schedule "1 hour before" reminder for renewed week
                                                # Auto-renewal is always normal mode (never test mode)
                                                reminder_time_utc_renewal = start_time_utc - timedelta(hours=1)
                                                if reminder_time_utc_renewal > datetime.now(pytz.UTC):
                                                    reminder_job_id_renewal = f"reminder_{job_id_new}"
                                                    method_names_renewal = {1: "Method 1: Follow Suggestions", 4: "Method 4: Unfollow Non-Followers", 9: "Method 9: Warmup"}
                                                    method_label_renewal = method_names_renewal.get(day_method, f"method {day_method}")
                                                    activity_text_renewal = f"{target_count} follows ({method_label_renewal})" if not is_rest else f"{method_label_renewal}"
                                                    
                                                    try:
                                                        scheduler.add_job(
                                                            send_weekly_reminder,
                                                            trigger=DateTrigger(run_date=reminder_time_utc_renewal, timezone=pytz.UTC),
                                                            args=[task_id, actual_day_log, start_time_local.strftime('%Y-%m-%d %H:%M'), activity_text_renewal, time_zone, False],
                                                            id=reminder_job_id_renewal,
                                                            name=f"Reminder for renewed day {day_index_new} of task {task_id}",
                                                        )
                                                        logger.info(f"[WEEKLY-RENEWAL] ⏰ Scheduled reminder for renewed Day {day_index_new}: 1 hour before start")
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
                                                            label = f"{actual_day_name} {date_str}: Off day - No tasks scheduled"
                                                        elif is_rest_d:
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
                                                        try:
                                                            accounts_notify = account_usernames if account_usernames else accounts_notify
                                                        except NameError:
                                                            pass
                                                        try:
                                                            per_acc_notify = per_account_plans if per_account_plans else per_acc_notify
                                                        except NameError:
                                                            pass
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
                                                    
                                                    from Bot.discord_bot import get_bot_instance
                                                    bot = get_bot_instance()
                                                    bot.send_message_sync({
                                                        "message": summary,
                                                        "task_id": task_id,
                                                        "job_id": f"weekly_renewal_{task_id}",
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



                # Get the bot instance and use the thread-safe method

                bot = get_bot_instance()

                bot.send_message_sync(

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

                # Get the bot instance and use the thread-safe method

                bot = get_bot_instance()

                bot.send_message_sync(

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

            {

                "$set": {"status": "scheduled"},

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

                "$push": {"activeJobs": jobInstance}

                # Remove the "$unset": {"scheduledTime": ""} line for daily tasks

            }

        else:

            # Only update activeJobs if status is not "awaiting"

            update_operation = {

                "$push": {"activeJobs": jobInstance}

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



        # Update status only if it's not 'running' AND clear scheduledTime

        tasks_collection.update_one(

            {"id": task_id, "status": {"$ne": "running"}},

            {

                "$set": {"status": "scheduled"},

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

