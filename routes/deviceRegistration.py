

import asyncio
import time
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Depends
from pymongo import MongoClient
from pydantic import BaseModel
from typing import List
import random
from datetime import datetime, timedelta
from utils.utils import get_current_user, check_for_Job_clashes, split_message
from models.tasks import tasks_collection
# from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
import pytz
import json
import uuid
from fastapi.responses import JSONResponse
from Bot.discord_bot import bot_instance

# MongoDB Connection
client = MongoClient(
    "mongodb+srv://abdullahnoor94:dodge2018@appilot.ds9ll.mongodb.net/?retryWrites=true&w=majority&appName=Appilot")
db = client["Appilot"]
device_collection = db["devices"]

# Create Router
device_router = APIRouter(prefix="")
executors = {
    'default': ThreadPoolExecutor(20)  # 4 cores * 5
}
job_defaults = {
    'misfire_grace_time': 300,  # Allow jobs to execute up to 5 minutes late
    'coalesce': False,          # Don't combine missed executions
    'max_instances': 10         # Allow many instances of the same job to run concurrently
}
scheduler = AsyncIOScheduler(executors=executors, job_defaults=job_defaults)
# scheduler = AsyncIOScheduler()
scheduler.start()
print('started scheduling')

# In-memory storage for WebSocket connections and mapping them to device IDs
active_connections: List[WebSocket] = []
device_connections = {}  # To store device_id: websocket mapping

# Device registration model with updated fields
class DeviceRegistration(BaseModel):
    deviceName: str
    deviceId: str
    model: str
    botName: List[str]
    status: bool = True
    activationDate: str
    email: str

# Command model
class CommandRequest(BaseModel):
    command: dict
    device_ids: List[str]
    
class StopTaskCommandRequest(BaseModel):
    command: dict
    Task_ids: List[str]


def register_device(device_data: DeviceRegistration):
    device = device_collection.find_one({"deviceId": device_data.deviceId})
    if device:
        raise HTTPException(
            status_code=400, detail="Device already registered")
    device_collection.insert_one(device_data.dict())
    return {"message": "Device registered successfully", "deviceId": device_data.deviceId}

# Device Registration Endpoint (POST)
@device_router.post("/register_device")
async def register_device_endpoint(device_data: DeviceRegistration):
    return register_device(device_data)

# Endpoint to check the device status
@device_router.get("/device_status/{device_id}")
async def check_device_status(device_id: str):
    device = device_collection.find_one({"deviceId": device_id})
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    return {"device_id": device_id, "status": device["status"]}

# Endpoint to update the device status
@device_router.put("/update_status/{device_id}")
async def update_device_status(device_id: str, status: bool):
    device = device_collection.find_one({"deviceId": device_id})
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    device_collection.update_one({"deviceId": device_id}, {
                                 "$set": {"status": status}})
    return {"message": f"Device {device_id} status updated to {status}"}

# Endpoint for checking device registration
@device_router.get("/device_registration/{device_id}")
async def check_device_registration(device_id: str):
    device = device_collection.find_one({"deviceId": device_id})
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    if not device["status"]:
        device_collection.update_one({"deviceId": device_id}, {
                                     "$set": {"status": True}})

    return True

# @device_router.websocket("/ws/{device_id}")
# async def websocket_endpoint(websocket: WebSocket, device_id: str):
#     await websocket.accept()
#     device_connections[device_id] = websocket
#     active_connections.append(websocket)

#     try:
#         while True:
#             # Receive JSON message from client
#             data = await websocket.receive_text()
#             print(f"Message from {device_id}: {data}")

#             try:
                
                
#                 payload = json.loads(data)
#                 message = payload.get("message")
#                 task_id = payload.get("task_id")
#                 job_id = payload.get("job_id")
#                 message_type = payload.get("type")
#                 print(f"Parsed payload: message={message}, task_id={task_id}, job_id={job_id}")
                
                
#                 if message_type == "ping":
#                     pong_response = {
#                         "type": "pong",
#                         "timestamp": payload.get("timestamp", int(time.time() * 1000))
#                     }
#                     await websocket.send_text(json.dumps(pong_response))
#                     print(f"Sent pong response to ({device_id})")
#                     continue
                
                
#                 taskData = tasks_collection.find_one(
#                     {"id": task_id}, {"serverId": 1, "channelId": 1, "_id": 0}
#                 )

#                 if message_type == "update":
#                     print(f"Processing 'update' message for task_id {task_id}")
#                     if taskData and taskData.get("serverId") and taskData.get("channelId"):
#                         server_id = int(taskData["serverId"]) if isinstance(
#                             taskData["serverId"], str) and taskData["serverId"].isdigit() else taskData["serverId"]
#                         channel_id = int(taskData["channelId"]) if isinstance(
#                             taskData["channelId"], str) and taskData["channelId"].isdigit() else taskData["channelId"]

#                         await bot_instance.send_message({
#                             "message": message,
#                             "task_id": task_id,
#                             "job_id": job_id,
#                             "server_id": server_id,
#                             "channel_id": channel_id,
#                             "type": "update"
#                         })

#                 elif message_type == "error":
#                     print(f"Processing 'error' message for task_id {task_id}")
#                     if taskData and taskData.get("serverId") and taskData.get("channelId"):
#                         server_id = int(taskData["serverId"]) if isinstance(
#                             taskData["serverId"], str) and taskData["serverId"].isdigit() else taskData["serverId"]
#                         channel_id = int(taskData["channelId"]) if isinstance(
#                             taskData["channelId"], str) and taskData["channelId"].isdigit() else taskData["channelId"]

#                         await bot_instance.send_message({
#                             "message": message,
#                             "task_id": task_id,
#                             "job_id": job_id,
#                             "server_id": server_id,
#                             "channel_id": channel_id,
#                             "type": "error"
#                         })

#                 elif message_type == "final":
#                     print(f"Processing 'final' message for task_id {task_id}")
#                     if taskData and taskData.get("serverId") and taskData.get("channelId"):
#                         server_id = int(taskData["serverId"]) if isinstance(
#                             taskData["serverId"], str) and taskData["serverId"].isdigit() else taskData["serverId"]
#                         channel_id = int(taskData["channelId"]) if isinstance(
#                             taskData["channelId"], str) and taskData["channelId"].isdigit() else taskData["channelId"]

#                         message_length = len(message) if message else 0
#                         print(f"Message Length: {message_length}")

#                         if message_length > 1000:
#                             message_chunks = split_message(message)
#                             for chunk in message_chunks:
#                                 await bot_instance.send_message({
#                                     "message": chunk,
#                                     "task_id": task_id,
#                                     "job_id": job_id,
#                                     "server_id": server_id,
#                                     "channel_id": channel_id,
#                                     "type": "final"
#                                 })
#                         else:
#                             await bot_instance.send_message({
#                                 "message": message,
#                                 "task_id": task_id,
#                                 "job_id": job_id,
#                                 "server_id": server_id,
#                                 "channel_id": channel_id,
#                                 "type": "final"
#                             })

#                 else:
#                     print(f"Skipping message send. Missing or empty serverId/channelId for task {task_id}")

#                 # Update MongoDB to remove job_id from active jobs
#                 tasks_collection.update_one(
#                     {"id": task_id},
#                     {
#                         "$pull": {
#                             "activeJobs": {
#                                 "job_id": job_id
#                             }
#                         }
#                     }
#                 )

#                 # Check if task is still active
#                 task = tasks_collection.find_one({"id": task_id})
#                 if task:
#                     status = "awaiting" if len(task.get("activeJobs", [])) == 0 else "scheduled"
#                     tasks_collection.update_one(
#                         {"id": task_id},
#                         {"$set": {"status": status}}
#                     )

#             except json.JSONDecodeError:
#                 print(f"Invalid JSON received from {device_id}: {data}")

#     except WebSocketDisconnect:
#         print(f"Device {device_id} disconnected.")
#         device_collection.update_one(
#             {"deviceId": device_id},
#             {"$set": {"status": False}}
#         )
#         active_connections.remove(websocket)
#         device_connections.pop(device_id, None)




@device_router.websocket("/ws/{device_id}")
async def websocket_endpoint(websocket: WebSocket, device_id: str):
    await websocket.accept()
    device_connections[device_id] = websocket
    active_connections.append(websocket)

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
                print(f"Parsed payload: message={message}, task_id={task_id}, job_id={job_id}")
                
                device_info = device_collection.find_one({"deviceId": device_id})
                device_name = device_info.get("deviceName", device_id) if device_info else device_id
                if message:
                    message = f"Device Name: {device_name}\n\n{message}"
                
                
                if message_type == "ping":
                    pong_response = {
                        "type": "pong",
                        "timestamp": payload.get("timestamp", int(time.time() * 1000))
                    }
                    await websocket.send_text(json.dumps(pong_response))
                    print(f"Sent pong response to ({device_id})")
                    continue
                
                taskData = tasks_collection.find_one(
                    {"id": task_id}, {"serverId": 1, "channelId": 1, "_id": 0}
                )

                if message_type == "update":
                    print(f"Processing 'update' message for task_id {task_id}")
                    if taskData and taskData.get("serverId") and taskData.get("channelId"):
                        server_id = int(taskData["serverId"]) if isinstance(
                            taskData["serverId"], str) and taskData["serverId"].isdigit() else taskData["serverId"]
                        channel_id = int(taskData["channelId"]) if isinstance(
                            taskData["channelId"], str) and taskData["channelId"].isdigit() else taskData["channelId"]

                        await bot_instance.send_message({
                            "message": message,
                            "task_id": task_id,
                            "job_id": job_id,
                            "server_id": server_id,
                            "channel_id": channel_id,
                            "type": "update"
                        })
                    continue

                elif message_type in ["error","final"]:
                    print(f"Processing 'final' message for task_id {task_id}")
                    if taskData and taskData.get("serverId") and taskData.get("channelId"):
                        server_id = int(taskData["serverId"]) if isinstance(
                            taskData["serverId"], str) and taskData["serverId"].isdigit() else taskData["serverId"]
                        channel_id = int(taskData["channelId"]) if isinstance(
                            taskData["channelId"], str) and taskData["channelId"].isdigit() else taskData["channelId"]

                        message_length = len(message) if message else 0
                        print(f"Message Length: {message_length}")

                        if message_length > 1000:
                            message_chunks = split_message(message)
                            for chunk in message_chunks:
                                await bot_instance.send_message({
                                    "message": chunk,
                                    "task_id": task_id,
                                    "job_id": job_id,
                                    "server_id": server_id,
                                    "channel_id": channel_id,
                                    "type": message_type
                                })
                        else:
                            await bot_instance.send_message({
                                "message": message,
                                "task_id": task_id,
                                "job_id": job_id,
                                "server_id": server_id,
                                "channel_id": channel_id,
                                "type": message_type
                            })
                    tasks_collection.update_one(
                    {"id": task_id},
                    {
                        "$pull": {
                            "activeJobs": {
                                "job_id": job_id
                            }
                        }
                    }
                    )

                    # Check if task is still active and update status (only for non-update messages)
                    task = tasks_collection.find_one({"id": task_id})
                    if task:
                        status = "awaiting" if len(task.get("activeJobs", [])) == 0 else "scheduled"
                        tasks_collection.update_one(
                            {"id": task_id},
                            {"$set": {"status": status}}
                        )

                else:
                    print(f"Skipping message send. Missing or empty serverId/channelId for task {task_id}")

            except json.JSONDecodeError:
                print(f"Invalid JSON received from {device_id}: {data}")

    except WebSocketDisconnect:
        print(f"Device {device_id} disconnected.")
        device_collection.update_one(
            {"deviceId": device_id},
            {"$set": {"status": False}}
        )
        active_connections.remove(websocket)
        device_connections.pop(device_id, None)

@device_router.post("/send_command")
async def send_command(request: CommandRequest, current_user: dict = Depends(get_current_user)):
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
        {"id": task_id}, {"$set": {"inputs": newInputs, "deviceIds":device_ids}})
    

    try:

        user_tz = pytz.timezone(time_zone)
        now = datetime.now(user_tz)
        print(f"Current time in {time_zone}: {now}")

        if durationType in ['DurationWithExactStartTime', 'ExactStartTime']:
            time_str = request.command.get("exactStartTime")
            hour, minute = parse_time(time_str)

            target_time = now.replace(
                hour=hour, minute=minute, second=0, microsecond=0)
            end_time_delta = timedelta(minutes=duration)
            target_end_time = target_time + end_time_delta

            if target_time < now:
                target_time += timedelta(days=1)
                target_end_time += timedelta(days=1)

            target_time_utc = target_time.astimezone(pytz.UTC)
            target_end_time_utc = target_end_time.astimezone(pytz.UTC)

            if check_for_job_clashes(target_time_utc, target_end_time_utc, task_id, device_ids):
                return JSONResponse(content={"message": "Task already Scheduled on this time"}, status_code=400)

            job_id = f"cmd_{uuid.uuid4()}"
            command['job_id'] = job_id
            schedule_single_job(
                target_time_utc, target_end_time_utc, device_ids, command, job_id, task_id)

        elif durationType == 'DurationWithTimeWindow':
            start_time_str = command.get("startInput")
            end_time_str = command.get("endInput")

            start_hour, start_minute = parse_time(start_time_str)
            end_hour, end_minute = parse_time(end_time_str)

            start_time = now.replace(
                hour=start_hour, minute=start_minute, second=0, microsecond=0)
            end_time = now.replace(
                hour=end_hour, minute=end_minute, second=0, microsecond=0)

            if end_time < start_time:
                end_time += timedelta(days=1)
            if start_time < now:
                start_time += timedelta(days=1)
                end_time += timedelta(days=1)

            if check_for_job_clashes(start_time, end_time, task_id, device_ids):
                return JSONResponse(content={"message": "Task already Scheduled on this time"}, status_code=400)

            time_window = (end_time - start_time).total_seconds() / 60
            if abs(time_window - duration) <= 10:
                job_id = f"cmd_{uuid.uuid4()}"
                command['job_id'] = job_id
                schedule_single_job(start_time, end_time,
                                    device_ids, command, job_id, task_id)
            else:
                random_durations, start_times = generate_random_durations_and_start_times(
                    duration, start_time, end_time)
                schedule_split_jobs(
                    start_times, random_durations, device_ids, command, task_id)

        elif durationType == 'EveryDayAutomaticRun':
            schedule_recurring_job(command, device_ids)

        return {"message": "Command scheduled successfully"}

    except pytz.exceptions.UnknownTimeZoneError:
        raise HTTPException(
            status_code=400, detail=f"Invalid timezone: {time_zone}")
    except ValueError as e:
        raise HTTPException(
            status_code=400, detail=f"Invalid time format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

# @device_router.post("/stop_task")
# async def stop_task(request: StopTaskCommandRequest, current_user: dict = Depends(get_current_user)):
#     command = request.command
#     task_ids = request.Task_ids

#     print(f"[LOG] Received stop command: {command}")
#     print(f"[LOG] Task IDs: {task_ids}")

#     connected_devices = []
#     not_connected_devices = set()

#     for task_Id in task_ids:
#         print(f"[LOG] Checking task ID: {task_Id}")

#         task = tasks_collection.find_one(
#             {"id": task_Id}
#         )

#         if not task:
#             print(f"[LOG] Task {task_Id} not found in database.")
#             continue  

#         active_jobs = task.get("activeJobs", [])
#         print(f"[LOG] Task {task_Id} has {len(active_jobs)} active jobs.")

#         for job in active_jobs:
#             job_id = job.get("job_id")
#             device_ids = job.get("device_ids", [])

#             if not job_id or not device_ids:
#                 print(f"[LOG] Skipping job {job_id} due to missing fields.")
#                 continue

#             print(f"[LOG] Sending command for job {job_id}")

#             for device_id in device_ids:
#                 websocket = device_connections.get(device_id)
#                 if websocket:
#                     print(f"[LOG] Device {device_id} is connected for job {job_id}.")
#                     connected_devices.append((device_id, websocket))
#                 else:
#                     print(f"[LOG] Device {device_id} is NOT connected for job {job_id}.")
#                     not_connected_devices.add(device_id)

#     print("[LOG] Attempting to send command to connected devices.")
#     for device_id, websocket in connected_devices:
#         try:
#             await websocket.send_text(json.dumps(command))
#             print(f"[LOG] Successfully sent command to device {device_id}")
#         except Exception as e:
#             print(f"[ERROR] Error sending command to device {device_id}: {str(e)}")
#             not_connected_devices.add(device_id)

#     print(f"[LOG] Connected devices: {connected_devices}")
#     print(f"[LOG] Not connected devices: {list(not_connected_devices)}")  # Convert set to list for printing
#     task = tasks_collection.update_one(
#             {"id": task_Id}, 
#             {"$set":{"activeJobs": [], "status":"awaiting"}}
#         )

#     return {"message": "Stop Command Sent successfully"}

@device_router.post("/stop_task")
async def stop_task(request: StopTaskCommandRequest, current_user: dict = Depends(get_current_user)):
    command = request.command
    task_ids = request.Task_ids

    print(f"[LOG] Received stop command: {command}")
    print(f"[LOG] Task IDs: {task_ids}")

    if not task_ids:
        return JSONResponse(status_code=404, content={"message": "No tasks provided"})

    connected_devices = []
    not_connected_devices = set()
    device_names = {}
    task_names = {}
    
    any_device_connected = False

    # First, gather all device information
    for task_id in task_ids:
        print(f"[LOG] Checking task ID: {task_id}")

        task = tasks_collection.find_one({"id": task_id})

        if not task:
            print(f"[LOG] Task {task_id} not found in database.")
            continue
            
        # Store task name for later use (using taskName field from the DB example)
        task_names[task_id] = task.get("taskName", "Unknown Task")

        active_jobs = task.get("activeJobs", [])
        print(f"[LOG] Task {task_id} has {len(active_jobs)} active jobs.")
        
        if not active_jobs:
            print(f"[LOG] No active jobs for task {task_id}")
            continue

        for job in active_jobs:
            job_id = job.get("job_id")
            device_ids = job.get("device_ids", [])

            if not job_id or not device_ids:
                print(f"[LOG] Skipping job {job_id} due to missing fields.")
                continue

            print(f"[LOG] Checking devices for job {job_id}")

            # Get device names
            for device_id in device_ids:
                # Get device name from database
                device = device_collection.find_one({"id": device_id})
                device_name = device.get("deviceName", device_id) if device else device_id
                device_names[device_id] = device_name
                
                websocket = device_connections.get(device_id)
                if websocket:
                    print(f"[LOG] Device {device_id} ({device_name}) is connected for job {job_id}.")
                    connected_devices.append((device_id, websocket))
                    any_device_connected = True
                else:
                    print(f"[LOG] Device {device_id} ({device_name}) is NOT connected for job {job_id}.")
                    not_connected_devices.add(device_id)

    # If no device is connected at all
    if not any_device_connected:
        return JSONResponse(status_code=404, content={"message": "No active devices connected"})

    # Send stop command to connected devices
    print("[LOG] Attempting to send command to connected devices.")
    for device_id, websocket in connected_devices:
        try:
            await websocket.send_text(json.dumps(command))
            print(f"[LOG] Successfully sent command to device {device_id} ({device_names[device_id]})")
        except Exception as e:
            print(f"[ERROR] Error sending command to device {device_id} ({device_names[device_id]}): {str(e)}")
            not_connected_devices.add(device_id)

    # Update tasks to remove active jobs
    for task_id in task_ids:
        tasks_collection.update_one(
            {"id": task_id}, 
            {"$set": {"activeJobs": [], "status": "awaiting"}}
        )

    # # Send notification to Discord
    # try:
    #     for task_id in task_ids:
    #         task = tasks_collection.find_one({"id": task_id})
    #         if not task:
    #             continue
                
    #         # Extract Discord notification details
    #         server_id = task.get("serverId")
    #         channel_id = task.get("channelId")
    #         task_name = task.get("taskName", "Unknown Task")
            
    #         # Prepare message for Discord
    #         message = f"All Jobs of Task {task_name} stopped.\n"
            
    #         # Add connected devices to message
    #         connected_device_names = [device_names.get(device_id, device_id) for device_id, _ in connected_devices if device_id in task.get("deviceIds", [])]
    #         if connected_device_names:
    #             message += f"Connected devices: {', '.join(connected_device_names)}\n"
            
    #         # Add not connected devices to message
    #         not_connected_device_names = [device_names.get(device_id, device_id) for device_id in not_connected_devices if device_id in task.get("deviceIds", [])]
    #         if not_connected_device_names:
    #             message += f"Not connected devices: {', '.join(not_connected_device_names)}"
            
    #         # Send to Discord using the provided method
    #         if server_id and channel_id:
    #             await bot_instance.send_message({
    #                 "message": message,
    #                 "task_id": task_id,
    #                 "server_id": server_id,
    #                 "channel_id": channel_id,
    #                 "type": "update"
    #             })
    # except Exception as e:
    #     print(f"[ERROR] Failed to send Discord notification: {str(e)}")

    return {"message": "Stop Command Sent successfully"}
    
async def send_command_to_devices(device_ids, command):
    print(f"Executing command for devices: {device_ids}, command: {command}")

    task_id = command.get("task_id")
    job_id = command.get("job_id")
    is_recurring = command.get("isRecurring", False)
    
    # Get task information with only needed fields
    task = tasks_collection.find_one(
        {"id": task_id}, 
        {"serverId": 1, "channelId": 1, "_id": 0}
    )
    
    if not task:
        print(f"Task {task_id} not found")
        return
        
    # Early validation of server and channel IDs
    if not task.get("serverId") or not task.get("channelId"):
        print(f"Skipping error message. Missing serverId/channelId for task {task_id}")
        return
        
    # Parse server and channel IDs once
    server_id = int(task["serverId"]) if isinstance(
        task["serverId"], str) and task["serverId"].isdigit() else task["serverId"]
    channel_id = int(task["channelId"]) if isinstance(
        task["channelId"], str) and task["channelId"].isdigit() else task["channelId"]
    
    # Separate connected and disconnected devices
    connected_devices = []
    not_connected_devices = []
    
    for device_id in device_ids:
        websocket = device_connections.get(device_id)
        if websocket:
            connected_devices.append((device_id, websocket))
        else:
            not_connected_devices.append(device_id)
    
    # Fetch device names for all devices at once
    all_device_ids = device_ids.copy()
    device_name_map = {}
    
    if all_device_ids:
        device_docs = list(device_collection.find(
            {"deviceId": {"$in": all_device_ids}}, 
            {"deviceId": 1, "deviceName": 1, "_id": 0}
        ))
        
        for doc in device_docs:
            device_id = doc.get("deviceId")
            device_name = doc.get("deviceName", "Unknown Device")
            device_name_map[device_id] = device_name
    
    # Handle disconnected devices
    if not_connected_devices:
        disconnected_names = [device_name_map.get(d_id, "Unknown Device") for d_id in not_connected_devices]
        error_message = f"Error: The following devices are not connected: {', '.join(disconnected_names)}"
        
        await bot_instance.send_message({
            "message": error_message,
            "task_id": task_id,
            "job_id": job_id,
            "server_id": server_id,
            "channel_id": channel_id,
            "type": "error"
        })

    # Send commands to connected devices
    task_status_updated = False
    failed_devices = []
    
    for device_id, websocket in connected_devices:
        try:
            await websocket.send_text(json.dumps(command))
            
            # Update task status after first successful command
            if not task_status_updated:
                tasks_collection.update_one(
                    {"id": task_id},
                    {"$set": {"status": "running"}}
                )
                task_status_updated = True
                
        except Exception as e:
            print(f"Error sending command to device {device_id}: {str(e)}")
            not_connected_devices.append(device_id)
            failed_devices.append(device_id)
    
    # Handle devices that failed during command sending
    if failed_devices:
        failed_names = [device_name_map.get(d_id, "Unknown Device") for d_id in failed_devices]
        error_msg = f"Error sending command to the following devices: {', '.join(failed_names)}"
        
        await bot_instance.send_message({
            "message": error_msg,
            "task_id": task_id,
            "job_id": job_id,
            "server_id": server_id,
            "channel_id": channel_id,
            "type": "error"
        })

    # Update database for disconnected devices in a single operation
    if not_connected_devices:
        tasks_collection.update_one(
            {"id": task_id, "activeJobs.job_id": job_id},
            {"$pull": {"activeJobs.$.device_ids": {"$in": not_connected_devices}}}
        )

    # If all devices are disconnected
    if len(not_connected_devices) == len(device_ids):
        # Remove job from activeJobs
        tasks_collection.update_one(
            {"id": task_id},
            {"$pull": {"activeJobs": {"job_id": job_id}}}
        )
        
        # Send "all disconnected" message if no devices were connected
        if not connected_devices:
            device_name_list = [device_name_map.get(d_id, "Unknown Device") for d_id in device_ids]
            all_disconnected_msg = f"Task cannot be executed. All target devices are disconnected: {', '.join(device_name_list)}"
            
            await bot_instance.send_message({
                "message": all_disconnected_msg,
                "task_id": task_id,
                "job_id": job_id,
                "server_id": server_id,
                "channel_id": channel_id,
                "type": "error"
            })
        
        print(f"Job {job_id} is no longer active as no devices are connected.")

    print(f"Connected devices: {len(connected_devices)}, Not connected devices: {len(not_connected_devices)}")
    
    # Handle recurring schedule if needed
    if is_recurring:
        schedule_recurring_job(command, device_ids)

def parse_time(time_str: str) -> tuple:
    """Parse time string in 'HH:MM' format to a tuple of integers (hour, minute)."""
    hour, minute = map(int, time_str.split(':'))
    return hour, minute

def check_for_job_clashes(start_time, end_time, task_id, device_ids) -> bool:
    """Check for job clashes in the scheduled time window."""
    return check_for_Job_clashes(start_time, end_time, task_id, device_ids)

# def schedule_single_job(start_time, end_time, device_ids, command, job_id: str, task_id: str) -> None:
#     """Schedule a single job with a defined start and end time."""
#     jobInstance = {
#         "job_id": job_id,
#         "startTime": start_time,
#         "endTime": end_time,
#         "device_ids": device_ids
#     }

#     try:
#         scheduler.add_job(
#             send_command_to_devices,
#             trigger=DateTrigger(
#                 run_date=start_time.astimezone(pytz.UTC),
#                 timezone=pytz.UTC
#             ),
#             args=[device_ids, {**command,
#                                "duration": int(command.get("duration", 0))}],
#             id=job_id,
#             name=f"Single session command for devices {device_ids}"
#         )

#         tasks_collection.update_one(
#             {"id": task_id},
#             {"$set": {"status": "scheduled"},
#              "$push": {"activeJobs": jobInstance}}
#         )
#     except Exception as e:
#         print(f"Failed to schedule single job: {str(e)}")
#         raise HTTPException(
#             status_code=500, detail=f"Failed to schedule job: {str(e)}")


def schedule_single_job(start_time, end_time, device_ids, command, job_id: str, task_id: str) -> None:
    """Schedule a single job with a defined start and end time."""
    jobInstance = {
        "job_id": job_id,
        "startTime": start_time,
        "endTime": end_time,
        "device_ids": device_ids
    }

    try:
        scheduler.add_job(
            send_command_to_devices,
            trigger=DateTrigger(
                run_date=start_time.astimezone(pytz.UTC),
                timezone=pytz.UTC
            ),
            args=[device_ids, {**command,
                               "duration": int(command.get("duration", 0))}],
            id=job_id,
            name=f"Single session command for devices {device_ids}"
        )

        tasks_collection.update_one(
            {"id": task_id},
            {"$set": {"status": "scheduled"},
             "$push": {"activeJobs": jobInstance}}
        )
        
        # Get device names for notification
        device_docs = list(device_collection.find(
            {"deviceId": {"$in": device_ids}}, 
            {"deviceId": 1, "deviceName": 1, "_id": 0}
        ))
        device_names = [doc.get("deviceName", "Unknown Device") for doc in device_docs]
        
        # Get task details for notification
        task = tasks_collection.find_one({"id": task_id})
        time_zone = command.get("timeZone", "UTC")
        
        # Send schedule notification asynchronously
        asyncio.create_task(
            send_schedule_notification(task, device_names, start_time, end_time, time_zone, job_id)
        )
        
    except Exception as e:
        print(f"Failed to schedule single job: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to schedule job: {str(e)}")

def generate_random_durations_and_start_times(duration: int, start_time: datetime, end_time: datetime) -> tuple:
    """Generate random durations and start times for split jobs."""
    random_durations = generate_random_durations(duration)
    start_times = get_random_start_times(
        start_time, end_time, random_durations)
    return random_durations, start_times

# def schedule_split_jobs(start_times: List[datetime], random_durations: List[int], device_ids: List[str], command: dict, task_id: str) -> None:
#     """Schedule multiple jobs based on random start times and durations."""
#     for i, (start_time, duration) in enumerate(zip(start_times, random_durations)):
#         job_id = f"cmd_{uuid.uuid4()}"
#         end_time = start_time + timedelta(minutes=duration)
#         jobInstance = {
#             "job_id": job_id,
#             "startTime": start_time,
#             "endTime": end_time,
#             "device_ids": device_ids
#         }

#         modified_command = {**command, "duration": duration}
#         try:
#             scheduler.add_job(
#                 send_command_to_devices,
#                 trigger=DateTrigger(
#                     run_date=start_time.astimezone(pytz.UTC),
#                     timezone=pytz.UTC
#                 ),
#                 args=[device_ids, modified_command],
#                 id=job_id,
#                 name=f"Part {i+1} of split command for devices {device_ids}"
#             )

#             tasks_collection.update_one(
#                 {"id": task_id},
#                 {"$set": {"isScheduled": True},
#                  "$push": {"activeJobs": jobInstance}}
#             )
#         except Exception as e:
#             print(f"Failed to schedule split job {i+1}: {str(e)}")
#             raise HTTPException(
#                 status_code=500, detail=f"Failed to schedule job: {str(e)}")



def schedule_split_jobs(start_times: List[datetime], random_durations: List[int], device_ids: List[str], command: dict, task_id: str) -> None:
    """Schedule multiple jobs based on random start times and durations."""
    job_instances = []
    scheduled_jobs = []
    
    for i, (start_time, duration) in enumerate(zip(start_times, random_durations)):
        job_id = f"cmd_{uuid.uuid4()}"
        end_time = start_time + timedelta(minutes=duration)
        jobInstance = {
            "job_id": job_id,
            "startTime": start_time,
            "endTime": end_time,
            "device_ids": device_ids
        }
        job_instances.append(jobInstance)

        modified_command = {**command, "duration": duration, "job_id": job_id}
        try:
            scheduler.add_job(
                send_command_to_devices,
                trigger=DateTrigger(
                    run_date=start_time.astimezone(pytz.UTC),
                    timezone=pytz.UTC
                ),
                args=[device_ids, modified_command],
                id=job_id,
                name=f"Part {i+1} of split command for devices {device_ids}"
            )
            scheduled_jobs.append(job_id)
            
        except Exception as e:
            print(f"Failed to schedule split job {i+1}: {str(e)}")
            # Remove previously scheduled jobs if any fail
            for scheduled_job_id in scheduled_jobs:
                try:
                    scheduler.remove_job(scheduled_job_id)
                except:
                    pass
            raise HTTPException(
                status_code=500, detail=f"Failed to schedule job: {str(e)}")
    
    # Only update database if all jobs were scheduled successfully
    if scheduled_jobs:
        tasks_collection.update_one(
            {"id": task_id},
            {"$set": {"status": "scheduled"},
             "$push": {"activeJobs": {"$each": job_instances}}}
        )
        
        # Get device names for notification
        device_docs = list(device_collection.find(
            {"deviceId": {"$in": device_ids}}, 
            {"deviceId": 1, "deviceName": 1, "_id": 0}
        ))
        device_names = [doc.get("deviceName", "Unknown Device") for doc in device_docs]
        
        # Get task details for notification
        task = tasks_collection.find_one({"id": task_id})
        time_zone = command.get("timeZone", "UTC")
        
        # Send split schedule notification asynchronously
        asyncio.create_task(
            send_split_schedule_notification(task, device_names, start_times, random_durations, time_zone)
        )




# def schedule_recurring_job(command: dict, device_ids: List[str]) -> None:
#     """Schedule the next day's task within the specified time window"""
#     task_id = command.get("task_id")
#     time_zone = command.get("timeZone", "UTC")
#     user_tz = pytz.timezone(time_zone)

#     # Get tomorrow's date
#     now = datetime.now(user_tz)

#     # Parse start and end times
#     start_time_str = command.get("startInput")
#     end_time_str = command.get("endInput")
#     start_hour, start_minute = parse_time(start_time_str)
#     end_hour, end_minute = parse_time(end_time_str)

#     start_time = now.replace(
#         hour=start_hour,
#         minute=start_minute,
#         second=0,
#         microsecond=0
#     )
#     end_time = now.replace(
#         hour=end_hour,
#         minute=end_minute,
#         second=0,
#         microsecond=0
#     )

#     if start_time < now:
#         start_time += timedelta(days=1)

#     if end_time < start_time:
#         end_time += timedelta(days=1)

#     time_window_minutes = int((end_time - start_time).total_seconds() / 60)
#     random_minutes = random.randint(
#         0, time_window_minutes - int(command.get("duration", 0)))
#     random_start_time = start_time + timedelta(minutes=random_minutes)

#     start_time_utc = random_start_time.astimezone(pytz.UTC)

#     new_job_id = f"cmd_{uuid.uuid4()}"
#     modified_command = {
#         **command,
#         "job_id": new_job_id,
#         "isRecurring": True
#     }

#     jobInstance = {
#         "job_id": new_job_id,
#         "startTime": start_time_utc,
#         "device_ids": device_ids
#     }

#     try:
#         scheduler.add_job(
#             send_command_to_devices,
#             trigger=DateTrigger(
#                 run_date=start_time_utc,
#                 timezone=pytz.UTC
#             ),
#             args=[device_ids, modified_command],
#             id=new_job_id,
#             name=f"Recurring random-time command for devices {device_ids}"
#         )

#         tasks_collection.update_one(
#             {"id": task_id},
#             {
#                 "$set": {"status": "scheduled"},
#                 "$push": {"activeJobs": jobInstance}
#             }
#         )

#         print(f"Scheduled next day's task for {random_start_time} ({time_zone})")

#     except Exception as e:
#         print(f"Failed to schedule next day's job: {str(e)}")
#         raise HTTPException(
#             status_code=500, detail=f"Failed to schedule next day's job: {str(e)}")


# def schedule_recurring_job(command: dict, device_ids: List[str]) -> None:
#     """Schedule the next day's task within the specified time window"""
#     task_id = command.get("task_id")
#     time_zone = command.get("timeZone", "UTC")
#     user_tz = pytz.timezone(time_zone)

#     # Get tomorrow's date
#     now = datetime.now(user_tz)

#     # Parse start and end times
#     start_time_str = command.get("startInput")
#     end_time_str = command.get("endInput")
#     start_hour, start_minute = parse_time(start_time_str)
#     end_hour, end_minute = parse_time(end_time_str)

#     start_time = now.replace(
#         hour=start_hour,
#         minute=start_minute,
#         second=0,
#         microsecond=0
#     )
#     end_time = now.replace(
#         hour=end_hour,
#         minute=end_minute,
#         second=0,
#         microsecond=0
#     )

#     if start_time < now:
#         start_time += timedelta(days=1)

#     if end_time < start_time:
#         end_time += timedelta(days=1)

#     time_window_minutes = int((end_time - start_time).total_seconds() / 60)
#     duration = int(command.get("duration", 0))
    
#     # Ensure we don't schedule if the duration exceeds available time
#     if duration > time_window_minutes:
#         print(f"Duration ({duration}) exceeds available time window ({time_window_minutes})")
#         return
        
#     random_minutes = random.randint(0, time_window_minutes - duration)
#     random_start_time = start_time + timedelta(minutes=random_minutes)
#     random_end_time = random_start_time + timedelta(minutes=duration)

#     start_time_utc = random_start_time.astimezone(pytz.UTC)
#     end_time_utc = random_end_time.astimezone(pytz.UTC)

#     new_job_id = f"cmd_{uuid.uuid4()}"
#     modified_command = {
#         **command,
#         "job_id": new_job_id,
#         "isRecurring": True
#     }

#     jobInstance = {
#         "job_id": new_job_id,
#         "startTime": start_time_utc,
#         "endTime": end_time_utc,
#         "device_ids": device_ids
#     }

#     try:
#         scheduler.add_job(
#             send_command_to_devices,
#             trigger=DateTrigger(
#                 run_date=start_time_utc,
#                 timezone=pytz.UTC
#             ),
#             args=[device_ids, modified_command],
#             id=new_job_id,
#             name=f"Recurring random-time command for devices {device_ids}"
#         )

#         tasks_collection.update_one(
#             {"id": task_id},
#             {
#                 "$set": {"status": "scheduled"},
#                 "$push": {"activeJobs": jobInstance}
#             }
#         )

#         # Get device names for notification
#         device_docs = list(device_collection.find(
#             {"deviceId": {"$in": device_ids}}, 
#             {"deviceId": 1, "deviceName": 1, "_id": 0}
#         ))
#         device_names = [doc.get("deviceName", "Unknown Device") for doc in device_docs]
        
#         # Get task details for notification
#         task = tasks_collection.find_one({"id": task_id})
        
#         # Send schedule notification asynchronously
#         asyncio.create_task(
#             send_schedule_notification(task, device_names, random_start_time, random_end_time, time_zone, new_job_id)
#         )

#         print(f"Scheduled next day's task for {random_start_time} ({time_zone})")

#     except Exception as e:
#         print(f"Failed to schedule next day's job: {str(e)}")
#         raise HTTPException(
#             status_code=500, detail=f"Failed to schedule next day's job: {str(e)}")



def schedule_recurring_job(command: dict, device_ids: List[str]) -> None:
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
        hour=start_hour,
        minute=start_minute,
        second=0,
        microsecond=0
    )
    end_time = now.replace(
        hour=end_hour,
        minute=end_minute,
        second=0,
        microsecond=0
    )

    if start_time < now:
        start_time += timedelta(days=1)

    if end_time < start_time:
        end_time += timedelta(days=1)

    time_window_minutes = int((end_time - start_time).total_seconds() / 60)
    duration = int(command.get("duration", 0))
    
    # Ensure we don't schedule if the duration exceeds available time
    if duration > time_window_minutes:
        print(f"Duration ({duration}) exceeds available time window ({time_window_minutes})")
        return
        
    random_minutes = random.randint(0, time_window_minutes - duration)
    random_start_time = start_time + timedelta(minutes=random_minutes)
    random_end_time = random_start_time + timedelta(minutes=duration)

    start_time_utc = random_start_time.astimezone(pytz.UTC)
    end_time_utc = random_end_time.astimezone(pytz.UTC)

    new_job_id = f"cmd_{uuid.uuid4()}"
    modified_command = {
        **command,
        "job_id": new_job_id,
        "isRecurring": True
    }

    jobInstance = {
        "job_id": new_job_id,
        "startTime": start_time_utc,
        "endTime": end_time_utc,
        "device_ids": device_ids
    }

    try:
        scheduler.add_job(
            send_command_to_devices,
            trigger=DateTrigger(
                run_date=start_time_utc,
                timezone=pytz.UTC
            ),
            args=[device_ids, modified_command],
            id=new_job_id,
            name=f"Recurring random-time command for devices {device_ids}"
        )

        # First, get current task status
        task = tasks_collection.find_one({"id": task_id}, {"status": 1})
        
        if task and task.get("status") == "awaiting":
            # Update both status and activeJobs if status is "awaiting"
            update_operation = {
                "$set": {"status": "scheduled"},
                "$push": {"activeJobs": jobInstance}
            }
        else:
            # Only update activeJobs if status is not "awaiting"
            update_operation = {
                "$push": {"activeJobs": jobInstance}
            }
            
        # Update the task in the database
        tasks_collection.update_one(
            {"id": task_id},
            update_operation
        )

        # Get device names for notification
        device_docs = list(device_collection.find(
            {"deviceId": {"$in": device_ids}}, 
            {"deviceId": 1, "deviceName": 1, "_id": 0}
        ))
        device_names = [doc.get("deviceName", "Unknown Device") for doc in device_docs]
        
        # Get updated task details for notification
        task = tasks_collection.find_one({"id": task_id})
        
        # Send schedule notification asynchronously
        asyncio.create_task(
            send_schedule_notification(task, device_names, random_start_time, random_end_time, time_zone, new_job_id)
        )

        print(f"Scheduled next day's task for {random_start_time} ({time_zone})")

    except Exception as e:
        print(f"Failed to schedule next day's job: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to schedule next day's job: {str(e)}")




def generate_random_durations(total_duration: int, min_duration: int = 30) -> List[int]:
    """
    Generate 2 to 4 random durations that sum up to the total duration.
    Each duration will be at least min_duration minutes.
    """
    num_durations = random.randint(
        2, 4)  # Limit the number of partitions to 2, 3, or 4

    if total_duration <= min_duration:
        return [total_duration]

    durations = []
    remaining = total_duration

    for _ in range(num_durations - 1):
        max_possible = min(remaining - min_duration,
                           remaining // (num_durations - len(durations)))
        if max_possible <= min_duration:
            break
        duration = random.randint(min_duration, max_possible)
        durations.append(duration)
        remaining -= duration

    durations.append(remaining)  # Add the remaining time to the last partition

    return durations

def get_random_start_times(
    start_time: datetime,
    end_time: datetime,
    durations: List[int],
    min_gap: float = 1.5
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
    available_time = (end_time - start_time).total_seconds() / \
        60  # Convert to minutes

    # Check if there is enough time in the window
    if total_time_needed > available_time:
        raise ValueError(
            "Not enough time in the window for all sessions with minimum gaps")

    start_times = []
    current_time = start_time

    # Calculate maximum gap possible
    remaining_gaps = len(durations) - 1
    for i, duration in enumerate(durations):
        start_times.append(current_time)

        if remaining_gaps > 0:
            # Calculate the remaining time and the maximum possible gap
            time_left = (end_time - current_time).total_seconds() / 60
            time_needed = sum(durations[i+1:]) + remaining_gaps * min_gap
            max_gap = (time_left - time_needed) / remaining_gaps

            # Add a random gap after the session, but ensure it's at least min_gap
            gap = random.uniform(
                min_gap, max_gap) if max_gap > min_gap else min_gap
            current_time += timedelta(minutes=duration + gap)
            remaining_gaps -= 1
        else:
            # No more gaps to add, just adjust the current time
            current_time += timedelta(minutes=duration)

    return start_times



async def send_schedule_notification(task, device_names, start_time, end_time, time_zone, job_id):
    """Send a notification to Discord about a scheduled task."""
    if not task or not task.get("serverId") or not task.get("channelId"):
        print("Skipping notification. Missing serverId/channelId for task")
        return
    
    try:
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
        
        server_id = int(task["serverId"]) if isinstance(
            task["serverId"], str) and task["serverId"].isdigit() else task["serverId"]
        channel_id = int(task["channelId"]) if isinstance(
            task["channelId"], str) and task["channelId"].isdigit() else task["channelId"]
        
        # Send message to Discord
        await bot_instance.send_message({
            "message": message,
            "task_id": task.get("id"),
            "job_id": job_id,
            "server_id": server_id,
            "channel_id": channel_id,
            "type": "info"
        })
    except Exception as e:
        print(f"Error sending schedule notification: {str(e)}")
        
        

async def send_split_schedule_notification(task, device_names, start_times, durations, time_zone):
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
        timespan_end = (max(start_times) + timedelta(minutes=durations[-1])).strftime("%Y-%m-%d %H:%M")
        
        message = (
            f"📅 **Split Task Scheduled**: {task_name}\n"
            f"⏰ **Timespan**: {timespan_start} to {timespan_end} ({time_zone})\n"
            f"🔢 **Sessions**: {session_count} sessions (total {total_duration} minutes)\n"
            f"🔌 **Devices**: {device_list}"
        )
        
        server_id = int(task["serverId"]) if isinstance(
            task["serverId"], str) and task["serverId"].isdigit() else task["serverId"]
        channel_id = int(task["channelId"]) if isinstance(
            task["channelId"], str) and task["channelId"].isdigit() else task["channelId"]
        
        # Send message to Discord
        await bot_instance.send_message({
            "message": message,
            "task_id": task.get("id"),
            "job_id": f"split_{uuid.uuid4()}", # Generate a unique ID for this notification
            "server_id": server_id,
            "channel_id": channel_id,
            "type": "info"
        })
    except Exception as e:
        print(f"Error sending split schedule notification: {str(e)}")