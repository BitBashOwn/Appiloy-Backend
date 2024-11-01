from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Depends
from pymongo import MongoClient
from pydantic import BaseModel
from typing import List
from datetime import datetime, timedelta
from utils.utils import get_current_user
from models.tasks import tasks_collection
# from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
import json


# MongoDB Connection
client = MongoClient("mongodb+srv://abdullahnoor94:dodge2018@appilot.ds9ll.mongodb.net/?retryWrites=true&w=majority&appName=Appilot")
db = client["Appilot"]
device_collection = db["devices"]

# Create Router
device_router = APIRouter()
scheduler = AsyncIOScheduler()
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
    
# class CommandRequest(BaseModel):
#     command: dict
#     device_ids: List[str]
#     task: dict

# Dependency for registering and validating the device
def register_device(device_data: DeviceRegistration):
    device = device_collection.find_one({"deviceId": device_data.deviceId})
    if device:
        raise HTTPException(status_code=400, detail="Device already registered")
    result = device_collection.insert_one(device_data.dict())
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
    device_collection.update_one({"deviceId": device_id}, {"$set": {"status": status}})
    return {"message": f"Device {device_id} status updated to {status}"}

# Endpoint for checking device registration
@device_router.get("/device_registration/{device_id}")
async def check_device_registration(device_id: str):
    device = device_collection.find_one({"deviceId": device_id})
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    
    if not device["status"]:
        device_collection.update_one({"deviceId": device_id}, {"$set": {"status": True}})
    
    return True

# # WebSocket endpoint
# @device_router.websocket("/ws/{device_id}")
# async def websocket_endpoint(websocket: WebSocket, device_id: str):
#     await websocket.accept()
#     device_connections[device_id] = websocket
#     active_connections.append(websocket)

#     try:
#         while True:
#             data = await websocket.receive_text()
#             print(f"Message from {device_id}: {data}")
            
#             for connection in active_connections:
#                 if connection != websocket:
#                     await connection.send_text(f"Message from {device_id}: {data}")
#     except WebSocketDisconnect:
#         print(f"Device {device_id} disconnected.")
#         active_connections.remove(websocket)
#         device_connections.pop(device_id, None)
# WebSocket endpoint
@device_router.websocket("/ws/{device_id}")
async def websocket_endpoint(websocket: WebSocket, device_id: str):
    await websocket.accept()
    device_connections[device_id] = websocket
    active_connections.append(websocket)
    
    try:
        # Update device status to true when connected
        # device_collection.update_one({"deviceId": device_id}, {"$set": {"status": True}})
        
        while True:
            data = await websocket.receive_text()
            print(f"Message from {device_id}: {data}")
            for connection in active_connections:
                if connection != websocket:
                    await connection.send_text(f"Message from {device_id}: {data}")
                    
    except WebSocketDisconnect:
        print(f"Device {device_id} disconnected.")
        # Update device status to false when disconnected
        device_collection.update_one({"deviceId": device_id}, {"$set": {"status": False}})
        active_connections.remove(websocket)
        device_connections.pop(device_id, None)

# Command Endpoint
# @device_router.post("/send_command")
# async def send_command(request: CommandRequest):
#     print(request)
#     device_ids = request.device_ids
#     command = request.command
#     not_connected_devices = []

#     for device_id in device_ids:
#         websocket = device_connections.get(device_id)
#         if websocket:
#             await websocket.send_text(f"{command}")
#         else:
#             not_connected_devices.append(device_id)

#     if not_connected_devices:
#         print("error")
#         result = list(db["devices"].find(
#             {"deviceId": {"$in": not_connected_devices}}, {"deviceName": 1, "_id": 0}))
#         print(result)
#         raise HTTPException(status_code=404, detail={
#             "error": f"Devices with IDs {not_connected_devices} are not connected.",
#             "devices": result
#         })

#     return {"message": f"Command '{command}' sent to devices {device_ids}"}

# @device_router.post("/send_command")
# async def send_command(request: CommandRequest, current_user: dict = Depends(get_current_user)):
#     device_ids = request.device_ids
#     command = request.command
#     task = request.task
#     not_connected_devices = []

#     result = tasks_collection.update_one(
#         {"id": task["id"], "email": current_user.get("email")},
#         {"$set": {"inputs": task["inputs"], "deviceIds": task["deviceIds"], "schedules": task["schedules"],
#                   "LastModifiedDate": datetime.utcnow().timestamp(), "status": "active"}}
#     )
#     if result.modified_count == 0:
#         raise HTTPException(
#             status_code=404, detail="Task not found or update failed.")

#     for device_id in device_ids:
#         websocket = device_connections.get(device_id)
#         if websocket:
#             await websocket.send_text(f"{command}")
#         else:
#             not_connected_devices.append(device_id)

#     if not_connected_devices:
#         raise HTTPException(
#             status_code=404,
#             detail=f"Devices with IDs {not_connected_devices} are not connected."
#         )

#     # Return a success message with the command and the list of device IDs
#     return {"message": f"Command '{command}' sent to devices {device_ids} and task saved to db"}





async def send_command_to_devices(device_ids, command):
    print(f"Executing command for devices: {device_ids}, command: {command}")
    for device_id in device_ids:
        websocket = device_connections.get(device_id)
        if websocket:
            await websocket.send_text(json.dumps(command))  # Send command as JSON

@device_router.post("/send_command")
async def send_command(request: CommandRequest):
    command = request.command
    device_ids = request.device_ids
    durationType = request.command.get("durationtype")
    time_str = request.command.get("time")
    
    scheduled_jobs = []  # To keep track of scheduled jobs

    if durationType == 'Exact Start Time':
            # Parse the time string (e.g., "14:30")
            target_time = datetime.strptime(time_str, "%H:%M").replace(
                year=datetime.now().year,
                month=datetime.now().month,
                day=datetime.now().day
            )

            # Adjust target time if it's already passed today
            if target_time < datetime.now():
                target_time += timedelta(days=1)  # Schedule for the next day

            try:
                # Schedule the command to run at the specified time
                job = scheduler.add_job(send_command_to_devices, 'date', run_date=target_time, args=[device_ids, command])
                scheduled_jobs.append(job)
                print(f"Scheduled job: {job.id} at {target_time}")
            except Exception as e:
                print(f"Failed to schedule job: {e}")

    elif durationType == 'Randomized Start Time within a Window':
        start_time_str, end_time_str = request.schedule_times[0].split('-')
        start_time = datetime.strptime(start_time_str, "%H:%M").replace(
            year=datetime.now().year,
            month=datetime.now().month,
            day=datetime.now().day
        )
        end_time = datetime.strptime(end_time_str, "%H:%M").replace(
            year=datetime.now().year,
            month=datetime.now().month,
            day=datetime.now().day
        )
        
        # Adjust start and end time if necessary
        if start_time < datetime.now():
            start_time += timedelta(days=1)
            end_time += timedelta(days=1)

        # Calculate the interval in minutes
        interval_minutes = request.duration_minutes
        duration = (end_time - start_time).seconds // 60  # Total duration in minutes

        # Schedule commands in intervals
        current_time = start_time
        while current_time < end_time:
            try:
                job = scheduler.add_job(send_command_to_devices, 'date', run_date=current_time, args=[device_ids, command])
                scheduled_jobs.append(job)
                print(f"Scheduled job: {job.id} at {current_time}")
            except Exception as e:
                print(f"Failed to schedule job: {e}")
            current_time += timedelta(minutes=interval_minutes)

    return {
        "message": f"Command '{command}' scheduled for devices {device_ids}",
        "scheduled_jobs": [job.id for job in scheduled_jobs]  # List of scheduled job IDs
    }

# @device_router.post("/send_command")
# async def send_command(request: CommandRequest):
#     command = request.command
#     device_ids = request.device_ids
#     durationType = request.command.get("durationtype")
#     time_str = request.command.get("time")
#     time_zone = request.command.get("timeZone")

#     user_tz = pytz.timezone(time_zone)
#     now = datetime.now(user_tz)
#     print(f"Current time in user timezone: {now}")

#     scheduled_jobs = []

#     if durationType == 'Exact Start Time':
#         target_time = user_tz.localize(datetime.strptime(time_str, "%H:%M")).replace(
#             year=now.year,
#             month=now.month,
#             day=now.day
#         )

#         if target_time < now:
#             target_time += timedelta(days=1)  # Schedule for the next day

#         target_time_utc = target_time.astimezone(pytz.utc)
#         print(f"Scheduling for Exact Start Time at {target_time_utc}")

#         try:
#             job = scheduler.add_job(send_command_to_devices, 'date', run_date=target_time_utc, args=[device_ids, command])
#             scheduled_jobs.append(job)
#             print(f"Scheduled job: {job.id} at {target_time_utc}")
#         except Exception as e:
#             print(f"Failed to schedule job: {e}")

#     elif durationType == 'Randomized Start Time within a Window':
#         # Similar logging and error handling for randomized scheduling...
#         pass

#     return {
#         "message": f"Command '{command}' scheduled for devices {device_ids}",
#         "scheduled_jobs": [job.id for job in scheduled_jobs]  # List of scheduled job IDs
#     }
