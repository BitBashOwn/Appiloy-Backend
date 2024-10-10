from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Depends
from pymongo import MongoClient
from pydantic import BaseModel
from typing import List
from datetime import datetime
from utils.utils import get_current_user
from models.tasks import tasks_collection


# MongoDB Connection
client = MongoClient("mongodb+srv://abdullahnoor94:dodge2018@appilot.ds9ll.mongodb.net/?retryWrites=true&w=majority&appName=Appilot")
db = client["Appilot"]
device_collection = db["devices"]

# Create Router
device_router = APIRouter()

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
@device_router.post("/send_command")
async def send_command(request: CommandRequest):
    device_ids = request.device_ids
    command = request.command
    not_connected_devices = []

    for device_id in device_ids:
        websocket = device_connections.get(device_id)
        if websocket:
            await websocket.send_text(f"{command}")
        else:
            not_connected_devices.append(device_id)
    
    if not_connected_devices:
        raise HTTPException(status_code=404, detail=f"Devices with IDs {not_connected_devices} are not connected.")
    
    return {"message": f"Command '{command}' sent to devices {device_ids}"}

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
