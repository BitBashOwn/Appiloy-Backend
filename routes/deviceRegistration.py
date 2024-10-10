from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException
from pymongo import MongoClient
from pydantic import BaseModel
from typing import List

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
    app_name: str
    device_id: str

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

# WebSocket endpoint
@device_router.websocket("/ws/{device_id}")
async def websocket_endpoint(websocket: WebSocket, device_id: str):
    await websocket.accept()
    device_connections[device_id] = websocket
    active_connections.append(websocket)

    try:
        while True:
            data = await websocket.receive_text()
            print(f"Message from {device_id}: {data}")
            
            for connection in active_connections:
                if connection != websocket:
                    await connection.send_text(f"Message from {device_id}: {data}")
    except WebSocketDisconnect:
        print(f"Device {device_id} disconnected.")
        active_connections.remove(websocket)
        device_connections.pop(device_id, None)

# Command Endpoint
@device_router.post("/send_command")
async def send_command(request: CommandRequest):
    device_id = request.device_id
    app_name = request.app_name

    websocket = device_connections.get(device_id)
    if websocket:
        await websocket.send_text(f"{app_name}")
        return {"message": f"Command '{app_name}' sent to device {device_id}"}
    else:
        raise HTTPException(status_code=404, detail=f"Device with ID {device_id} is not connected.")

