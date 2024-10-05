from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException
from pymongo import MongoClient
from pydantic import BaseModel
from typing import List
# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["appilot"]
device_collection = db["devices"]
# FastAPI instance
app = FastAPI()
# Device registration model
class DeviceRegistration(BaseModel):
    device_name: str
    model_number: str
    device_id: str
    activation_date: str
# In-memory storage for WebSocket connections
active_connections: List[WebSocket] = []
# Dependency for registering and validating the device
def register_device(device_data: DeviceRegistration):
    device = device_collection.find_one({"device_id": device_data.device_id})
    if device:
        raise HTTPException(status_code=400, detail="Device already registered")
    # Insert the new device into the MongoDB collection
    result = device_collection.insert_one(device_data.dict())
    return {"message": "Device registered successfully", "device_id": str(result.inserted_id)}
# Device Registration Endpoint (POST)
@app.post("/register_device")
async def register_device_endpoint(device_data: DeviceRegistration):
    return register_device(device_data)
# WebSocket endpoint
@app.websocket("/ws/{device_id}")
async def websocket_endpoint(websocket: WebSocket, device_id: str):
    # Accept the WebSocket connection
    await websocket.accept()
    # Add the WebSocket to the list of active connections
    active_connections.append(websocket)
    try:
        while True:
            # Await incoming messages from the WebSocket
            data = await websocket.receive_text()
            print(f"Message from {device_id}: {data}")
            # Broadcast the message to all active WebSocket connections (if needed)
            for connection in active_connections:
                if connection != websocket:
                    await connection.send_text(f"Message from {device_id}: {data}")
    except WebSocketDisconnect:
        print(f"Device {device_id} disconnected.")
        # Remove the WebSocket from active connections when disconnected
        active_connections.remove(websocket)