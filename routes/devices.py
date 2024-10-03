from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from utils.utils import get_current_user
from models.devices import devices_collection
from pydantic import BaseModel
from bson import ObjectId
import json


devices_router = APIRouter()


class deleteRequest(BaseModel):
    devices: list


@devices_router.get("/devices")
async def get_devices(current_user: dict = Depends(get_current_user)):
    data = list(devices_collection.find())

    for device in data:
        if '_id' in device:
            device['_id'] = str(device['_id'])

    devices = json.dumps(data)
    return JSONResponse(content={"devices": devices}, status_code=200)


@devices_router.delete("/delete-devices")
async def delete_devices(devices: deleteRequest, current_user: dict = Depends(get_current_user)):
    print("Devices to delete:", devices.devices)
    object_ids = [ObjectId(device_id) for device_id in devices.devices]
    result = devices_collection.delete_many({"_id": {"$in": object_ids}})
    print(result)
    
    return JSONResponse(content={"message": "Devices deleted successfully"}, status_code=200)

