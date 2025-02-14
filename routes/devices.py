from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from utils.utils import get_current_user
# from models.devices import devices_collection
from config.database import db
from pydantic import BaseModel
from bson import ObjectId
import json


devices_router = APIRouter()


class deleteRequest(BaseModel):
    devices: list


class updateRequest(BaseModel):
    devices: list
    dataToUpdate: dict[str, str]


# @devices_router.get("/devices")
# async def get_devices(current_user: dict = Depends(get_current_user)):
#     # print(current_user)
#     # data = list(devices_collection.find({"email": current_user.get("email")}, {"_id": 0}))
#     data = list(db["devices"].find({"email": current_user.get("email")}))

#     # for device in data:
#     #     if '_id' in device:
#     #         device['_id'] = str(device['_id'])

#     # devices = json.dumps(data)
#     devices = data
#     return JSONResponse(content={"devices": devices}, status_code=200)
# Helper function to convert MongoDB ObjectId to string
def convert_object_id(data):
    if isinstance(data, list):
        for item in data:
            item['_id'] = str(item['_id'])
    elif isinstance(data, dict):
        data['_id'] = str(data['_id'])
    return data


@devices_router.get("/devices")
async def get_devices(current_user: dict = Depends(get_current_user)):

    # Query the devices for the current user from MongoDB
    data = list(db["devices"].find(
        {"email": current_user.get("email")}, {"_id": 0}))

    # Convert ObjectId fields to strings for JSON serialization
    # data = convert_object_id(data)

    return JSONResponse(content={"devices": data}, status_code=200)


@devices_router.delete("/delete-devices")
async def delete_devices(devices: deleteRequest, current_user: dict = Depends(get_current_user)):
    print("Devices to delete:", devices.devices)
    # object_ids = [ObjectId(device_id) for device_id in devices.devices]
    # result = devices_collection.delete_many({"_id": {"$in": object_ids}})
    result = db["devices"].delete_many(
        {"deviceId": {"$in": devices.devices}, "email": current_user.get("email")})
    print(result)

    return JSONResponse(content={"message": "Devices deleted successfully"}, status_code=200)


@devices_router.patch("/edit-device")
async def delete_devices(data: updateRequest, current_user: dict = Depends(get_current_user)):
    print("Update Data Request Data:", data)

    if not data.devices:
        raise HTTPException(
            status_code=400, detail="No devices provided for update.")
    if not data.dataToUpdate:
        raise HTTPException(
            status_code=400, detail="No data provided to update")

    update_query = {"$set": data.dataToUpdate}

    result = db["devices"].update_many(
        {"deviceId": {"$in": data.devices}, "email": current_user.get("email")}, update_query)

    if result.matched_count == 0:
        raise HTTPException(
            status_code=404, detail="No devices found for the given IDs or user.")

    return JSONResponse(
        content={"message": f"{result.modified_count} devices updated successfully"},
        status_code=200
    )
