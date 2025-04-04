from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from utils.utils import get_current_user
# from models.devices import devices_collection
from config.database import db
from pydantic import BaseModel
from typing import Dict
from connection_registry import is_device_connected
from models.devices import devices_collection
from logger import logger

devices_router = APIRouter()


class deleteRequest(BaseModel):
    devices: list

class updateStatusRequest(BaseModel):
    devices: list
    
class updateRequest(BaseModel):
    devices: list
    dataToUpdate: Dict[str, str]


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
async def edit_devices(data: updateRequest, current_user: dict = Depends(get_current_user)):
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


@devices_router.patch("/update-status")
async def update_status(data: updateStatusRequest, current_user: dict = Depends(get_current_user)):
    try:
        logger.info(f"Update Data Request Data: {data}")

        if not data.devices:
            raise HTTPException(
                status_code=400, detail="No devices provided for updating status."
            )

        connected_devices = []
        not_connected_devices = []

        for device in data.devices:
            if is_device_connected(device):
                connected_devices.append(device)
            else:
                not_connected_devices.append(device)

        connected_result = None
        not_connected_result = None

        if connected_devices:
            connected_result = devices_collection.update_many(
                {"deviceId": {"$in": connected_devices}, "email": current_user.get("email")},
                {"$set": {"status": True}}
            )

        if not_connected_devices:
            not_connected_result = devices_collection.update_many(
                {"deviceId": {"$in": not_connected_devices}, "email": current_user.get("email")},
                {"$set": {"status": False}}
            )

        connected_count = connected_result.matched_count if connected_result else 0
        not_connected_count = not_connected_result.matched_count if not_connected_result else 0

        return_message = f"{connected_count} devices connected and {not_connected_count} devices not connected"
        
        logger.info(return_message)

        return JSONResponse(
            content={"message": return_message},
            status_code=200
        )

    except HTTPException as e:
        raise e  # Let FastAPI handle HTTPExceptions as-is

    except Exception as e:
        # Log the exception if you have a logger
        logger.error(f"Unexpected error: {e}")
        return JSONResponse(
            content={"message": "An unexpected error occurred. Please try again later."},
            status_code=500
        )