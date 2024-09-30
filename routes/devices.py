from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from utils.utils import get_current_user
from models.devices import devices_collection


devices_router = APIRouter()

devices = [
        {"id": 1, "name": "Device A", "owner": current_user["email"]},
        {"id": 2, "name": "Device B", "owner": current_user["email"]},
    ]

@devices_router.get("/devices")
async def get_devices(current_user: dict = Depends(get_current_user)):
    devices_collection.insert_many()
    

    return JSONResponse(content={"user": current_user, "devices": devices}, status_code=200)
