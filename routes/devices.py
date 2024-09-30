from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from utils.utils import get_current_user
from models.devices import devices_collection


devices_router = APIRouter()


@devices_router.get("/devices")
async def get_devices(current_user: dict = Depends(get_current_user)):
    devices_collection.insert_many([
        {
            "deviceName": "iPhone 13",
            "model": "A2634",
            "botName": ["Instagram"],
            "status": True,
            "activationDate": "2023-01-15T00:00:00"
        },
        {
            "deviceName": "Samsung Galaxy S21",
            "model": "SM-G998B",
            "botName": ["Facebook"],
            "status": False,
            "activationDate": "2022-12-05T00:00:00"
        },
        {
            "deviceName": "Google Pixel 6",
            "model": "GD1YQ",
            "botName": ["Twitter"],
            "status": True,
            "activationDate": "2023-03-21T00:00:00"
        },
        {
            "deviceName": "OnePlus 9",
            "model": "LE2115",
            "botName": ["Reddit"],
            "status": False,
            "activationDate": "2023-02-11T00:00:00"
        },
        {
            "deviceName": "iPad Pro",
            "model": "A2378",
            "botName": ["Snapchat"],
            "status": False,
            "activationDate": "2022-10-20T00:00:00"
        },
        {
            "deviceName": "MacBook Air M1",
            "model": "A2337",
            "botName": ["LinkedIn"],
            "status": True,
            "activationDate": "2022-08-14T00:00:00"
        },
        {
            "deviceName": "Sony Xperia 1 III",
            "model": "XQ-BC72",
            "botName": ["Pinterest"],
            "status": False,
            "activationDate": "2023-04-08T00:00:00"
        },
        {
            "deviceName": "Microsoft Surface Pro 8",
            "model": "1983",
            "botName": ["YouTube"],
            "status": False,
            "activationDate": "2022-07-03T00:00:00"
        },
        {
            "deviceName": "Huawei P40",
            "model": "ANA-NX9",
            "botName": ["TikTok"],
            "status": True,
            "activationDate": "2023-05-15T00:00:00"
        },
        {
            "deviceName": "Xiaomi Mi 11",
            "model": "M2011K2G",
            "botName": ["Tumblr"],
            "status": False,
            "activationDate": "2022-09-01T00:00:00"
        },
        {
            "deviceName": "Nokia 8.3 5G",
            "model": "TA-1243",
            "botName": ["Discord"],
            "status": True,
            "activationDate": "2023-07-10T00:00:00"
        },
        {
            "deviceName": "Oppo Find X3",
            "model": "CPH2173",
            "botName": ["Telegram"],
            "status": False,
            "activationDate": "2023-06-20T00:00:00"
        },
        {
            "deviceName": "Sony Xperia 5 II",
            "model": "XQ-AS52",
            "botName": ["WhatsApp"],
            "status": True,
            "activationDate": "2023-02-28T00:00:00"
        }
    ]
    )

    return JSONResponse(content={"user": current_user}, status_code=200)
