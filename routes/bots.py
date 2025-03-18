from fastapi import APIRouter, Query, Depends
from models.bots import bots_collection
from fastapi.responses import JSONResponse
from typing import List
from utils.utils import get_current_user


bots_router = APIRouter()


@bots_router.get("/bots")
def getBots(current_user: dict = Depends(get_current_user)):
    try:
        bots = list(bots_collection.find(
            {}, {"_id": 0, "id": 1, "botName": 1, "description": 1, "noOfUsers": 1, "imagePath": 1, "platform": 1, "development": 1}).sort("development", -1))

        return JSONResponse(content={"message": "successfully fetched data", "data": bots}, status_code=200)

    except Exception as e:
        return JSONResponse(content={"message": "could not fetch data", "error": str(e)}, status_code=500)


@bots_router.get("/get-bot")
def get_bot_by_name(name: str, fields: List[str] = Query(...), current_user: dict = Depends(get_current_user)):
    try:
        projection = {field: 1 for field in fields}
        projection.update({"_id": 0})
        bot = bots_collection.find_one({"botName": name}, projection)
        if bot:
            return JSONResponse(content={"message": "successfully fetched data", "data": bot}, status_code=200)
    except Exception as e:
        return JSONResponse(content={"message": "could not fetch data", "error": str(e)}, status_code=500)
