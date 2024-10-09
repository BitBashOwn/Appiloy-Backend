from fastapi import APIRouter, Depends, HTTPException, Query
from models.tasks import taskModel, tasks_collection
from models.bots import bots_collection
from bson import ObjectId
from fastapi.responses import JSONResponse
from jose import JWTError
from utils.utils import generate_unique_id, get_current_user
from datetime import datetime
from pydantic import BaseModel
from typing import List


class deleteRequest(BaseModel):
    tasks: list


class inputsSaveRequest(BaseModel):
    inputs: list
    id: str


class devicesSaveRequest(BaseModel):
    devices: list
    id: str


tasks_router = APIRouter()


@tasks_router.post("/create-task")
async def create_Task(task: taskModel):
    try:
        task_dict = task.dict(by_alias=True)
        task_id = generate_unique_id()
        bot = bots_collection.find_one(
            {"id": task.bot}, {"inputs": 1, "schedules": 1})
        task_dict.update({
            "id": task_id,
            "inputs": bot.get("inputs"),
            "LastModifiedDate": datetime.utcnow().timestamp(),
            "activationDate":  datetime.utcnow(),
            "deviceIds": [],
            "schedules": bot.get("schedules")
        })
        # task_dict["bot"] = ObjectId(task_dict["bot"])
        result = tasks_collection.insert_one(task_dict)
        return JSONResponse(content={"message": "Task created successfully!", "id": task_id}, status_code=200)

    except JWTError:
        return JSONResponse(content={"message": "sorry could not create task"}, status_code=400)


@tasks_router.get("/get-task")
async def get_Task(id: str):
    try:
        task = tasks_collection.find_one(
            {"id": id}, {"_id": 0, "activationDate": 0, "status": 0, "email": 0})
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        if 'activationDate' in task and isinstance(task['activationDate'], datetime):
            task['activationDate'] = task['activationDate'].isoformat()
        # Fetch associated bot details
        bot = bots_collection.find_one({"id": task.get("bot")}, {
                                       "_id": 0, "platform": 1, "botName": 1, "imagePath": 1, "id": 1})
        if not bot:
            raise HTTPException(status_code=404, detail="Bot not found")

        return JSONResponse(content={"message": "Task fetched successfully!", "task": task, "bot": bot}, status_code=200)

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(
            status_code=400, detail="Error fetching task or bot data")


@tasks_router.get("/get-all-task")
async def get_Task(current_user: dict = Depends(get_current_user)):
    try:
        tasks = list(tasks_collection.find(
            {"email": current_user.get("email")}, {"_id": 0}))
        for task in tasks:
            if 'activationDate' in task and isinstance(task['activationDate'], datetime):
                task['activationDate'] = task['activationDate'].isoformat()
            bot_id = task.get("bot")
            if bot_id:
                # Fetch the corresponding bot details.
                bot = bots_collection.find_one(
                    {"id": bot_id},
                    {"_id": 0, "platform": 1, "botName": 1, "imagePath": 1, "id": 1}
                )
                task.update({"botDetails": bot})
        return JSONResponse(content={"message": "Task fetched successfully!", "tasks": tasks}, status_code=200)

    except Exception as e:
        return JSONResponse(content={"message": "Error fetching task"}, status_code=400)


@tasks_router.delete("/delete-tasks")
async def delete_tasks(tasks: deleteRequest, current_user: dict = Depends(get_current_user)):
    # print("Devices to delete:", tasks.tasks)
    # object_ids = [ObjectId(device_id) for device_id in devices.devices]
    # result = devices_collection.delete_many({"_id": {"$in": object_ids}})
    result = tasks_collection.delete_many(
        {"id": {"$in": tasks.tasks}, "email": current_user.get("email")})

    return JSONResponse(content={"message": "Devices deleted successfully"}, status_code=200)


@tasks_router.get("/get-task-fields")
def get_task_fields(id: str, fields: List[str] = Query(...), current_user: dict = Depends(get_current_user)):
    try:
        projection = {field: 1 for field in fields}
        projection.update({"_id": 0})
        task = tasks_collection.find_one(
            {"id": id, "email": current_user.get('email')}, projection)
        if task:
            # bot['_id'] = str(bot['_id'])
            return JSONResponse(content={"message": "successfully fetched data", "data": task}, status_code=200)
    except Exception as e:
        return JSONResponse(content={"message": "could not fetch data", "error": str(e)}, status_code=500)


@tasks_router.post("/save-inputs")
async def save_task_inputs(inputs: inputsSaveRequest, current_user: dict = Depends(get_current_user)):
    result = tasks_collection.update_one({"id": inputs.id, "email": current_user.get(
        "email")}, {"$set": {"inputs": inputs.inputs}})

    return JSONResponse(content={"message": "Inputs updated successfully"}, status_code=200)


@tasks_router.post("/save-device")
async def save_task_devices(data: devicesSaveRequest, current_user: dict = Depends(get_current_user)):
    result = tasks_collection.update_one({"id": data.id, "email": current_user.get(
        "email")}, {"$set": {"deviceIds": data.devices}})

    return JSONResponse(content={"message": "devices updated successfully"}, status_code=200)
