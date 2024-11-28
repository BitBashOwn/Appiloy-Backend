from fastapi import APIRouter, Depends, HTTPException, Query
from models.tasks import taskModel, tasks_collection
from models.bots import bots_collection
from bson import ObjectId
from fastapi.responses import JSONResponse
from jose import JWTError
from utils.utils import generate_unique_id, get_current_user,get_Running_Tasks
from datetime import datetime
from pydantic import BaseModel
from typing import List
import traceback


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
async def create_Task(task: taskModel, current_user: dict = Depends(get_current_user)):
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
async def get_Task(id: str, current_user: dict = Depends(get_current_user)):
    try:
        task = tasks_collection.find_one(
            {"id": id}, {"_id": 0, "activationDate": 0, "status": 0,"activeJobs":0})
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
    print("entered /get-all-task")
    try:
        tasks = list(tasks_collection.find(
            {"email": current_user.get("email")}, {"_id": 0}))
        for task in tasks:
            if 'activationDate' in task and isinstance(task['activationDate'], datetime):
                task['activationDate'] = task['activationDate'].isoformat()

            if task.get("isScheduled"):
                active_jobs = task.get("activeJobs", [])
                for job in active_jobs:
                    job["startTime"] = job["startTime"].isoformat()
                    job["endTime"] = job["endTime"].isoformat()

            bot_id = task.get("bot")
            if bot_id:
                bot = bots_collection.find_one(
                    {"id": bot_id},
                    {"_id": 0, "platform": 1, "botName": 1, "imagePath": 1, "id": 1}
                )
                task.update({"botDetails": bot})
        return JSONResponse(content={"message": "Task fetched successfully!", "tasks": tasks}, status_code=200)
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        traceback.print_exc()
        return JSONResponse(content={"message": "Error fetching task", "error": str(e)}, status_code=400)

@tasks_router.get("/get-scheduled-tasks")
async def get_scheduled_tasks(current_user: dict = Depends(get_current_user)):
    try:
        # Get scheduled tasks, excluding activeJobs and _id
        result = list(tasks_collection.find(
            {"email": current_user.get("email"), "status": "scheduled"}, {"_id": 0, "activeJobs": 0}))

        for task in result:
            # Convert activationDate to ISO format if it's a datetime object
            if 'activationDate' in task and isinstance(task['activationDate'], datetime):
                task['activationDate'] = task['activationDate'].isoformat()

            # Fetch and update bot details
            bot_id = task.get("bot")
            if bot_id:
                bot = bots_collection.find_one(
                    {"id": bot_id},
                    {"_id": 0, "platform": 1, "botName": 1, "imagePath": 1, "id": 1}
                )
                if bot:  # Make sure bot is not None
                    task.update({"botDetails": bot})

        return JSONResponse(content={"message": "Scheduled tasks fetched successfully!", "tasks": result}, status_code=200)

    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        traceback.print_exc()
        return JSONResponse(content={"message": "Error fetching scheduled tasks", "error": str(e)}, status_code=400)
    
    
@tasks_router.get("/get-running-tasks")
async def get_running_tasks(current_user: dict = Depends(get_current_user)):
    try:
        # Get scheduled tasks, excluding activeJobs and _id
        result = list(tasks_collection.find(
            {"email": current_user.get("email"), "status": "running"}, {"_id": 0}))
        
        result = get_Running_Tasks(result)
        for task in result:
            
            if 'activationDate' in task and isinstance(task['activationDate'], datetime):
                task['activationDate'] = task['activationDate'].isoformat()
                
            if task.get("isScheduled"):
                active_jobs = task.get("activeJobs", [])
                for job in active_jobs:
                    job["startTime"] = job["startTime"].isoformat()
                    job["endTime"] = job["endTime"].isoformat()
            
            bot_id = task.get("bot")
            if bot_id:
                bot = bots_collection.find_one(
                    {"id": bot_id},
                    {"_id": 0, "platform": 1, "botName": 1, "imagePath": 1, "id": 1}
                )
                if bot:  
                    task.update({"botDetails": bot})
        return JSONResponse(content={"message": "Running tasks fetched successfully!", "tasks": result}, status_code=200)

    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        traceback.print_exc()
        return JSONResponse(content={"message": "Error fetching running tasks", "error": str(e)}, status_code=400)


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


@tasks_router.put("/update-task")
async def update_task(data: dict, current_user: dict = Depends(get_current_user)):
    print(data)
    result = tasks_collection.update_one({"id": data["id"], "email": current_user.get(
        "email")}, {"$set": data["data"]})

    return JSONResponse(content={"message": "Updated successfully"}, status_code=200)



