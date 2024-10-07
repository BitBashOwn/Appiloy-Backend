from fastapi import APIRouter, Depends,HTTPException
from models.tasks import taskModel, tasks_collection
from models.bots import bots_collection
from bson import ObjectId
from fastapi.responses import JSONResponse
from jose import JWTError
from utils.utils import generate_unique_id
from datetime import datetime


tasks_router = APIRouter()


@tasks_router.post("/create-task")
async def create_Task(task: taskModel):
    try:
        task_dict = task.dict(by_alias=True)
        task_id = generate_unique_id()
        bot = bots_collection.find_one({"id": task.bot}, {"inputs": 1})
        task_dict.update({"id": task_id, "inputs": bot.get(
            "inputs"), "LastModifiedDate": datetime.utcnow().timestamp()})
        # task_dict["bot"] = ObjectId(task_dict["bot"])
        result = tasks_collection.insert_one(task_dict)
        return JSONResponse(content={"message": "Task created successfully!", "id": task_id}, status_code=200)

    except JWTError:
        return JSONResponse(content={"message": "sorry could not create task"}, status_code=400)


@tasks_router.get("/get-task")
async def get_Task(id: str):
    try:
        task = tasks_collection.find_one({"id": id}, {"_id": 0})
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        
        # Fetch associated bot details
        bot = bots_collection.find_one({"id": task.get("bot")}, {"_id": 0})
        if not bot:
            raise HTTPException(status_code=404, detail="Bot not found")
        
        return JSONResponse(content={"message": "Task fetched successfully!", "task": task, "bot": bot}, status_code=200)

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=400, detail="Error fetching task or bot data")


