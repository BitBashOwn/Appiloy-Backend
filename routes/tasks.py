from fastapi import APIRouter, Depends
from models.tasks import taskModel, tasks_collection
from bson import ObjectId
from fastapi.responses import JSONResponse




tasks_router = APIRouter()

@tasks_router.post("/create-task")
def create_Task(task: taskModel):
    print(task)
    task_dict = task.dict(by_alias=True)
    task_dict["bot"] = ObjectId(task_dict["bot"])
    result = tasks_collection.insert_one(task_dict)
    return JSONResponse(content={"message": "Task created successfully!"}, status_code=200)