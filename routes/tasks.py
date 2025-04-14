from fastapi import APIRouter, Depends, HTTPException, Query
from pymongo import UpdateOne
from models.tasks import taskModel, tasks_collection
from models.bots import bots_collection
from fastapi.responses import JSONResponse
from jose import JWTError
from utils.utils import generate_unique_id, get_current_user,get_Running_Tasks
from datetime import datetime, timedelta
from pydantic import BaseModel
from typing import List
import traceback
import pytz


class deleteRequest(BaseModel):
    tasks: list
    
    
    
class clearOldJobsRequest(BaseModel):
    Task_ids: list
    command: dict

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
async def get_all_Task(current_user: dict = Depends(get_current_user)):
    print("entered /get-all-task")
    try:
        tasks = list(tasks_collection.find(
            {"email": current_user.get("email")}, {"_id": 0, "activeJobs": 0}))
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
            {"email": current_user.get("email"), "status": "running"}, {"_id": 0, "activeJobs":0}))
        
        # result = get_Running_Tasks(result)
        for task in result:
            
            if 'activationDate' in task and isinstance(task['activationDate'], datetime):
                task['activationDate'] = task['activationDate'].isoformat()
                
            # if task.get("isScheduled"):
            #     active_jobs = task.get("activeJobs", [])
            #     for job in active_jobs:
            #         job["startTime"] = job["startTime"].isoformat()
            #         job["endTime"] = job["endTime"].isoformat()
            
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

@tasks_router.put("/clear-old-jobs")
async def clear_old_jobs(tasks: clearOldJobsRequest, current_user: dict = Depends(get_current_user)):
    task_ids = tasks.Task_ids
    time_zone = tasks.command.get("timeZone", "UTC")
    
    print(f"[LOG] Clearing old jobs for tasks: {task_ids}")
    print(f"[LOG] Using time zone: {time_zone}")
    
    if not task_ids:
        return JSONResponse(content={"message": "No tasks provided"}, status_code=400)
        
    try:
        # Get current time in the specified timezone
        user_tz = pytz.timezone(time_zone)
        current_time = datetime.now(user_tz)
        print(f"[LOG] Current time in {time_zone}: {current_time}")
        
        # Calculate the cutoff time for running tasks (24 hours ago)
        cutoff_time_running = current_time - timedelta(hours=24)
        print(f"[LOG] Cutoff time for running tasks in {time_zone}: {cutoff_time_running}")
        
        # Fetch all tasks that need to be processed
        all_tasks = list(tasks_collection.find(
            {"id": {"$in": task_ids}, "email": current_user.get("email")}
        ))
        
        if not all_tasks:
            print("[LOG] No tasks found for the provided IDs and user")
            return JSONResponse(content={"message": "No tasks found"}, status_code=404)
            
        print(f"[LOG] Found {len(all_tasks)} tasks to process")
        
        bulk_operations = []
        processed_tasks = 0
        
        for task in all_tasks:
            task_id = task.get("id")
            current_status = task.get("status", "awaiting")
            current_jobs = task.get("activeJobs", [])
            
            # Skip tasks with no jobs
            if not current_jobs:
                print(f"[LOG] Task {task_id} has no active jobs, skipping")
                continue
                
            future_jobs = []
            old_jobs = []
            
            # Process each job in the task
            for job in current_jobs:
                start_time = job.get("startTime")
                job_id = job.get("job_id", "unknown_job")
                
                if not start_time:
                    old_jobs.append(job)  # Consider jobs without start time as old
                    continue
                    
                try:
                    # Handle different datetime formats
                    if isinstance(start_time, dict) and "$date" in start_time:
                        start_time_str = start_time["$date"]
                        job_start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    elif isinstance(start_time, datetime):
                        job_start_time = start_time
                    else:
                        print(f"[ERROR] Unrecognized startTime format in task {task_id}, job {job_id}: {type(start_time)}")
                        old_jobs.append(job)  # Consider problematic jobs as old to be safe
                        continue
                        
                    # Ensure the time is timezone-aware
                    if job_start_time.tzinfo is None:
                        # If time is naive (no timezone), assume it's in UTC
                        job_start_time = pytz.UTC.localize(job_start_time)
                    
                    # Convert job time to user's timezone for comparison
                    job_start_time = job_start_time.astimezone(user_tz)
                    
                    print(f"[LOG] Job {job_id} start time in {time_zone}: {job_start_time}")
                    
                    # Different handling based on task status
                    if current_status == "running":
                        # For running tasks, only clear jobs older than 24 hours
                        if job_start_time < cutoff_time_running:
                            print(f"[LOG] Job {job_id} is older than 24 hours, marking as old")
                            old_jobs.append(job)
                        else:
                            print(f"[LOG] Job {job_id} is within 24 hours, keeping it")
                            future_jobs.append(job)
                    else:
                        # For non-running tasks, check against current time
                        if job_start_time > current_time:
                            print(f"[LOG] Job {job_id} is in the future, keeping it")
                            future_jobs.append(job)
                        else:
                            print(f"[LOG] Job {job_id} is in the past, marking as old")
                            old_jobs.append(job)
                            
                except Exception as e:
                    print(f"[ERROR] Error processing job {job_id} in task {task_id}: {str(e)}")
                    old_jobs.append(job)  # Consider problematic jobs as old to be safe
            
            # Skip update if no jobs were classified as old
            if not old_jobs:
                print(f"[LOG] No old jobs found for task {task_id}, skipping update")
                continue
                
            # Determine new status based on remaining jobs and current status
            if future_jobs:
                new_status = "scheduled"
            else:
                new_status = "awaiting"
                
            # Keep running status if the task is currently running and we still have future jobs
            if current_status == "running" and future_jobs:
                new_status = "running"
                
            print(f"[LOG] Task {task_id}: Total jobs: {len(current_jobs)}, Old jobs: {len(old_jobs)}, Future jobs: {len(future_jobs)}")
            print(f"[LOG] Task {task_id}: Old status: {current_status}, New status: {new_status}")
            
            # Add update operation to bulk operations
            bulk_operations.append(UpdateOne(
                {"id": task_id},
                {"$set": {"activeJobs": future_jobs, "status": new_status}}
            ))
            
            processed_tasks += 1
            
        # Execute bulk update if there are any operations
        if bulk_operations:
            result = tasks_collection.bulk_write(bulk_operations)
            print(f"[LOG] Updated {result.modified_count} tasks")
            return JSONResponse(
                content={
                    "message": "Clear Old Jobs successfully",
                    "processed_tasks": processed_tasks,
                    "modified_tasks": result.modified_count
                }, 
                status_code=200
            )
        else:
            print("[LOG] No tasks needed updating")
            return JSONResponse(
                content={
                    "message": "No tasks needed updating",
                    "processed_tasks": 0,
                    "modified_tasks": 0
                }, 
                status_code=200
            )
            
    except pytz.exceptions.UnknownTimeZoneError:
        error_msg = f"Invalid timezone: {time_zone}"
        print(f"[ERROR] {error_msg}")
        return JSONResponse(
            status_code=400,
            content={"message": error_msg}
        )
    except Exception as e:
        print(f"[ERROR] General error in clear_old_jobs: {str(e)}")
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"message": f"An error occurred: {str(e)}"}
        )
        
        
@tasks_router.patch("/unschedule-jobs")
async def unschedule_jobs(tasks: deleteRequest, current_user: dict = Depends(get_current_user)):
    print(f"[LOG] Unscheduling jobs for tasks: {tasks.tasks}")
    
    if not tasks.tasks:
        return JSONResponse(content={"message": "No tasks provided"}, status_code=400)
        
    try:
        # Fetch all tasks that need to be processed
        result = tasks_collection.update_many(
            {"id": {"$in": tasks.tasks}, "email": current_user.get("email")},
            {"$set": {"activeJobs": [], "status": "awaiting"}}
        )
        
        if result.matched_count == 0:
            print("[LOG] No tasks found for the provided IDs and user")
            return JSONResponse(content={"message": "No tasks found"}, status_code=404)
        
        return JSONResponse(content={"message": f"Successfully unscheduled {result.modified_count} tasks."}, status_code=200)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")