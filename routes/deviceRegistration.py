from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Depends
from pymongo import MongoClient
from pydantic import BaseModel
from typing import List, Tuple
import random
from datetime import datetime, timedelta, timezone
from utils.utils import get_current_user, check_for_Job_clashes, split_message
from models.tasks import tasks_collection
# from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
import json
import uuid
from models.tasks import tasks_collection
from fastapi.responses import JSONResponse
from Bot.discord_bot import bot_instance

# MongoDB Connection
client = MongoClient(
    "mongodb+srv://abdullahnoor94:dodge2018@appilot.ds9ll.mongodb.net/?retryWrites=true&w=majority&appName=Appilot")
db = client["Appilot"]
device_collection = db["devices"]

# Create Router
device_router = APIRouter(prefix="")
scheduler = AsyncIOScheduler()
scheduler.start()
print('started scheduling')

# In-memory storage for WebSocket connections and mapping them to device IDs
active_connections: List[WebSocket] = []
device_connections = {}  # To store device_id: websocket mapping

# Device registration model with updated fields


class DeviceRegistration(BaseModel):
    deviceName: str
    deviceId: str
    model: str
    botName: List[str]
    status: bool = True
    activationDate: str
    email: str

# Command model


class CommandRequest(BaseModel):
    command: dict
    device_ids: List[str]
    
class StopTaskCommandRequest(BaseModel):
    command: dict
    Task_ids: List[str]


def register_device(device_data: DeviceRegistration):
    device = device_collection.find_one({"deviceId": device_data.deviceId})
    if device:
        raise HTTPException(
            status_code=400, detail="Device already registered")
    result = device_collection.insert_one(device_data.dict())
    return {"message": "Device registered successfully", "deviceId": device_data.deviceId}

# Device Registration Endpoint (POST)


@device_router.post("/register_device")
async def register_device_endpoint(device_data: DeviceRegistration):
    return register_device(device_data)

# Endpoint to check the device status


@device_router.get("/device_status/{device_id}")
async def check_device_status(device_id: str):
    device = device_collection.find_one({"deviceId": device_id})
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    return {"device_id": device_id, "status": device["status"]}

# Endpoint to update the device status


@device_router.put("/update_status/{device_id}")
async def update_device_status(device_id: str, status: bool):
    device = device_collection.find_one({"deviceId": device_id})
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    device_collection.update_one({"deviceId": device_id}, {
                                 "$set": {"status": status}})
    return {"message": f"Device {device_id} status updated to {status}"}

# Endpoint for checking device registration


@device_router.get("/device_registration/{device_id}")
async def check_device_registration(device_id: str):
    device = device_collection.find_one({"deviceId": device_id})
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    if not device["status"]:
        device_collection.update_one({"deviceId": device_id}, {
                                     "$set": {"status": True}})

    return True


@device_router.websocket("/ws/{device_id}")
async def websocket_endpoint(websocket: WebSocket, device_id: str):
    await websocket.accept()
    device_connections[device_id] = websocket
    active_connections.append(websocket)

    try:
        while True:
            # Receive JSON message from client
            data = await websocket.receive_text()
            print(f"Message from {device_id}: {data}")

            try:
                payload = json.loads(data)
                message = payload.get("message")
                task_id = payload.get("task_id")
                job_id = payload.get("job_id")

                print(f"Parsed payload: message={message}, task_id={task_id}, job_id={job_id}")

                taskData = tasks_collection.find_one(
                    {"id": task_id}, {"serverId": 1, "channelId": 1, "_id": 0}
                )

                if taskData and taskData.get("serverId") and taskData.get("channelId"):
                    server_id = int(taskData["serverId"]) if isinstance(
                        taskData["serverId"], str) and taskData["serverId"].isdigit() else taskData["serverId"]
                    channel_id = int(taskData["channelId"]) if isinstance(
                        taskData["channelId"], str) and taskData["channelId"].isdigit() else taskData["channelId"]

                    message_length = len(message) if message else 0
                    print(f"Message Length: {message_length}")

                    if message_length > 2000:
                        message_chunks = split_message(message)
                        for chunk in message_chunks:
                            await bot_instance.send_message({
                                "message": chunk,
                                "task_id": task_id,
                                "job_id": job_id,
                                "server_id": server_id,
                                "channel_id": channel_id
                            })
                    else:
                        await bot_instance.send_message({
                            "message": message,
                            "task_id": task_id,
                            "job_id": job_id,
                            "server_id": server_id,
                            "channel_id": channel_id
                        })

                else:
                    print(
                        f"Skipping message send. Missing or empty serverId/channelId for task {task_id}")

                tasks_collection.update_one(
                    {"id": task_id},
                    {
                        "$pull": {
                            "activeJobs": {
                                "job_id": job_id
                            }
                        }
                    }
                )

                task = tasks_collection.find_one({"id": task_id})
                if task:
                    status = "awaiting" if len(
                        task.get("activeJobs", [])) == 0 else "scheduled"
                    tasks_collection.update_one(
                        {"id": task_id},
                        {"$set": {"status": status}}
                    )

            except json.JSONDecodeError:
                print(f"Invalid JSON received from {device_id}: {data}")

    except WebSocketDisconnect:
        print(f"Device {device_id} disconnected.")
        device_collection.update_one(
            {"deviceId": device_id},
            {"$set": {"status": False}}
        )
        active_connections.remove(websocket)
        device_connections.pop(device_id, None)


@device_router.post("/send_command")
async def send_command(request: CommandRequest, current_user: dict = Depends(get_current_user)):
    task_id = request.command.get("task_id")
    command = request.command
    print(command)
    device_ids = request.device_ids
    duration = int(command.get("duration", 0))
    durationType = request.command.get("durationType")
    time_zone = request.command.get("timeZone", "UTC")
    newInputs = request.command.get("newInputs")
    newSchedules = request.command.get("newSchecdules")
    tasks_collection.update_one(
        {"id": task_id}, {"$set": {"inputs": newInputs, "schedules": newSchedules, "deviceIds":device_ids}})
    

    try:

        user_tz = pytz.timezone(time_zone)
        now = datetime.now(user_tz)
        print(f"Current time in {time_zone}: {now}")

        if durationType in ['DurationWithExactStartTime', 'ExactStartTime']:
            time_str = request.command.get("exactStartTime")
            hour, minute = parse_time(time_str)

            target_time = now.replace(
                hour=hour, minute=minute, second=0, microsecond=0)
            end_time_delta = timedelta(minutes=duration)
            target_end_time = target_time + end_time_delta

            if target_time < now:
                target_time += timedelta(days=1)
                target_end_time += timedelta(days=1)

            target_time_utc = target_time.astimezone(pytz.UTC)
            target_end_time_utc = target_end_time.astimezone(pytz.UTC)

            if check_for_job_clashes(target_time_utc, target_end_time_utc, task_id, device_ids):
                return JSONResponse(content={"message": "Task already Scheduled on this time"}, status_code=400)

            job_id = f"cmd_{uuid.uuid4()}"
            command['job_id'] = job_id
            schedule_single_job(
                target_time_utc, target_end_time_utc, device_ids, command, job_id, task_id)

        elif durationType == 'DurationWithTimeWindow':
            start_time_str = command.get("startInput")
            end_time_str = command.get("endInput")

            start_hour, start_minute = parse_time(start_time_str)
            end_hour, end_minute = parse_time(end_time_str)

            start_time = now.replace(
                hour=start_hour, minute=start_minute, second=0, microsecond=0)
            end_time = now.replace(
                hour=end_hour, minute=end_minute, second=0, microsecond=0)

            if end_time < start_time:
                end_time += timedelta(days=1)
            if start_time < now:
                start_time += timedelta(days=1)
                end_time += timedelta(days=1)

            if check_for_job_clashes(start_time, end_time, task_id, device_ids):
                return JSONResponse(content={"message": "Task already Scheduled on this time"}, status_code=400)

            time_window = (end_time - start_time).total_seconds() / 60
            if abs(time_window - duration) <= 10:
                job_id = f"cmd_{uuid.uuid4()}"
                command['job_id'] = job_id
                schedule_single_job(start_time, end_time,
                                    device_ids, command, job_id, task_id)
            else:
                random_durations, start_times = generate_random_durations_and_start_times(
                    duration, start_time, end_time)
                schedule_split_jobs(
                    start_times, random_durations, device_ids, command, task_id)

        elif durationType == 'EveryDayAutomaticRun':
            schedule_recurring_job(command, device_ids)

        return {"message": "Command scheduled successfully"}

    except pytz.exceptions.UnknownTimeZoneError:
        raise HTTPException(
            status_code=400, detail=f"Invalid timezone: {time_zone}")
    except ValueError as e:
        raise HTTPException(
            status_code=400, detail=f"Invalid time format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

@device_router.post("/stop_task")
async def stop_task(request: StopTaskCommandRequest, current_user: dict = Depends(get_current_user)):
    command = request.command
    task_ids = request.Task_ids

    print(f"[LOG] Received stop command: {command}")
    print(f"[LOG] Task IDs: {task_ids}")

    connected_devices = []
    not_connected_devices = set()

    for task_Id in task_ids:
        print(f"[LOG] Checking task ID: {task_Id}")

        task = tasks_collection.find_one(
            {"id": task_Id}, 
            {"status": 1, "activeJobs": 1}  # Fetching only necessary fields
        )

        if not task:
            print(f"[LOG] Task {task_Id} not found in database.")
            continue  

        active_jobs = task.get("activeJobs", [])
        print(f"[LOG] Task {task_Id} has {len(active_jobs)} active jobs.")

        for job in active_jobs:
            job_id = job.get("job_id")
            device_ids = job.get("device_ids", [])

            if not job_id or not device_ids:
                print(f"[LOG] Skipping job {job_id} due to missing fields.")
                continue

            print(f"[LOG] Sending command for job {job_id}")

            for device_id in device_ids:
                websocket = device_connections.get(device_id)
                if websocket:
                    print(f"[LOG] Device {device_id} is connected for job {job_id}.")
                    connected_devices.append((device_id, websocket))
                else:
                    print(f"[LOG] Device {device_id} is NOT connected for job {job_id}.")
                    not_connected_devices.add(device_id)

    print(f"[LOG] Attempting to send command to connected devices.")
    for device_id, websocket in connected_devices:
        try:
            await websocket.send_text(json.dumps(command))
            print(f"[LOG] Successfully sent command to device {device_id}")
        except Exception as e:
            print(f"[ERROR] Error sending command to device {device_id}: {str(e)}")
            not_connected_devices.add(device_id)

    print(f"[LOG] Connected devices: {connected_devices}")
    print(f"[LOG] Not connected devices: {list(not_connected_devices)}")  # Convert set to list for printing

    return {"message": "Stop Command Sent successfully"}


           
async def send_command_to_devices(device_ids, command):
    print(f"Executing command for devices: {device_ids}, command: {command}")

    task_id = command.get("task_id")
    job_id = command.get("job_id")
    is_recurring = command.get("isRecurring", False)
    task = tasks_collection.find_one({"id": task_id})
    if task:
        connected_devices = []
        not_connected_devices = []

        for device_id in device_ids:
            websocket = device_connections.get(device_id)
            if websocket:
                connected_devices.append((device_id, websocket))
            else:
                not_connected_devices.append(device_id)

        # Send commands to connected devices
        task_status_updated = False
        for device_id, websocket in connected_devices:
            try:
                await websocket.send_text(json.dumps(command))
            except Exception as e:
                print(f"Error sending command to device {device_id}: {str(e)}")
                not_connected_devices.append(device_id)
            else:
                if not task_status_updated:
                    tasks_collection.update_one(
                        {"id": task_id},
                        {"$set": {"status": "running"}}
                    )
                    task_status_updated = True

        # Handle not connected devices
        if not_connected_devices:
            tasks_collection.update_one(
                {"id": task_id, "activeJobs.job_id": job_id},
                {"$pull": {"activeJobs.$.device_ids": {"$in": not_connected_devices}}}
            )

        # If all devices are not connected, remove the job from activeJobs
        if len(not_connected_devices) == len(device_ids):
            tasks_collection.update_one(
                {"id": task_id},
                {"$pull": {"activeJobs": {"job_id": job_id}}}
            )

        print(f"Connected devices: {connected_devices}")
        print(f"Not connected devices: {not_connected_devices}")

        # Log if the job is no longer active
        if len(not_connected_devices) == len(device_ids):
            print(
                f"Job {job_id} is no longer active as no devices are connected.")

        if is_recurring:
            task = tasks_collection.find_one({"id": task_id})
            schedule_recurring_job(command, device_ids)


def parse_time(time_str: str) -> tuple:
    """Parse time string in 'HH:MM' format to a tuple of integers (hour, minute)."""
    hour, minute = map(int, time_str.split(':'))
    return hour, minute


def check_for_job_clashes(start_time, end_time, task_id, device_ids) -> bool:
    """Check for job clashes in the scheduled time window."""
    return check_for_Job_clashes(start_time, end_time, task_id, device_ids)


def schedule_single_job(start_time, end_time, device_ids, command, job_id: str, task_id: str) -> None:
    """Schedule a single job with a defined start and end time."""
    jobInstance = {
        "job_id": job_id,
        "startTime": start_time,
        "endTime": end_time,
        "device_ids": device_ids
    }

    try:
        job = scheduler.add_job(
            send_command_to_devices,
            trigger=DateTrigger(
                run_date=start_time.astimezone(pytz.UTC),
                timezone=pytz.UTC
            ),
            args=[device_ids, {**command,
                               "duration": int(command.get("duration", 0))}],
            id=job_id,
            name=f"Single session command for devices {device_ids}"
        )

        tasks_collection.update_one(
            {"id": task_id},
            {"$set": {"status": "scheduled"},
             "$push": {"activeJobs": jobInstance}}
        )
    except Exception as e:
        print(f"Failed to schedule single job: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to schedule job: {str(e)}")


def generate_random_durations_and_start_times(duration: int, start_time: datetime, end_time: datetime) -> tuple:
    """Generate random durations and start times for split jobs."""
    random_durations = generate_random_durations(duration)
    start_times = get_random_start_times(
        start_time, end_time, random_durations)
    return random_durations, start_times


def schedule_split_jobs(start_times: List[datetime], random_durations: List[int], device_ids: List[str], command: dict, task_id: str) -> None:
    """Schedule multiple jobs based on random start times and durations."""
    for i, (start_time, duration) in enumerate(zip(start_times, random_durations)):
        job_id = f"cmd_{uuid.uuid4()}"
        end_time = start_time + timedelta(minutes=duration)
        jobInstance = {
            "job_id": job_id,
            "startTime": start_time,
            "endTime": end_time,
            "device_ids": device_ids
        }

        modified_command = {**command, "duration": duration}
        try:
            job = scheduler.add_job(
                send_command_to_devices,
                trigger=DateTrigger(
                    run_date=start_time.astimezone(pytz.UTC),
                    timezone=pytz.UTC
                ),
                args=[device_ids, modified_command],
                id=job_id,
                name=f"Part {i+1} of split command for devices {device_ids}"
            )

            tasks_collection.update_one(
                {"id": task_id},
                {"$set": {"isScheduled": True},
                 "$push": {"activeJobs": jobInstance}}
            )
        except Exception as e:
            print(f"Failed to schedule split job {i+1}: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Failed to schedule job: {str(e)}")


def schedule_recurring_job(command: dict, device_ids: List[str]) -> None:
    """Schedule the next day's task within the specified time window"""
    task_id = command.get("task_id")
    time_zone = command.get("timeZone", "UTC")
    user_tz = pytz.timezone(time_zone)

    # Get tomorrow's date
    now = datetime.now(user_tz)

    # Parse start and end times
    start_time_str = command.get("startInput")
    end_time_str = command.get("endInput")
    start_hour, start_minute = parse_time(start_time_str)
    end_hour, end_minute = parse_time(end_time_str)

    start_time = now.replace(
        hour=start_hour,
        minute=start_minute,
        second=0,
        microsecond=0
    )
    end_time = now.replace(
        hour=end_hour,
        minute=end_minute,
        second=0,
        microsecond=0
    )

    if start_time < now:
        start_time += timedelta(days=1)

    if end_time < start_time:
        end_time += timedelta(days=1)

    time_window_minutes = int((end_time - start_time).total_seconds() / 60)
    random_minutes = random.randint(
        0, time_window_minutes - int(command.get("duration", 0)))
    random_start_time = start_time + timedelta(minutes=random_minutes)

    start_time_utc = random_start_time.astimezone(pytz.UTC)

    new_job_id = f"cmd_{uuid.uuid4()}"
    modified_command = {
        **command,
        "job_id": new_job_id,
        "isRecurring": True
    }

    jobInstance = {
        "job_id": new_job_id,
        "startTime": start_time_utc,
        "device_ids": device_ids
    }

    try:
        job = scheduler.add_job(
            send_command_to_devices,
            trigger=DateTrigger(
                run_date=start_time_utc,
                timezone=pytz.UTC
            ),
            args=[device_ids, modified_command],
            id=new_job_id,
            name=f"Recurring random-time command for devices {device_ids}"
        )

        tasks_collection.update_one(
            {"id": task_id},
            {
                "$set": {"status": "scheduled"},
                "$push": {"activeJobs": jobInstance}
            }
        )

        print(f"Scheduled next day's task for {
              random_start_time} ({time_zone})")

    except Exception as e:
        print(f"Failed to schedule next day's job: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to schedule next day's job: {str(e)}")


def generate_random_durations(total_duration: int, min_duration: int = 30) -> List[int]:
    """
    Generate 2 to 4 random durations that sum up to the total duration.
    Each duration will be at least min_duration minutes.
    """
    num_durations = random.randint(
        2, 4)  # Limit the number of partitions to 2, 3, or 4

    if total_duration <= min_duration:
        return [total_duration]

    durations = []
    remaining = total_duration

    for _ in range(num_durations - 1):
        max_possible = min(remaining - min_duration,
                           remaining // (num_durations - len(durations)))
        if max_possible <= min_duration:
            break
        duration = random.randint(min_duration, max_possible)
        durations.append(duration)
        remaining -= duration

    durations.append(remaining)  # Add the remaining time to the last partition

    return durations


def get_random_start_times(
    start_time: datetime,
    end_time: datetime,
    durations: List[int],
    min_gap: float = 1.5
) -> List[datetime]:
    """
    Generate random start times for each duration, ensuring minimum gap between sessions.
    If end_time is less than or equal to start_time, end_time is considered the next day.
    Returns list of start times in chronological order.
    """

    # If end_time is less than or equal to start_time, treat it as next day's time
    if end_time <= start_time:
        end_time += timedelta(days=1)

    total_time_needed = sum(durations) + (len(durations) - 1) * min_gap
    available_time = (end_time - start_time).total_seconds() / \
        60  # Convert to minutes

    # Check if there is enough time in the window
    if total_time_needed > available_time:
        raise ValueError(
            f"Not enough time in the window for all sessions with minimum gaps")

    start_times = []
    current_time = start_time

    # Calculate maximum gap possible
    remaining_gaps = len(durations) - 1
    for i, duration in enumerate(durations):
        start_times.append(current_time)

        if remaining_gaps > 0:
            # Calculate the remaining time and the maximum possible gap
            time_left = (end_time - current_time).total_seconds() / 60
            time_needed = sum(durations[i+1:]) + remaining_gaps * min_gap
            max_gap = (time_left - time_needed) / remaining_gaps

            # Add a random gap after the session, but ensure it's at least min_gap
            gap = random.uniform(
                min_gap, max_gap) if max_gap > min_gap else min_gap
            current_time += timedelta(minutes=duration + gap)
            remaining_gaps -= 1
        else:
            # No more gaps to add, just adjust the current time
            current_time += timedelta(minutes=duration)

    return start_times








































# @device_router.post("/send_command")
# async def send_command(request: CommandRequest, current_user: dict = Depends(get_current_user)):
#     task_id = request.command.get("task_id")
#     command = request.command
#     print(command)
#     device_ids = request.device_ids
#     duration = int(command.get("duration", 0))
#     durationType = request.command.get("durationType")
#     # Default to UTC if not provided
#     time_zone = request.command.get("timeZone", "UTC")

#     try:
#         user_tz = pytz.timezone(time_zone)
#         now = datetime.now(user_tz)
#         print(f"Current time in {time_zone}: {now}")

#         scheduled_jobs = []

#         if durationType == 'DurationWithExactStartTime' or durationType == 'ExactStartTime':
#             print("Duration matched")
#             time_str = request.command.get("exactStartTime")
#             hour, minute = map(int, time_str.split(':'))

#             target_time = now.replace(
#                 hour=hour,
#                 minute=minute,
#                 second=0,
#                 microsecond=0
#             )

#             end_time_delta = timedelta(minutes=duration)
#             target_end_time = target_time + end_time_delta

#             # If target time is in the past, schedule for next day
#             if target_time < now:
#                 target_time = target_time + timedelta(days=1)
#                 target_end_time = target_end_time + timedelta(days=1)

#             # Convert to UTC for scheduling
#             target_time_utc = target_time.astimezone(pytz.UTC)
#             target_end_time_utc = target_end_time.astimezone(pytz.UTC)

#             clashes_check = check_for_Job_clashes(target_time_utc, target_end_time_utc, task_id, device_ids)
#             if clashes_check:
#                 print("returned true from clash check")
#                 return JSONResponse(content={"message": "Task already Scheduled on this time"}, status_code=400)

#             job_id = f"cmd_{uuid.uuid4()}"
#             jobInstance = {
#                 "job_id": job_id,
#                 "startTime": target_time_utc,
#                 "endTime": target_end_time_utc,
#                 "device_ids": device_ids
#             }

#             command['job_id'] = job_id
#             print(f"Scheduling details:")
#             print(f"Original time string: {time_str}")
#             print(f"Target time (local): {target_time}")
#             print(f"Target time (UTC): {target_time_utc}")
#             print(f"Target end time (UTC): {target_end_time_utc}")

#             try:
#                 # Schedule the job using the async version of send_command_to_devices
#                 job = scheduler.add_job(
#                     send_command_to_devices,
#                     trigger=DateTrigger(
#                         run_date=target_time_utc,
#                         timezone=pytz.UTC
#                     ),
#                     args=[device_ids, command],
#                     id=job_id,  # Unique job ID
#                     name=f"Command for devices {device_ids}"
#                 )

#                 tasks_collection.update_one(
#                     {"id": task_id},
#                     {"$set": {
#                         "status": "scheduled",
#                     },
#                         "$push": {
#                         "activeJobs": jobInstance
#                     }}
#                 )

#             except Exception as e:
#                 print(f"Failed to schedule job: {str(e)}")
#                 raise HTTPException(
#                     status_code=500, detail=f"Failed to schedule job: {str(e)}")

#         elif durationType == 'DurationWithTimeWindow':
#             # Get start and end times
#             # time_data = command.get("time", {})
#             start_time_str = command.get("startInput")
#             end_time_str = command.get("endInput")

#             # Parse start and end times
#             start_hour, start_minute = map(int, start_time_str.split(':'))
#             end_hour, end_minute = map(int, end_time_str.split(':'))

#             # Create datetime objects for start and end times
#             start_time = now.replace(
#                 hour=start_hour, minute=start_minute, second=0, microsecond=0)
#             end_time = now.replace(
#                 hour=end_hour, minute=end_minute, second=0, microsecond=0)

#             # If end time is before start time, add one day to end time
#             if end_time < start_time:
#                 end_time += timedelta(days=1)

#             # If start time is in the past, move both times to next day
#             if start_time < now:
#                 start_time += timedelta(days=1)
#                 end_time += timedelta(days=1)


#             clashes_check = check_for_Job_clashes(start_time, end_time, task_id, device_ids)
#             if clashes_check:
#                 print("clash detected")
#                 return JSONResponse(content={"message": "Task already Scheduled on this time"}, status_code=400)

#             # Calculate time window in minutes
#             time_window = (end_time - start_time).total_seconds() / 60

#             print(f"Time window: {time_window} minutes")
#             print(f"Requested duration: {duration} minutes")

#             # Check if window is close to duration
#             if abs(time_window - duration) <= 10:
#                 # Schedule single job for entire duration
#                 try:
#                     job_id = f"cmd_{uuid.uuid4()}"
#                     jobInstance = {
#                         "job_id": job_id,
#                         "startTime": start_time,
#                         "endTime": end_time,
#                         "device_ids": device_ids
#                     }
#                     command['job_id'] = job_id
#                     job = scheduler.add_job(
#                         send_command_to_devices,
#                         trigger=DateTrigger(
#                             run_date=start_time.astimezone(pytz.UTC),
#                             timezone=pytz.UTC
#                         ),
#                         args=[device_ids, {**command, "duration": duration}],
#                         id=job_id,
#                         name=f"Single session command for devices {device_ids}"
#                     )
#                     # scheduled_jobs.append(job)
#                     tasks_collection.update_one(
#                         {"id": task_id},
#                         {"$set": {
#                             "status": "scheduled",
#                         },
#                             "$push": {
#                             "activeJobs": jobInstance
#                         }}
#                     )
#                 except Exception as e:
#                     print(f"Failed to schedule single job: {str(e)}")
#                     raise HTTPException(
#                         status_code=500, detail=f"Failed to schedule job: {str(e)}")
#             else:
#                 # Generate random durations
#                 random_durations = generate_random_durations(duration)
#                 print(f"Split into durations: {random_durations}")

#                 # Generate random start times
#                 try:
#                     start_times = get_random_start_times(
#                         start_time, end_time, random_durations)

#                     # Schedule jobs for each duration
#                     for i, (start_time, duration) in enumerate(zip(start_times, random_durations)):
#                         modified_command = {**command, "duration": duration}
#                         job_id = f"cmd_{uuid.uuid4()}"
#                         end_time_delta = timedelta(minutes=duration)
#                         end_time = start_time + end_time_delta
#                         jobInstance = {
#                             "job_id": job_id,
#                             "startTime": start_time,
#                             "endTime": end_time,
#                             "device_ids": device_ids
#                         }
#                         modified_command['job_id'] = job_id
#                         job = scheduler.add_job(
#                             send_command_to_devices,
#                             trigger=DateTrigger(
#                                 run_date=start_time.astimezone(pytz.UTC),
#                                 timezone=pytz.UTC
#                             ),
#                             args=[device_ids, modified_command],
#                             id=job_id,
#                             name=f"Part {i+1} of split command for devices {device_ids}")
#                     # scheduled_jobs.append(job)
#                         tasks_collection.update_one(
#                             {"id": task_id},
#                             {"$set": {
#                                 "isScheduled": True,
#                             },
#                                 "$push": {
#                                 "activeJobs": jobInstance
#                             }}
#                         )
#                         print(f"Scheduled part {i+1}: {duration} minutes at {start_time}")
#                 except Exception as e:
#                     print(f"Failed to schedule split jobs: {str(e)}")
#                     raise HTTPException(
#                         status_code=500, detail=f"Failed to schedule jobs: {str(e)}")

#         elif durationType == 'EveryDayAutomaticRun':
#             # Get start and end times
#             start_time_str = command.get("startInput")
#             end_time_str = command.get("endInput")

#             # Parse start and end times
#             start_hour, start_minute = map(int, start_time_str.split(':'))
#             end_hour, end_minute = map(int, end_time_str.split(':'))

#             # Create datetime objects for start and end times
#             start_time = now.replace(
#                 hour=start_hour, minute=start_minute, second=0, microsecond=0)
#             end_time = now.replace(
#                 hour=end_hour, minute=end_minute, second=0, microsecond=0)

#             # If end time is before start time, it means the window crosses midnight
#             if end_time < start_time:
#                 end_time += timedelta(days=1)

#             # Calculate the time window in minutes
#             time_window = (end_time - start_time).total_seconds() / 60

#             # If start time is in the past, move to next day
#             if start_time < now:
#                 start_time += timedelta(days=1)
#                 end_time += timedelta(days=1)

#             # Generate a random start time within the window for today
#             random_minutes = random.randint(0, int(time_window))
#             first_run_time = start_time + timedelta(minutes=random_minutes)

#             # Convert to UTC for scheduling
#             first_run_time_utc = first_run_time.astimezone(pytz.UTC)

#             # Create a job ID for today's run
#             job_id = f"cmd_{uuid.uuid4()}"
#             modified_command = {**command, "duration": duration}

#             jobInstance = {
#                 "job_id": job_id,
#                 "startTime": first_run_time_utc,
#                 "device_ids": device_ids,
#             }

#             command['job_id'] = job_id
#             command['isRecurring'] = True
#             job = scheduler.add_job(
#                             send_command_to_devices,
#                             trigger=DateTrigger(
#                                 run_date=start_time.astimezone(pytz.UTC),
#                                 timezone=pytz.UTC
#                             ),
#                             args=[device_ids, modified_command],
#                             id=job_id,
#                             name=f"Command for devices {device_ids}")
#                     # scheduled_jobs.append(job)
#             tasks_collection.update_one(
#                             {"id": task_id},
#                             {"$set": {
#                                 "isScheduled": True,
#                             },
#                                 "$push": {
#                                 "activeJobs": jobInstance
#                             }}
#                         )

#         return {
#             "message": "Command scheduled successfully"
#         }

#     except pytz.exceptions.UnknownTimeZoneError:
#         raise HTTPException(
#             status_code=400, detail=f"Invalid timezone: {time_zone}")
#     except ValueError as e:
#         raise HTTPException(
#             status_code=400, detail=f"Invalid time format: {str(e)}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

# old
# @device_router.websocket("/ws/{device_id}")
# async def websocket_endpoint(websocket: WebSocket, device_id: str):
#     await websocket.accept()
#     device_connections[device_id] = websocket
#     active_connections.append(websocket)

#     try:
#         # Update device status to true when connected
#         # device_collection.update_one({"deviceId": device_id}, {"$set": {"status": True}})

#         while True:
#             data = await websocket.receive_text()
#             print(f"Message from {device_id}: {data}")
#             for connection in active_connections:
#                 if connection != websocket:
#                     await connection.send_text(f"Message from {device_id}: {data}")

#     except WebSocketDisconnect:
#         print(f"Device {device_id} disconnected.")
#         # Update device status to false when disconnected
#         device_collection.update_one({"deviceId": device_id}, {
#                                      "$set": {"status": False}})
#         active_connections.remove(websocket)
#         device_connections.pop(device_id, None)

# new
# @device_router.websocket("/ws/{device_id}")
# async def websocket_endpoint(websocket: WebSocket, device_id: str):
#     await websocket.accept()
#     device_connections[device_id] = websocket
#     active_connections.append(websocket)

#     try:
#         while True:
#             # Receive JSON message from client
#             data = await websocket.receive_text()
#             print(f"Message from {device_id}: {data}")

#             # Parse the JSON payload
#             try:
#                 payload = json.loads(data)
#                 message = payload.get("message")
#                 task_id = payload.get("task_id")
#                 job_id = payload.get("job_id")

#                 print(f"Parsed payload: message={message}, task_id={task_id}, job_id={job_id}")

#                 tasks_collection.update_one(
#                     {"id": task_id},
#                     {
#                         "$pull": {
#                             "activeJobs": {
#                                 "job_id": job_id
#                             }
#                         }
#                     }
#                 )

#                 task = tasks_collection.find_one({"id": task_id})

#                 if task:
#                     status = "awaiting" if len(task.get("activeJobs", [])) == 0 else "scheduled"
#                     tasks_collection.update_one(
#                         {"id": task_id},
#                         {"$set": {"status": status}}
#                     )


#             except json.JSONDecodeError:
#                 print(f"Invalid JSON received from {device_id}: {data}")
#     except WebSocketDisconnect:
#         print(f"Device {device_id} disconnected.")
#         # Update device status to false when disconnected
#         device_collection.update_one({"deviceId": device_id}, {
#                                      "$set": {"status": False}})
#         active_connections.remove(websocket)
#         device_connections.pop(device_id, None)


# with bot
