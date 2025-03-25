# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import JSONResponse
# from routes.routes import router
# from routes.login import login_router
# from routes.devices import devices_router
# from routes.passwordReset import reset_router
# from routes.deviceRegistration import device_router
# from apscheduler.schedulers.background import BackgroundScheduler
# import uvicorn
# from routes.bots import bots_router
# from routes.tasks import tasks_router
# from Bot.discord_bot import bot_instance
# import asyncio
# from scheduler import scheduler

# app = FastAPI()

# allowed_origins = [
#     "http://localhost:5173",
#     "https://appilot-console.vercel.app/",
#     "https://appilot-console-4v67eq436-abdullahnoor-codes-projects.vercel.app/",
#     "https://appilot-console-git-main-abdullahnoor-codes-projects.vercel.app/"
# ]

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#         allow_methods=["*"],
#     allow_headers=["*"],
#     expose_headers=["Set-Cookie"]
# )

# @app.get("/")
# def index():
#     return JSONResponse(content={"message": "running"}, status_code=200)

# app.include_router(router)
# app.include_router(login_router)
# app.include_router(reset_router)
# app.include_router(devices_router)
# app.include_router(bots_router)
# app.include_router(tasks_router)
# app.include_router(device_router, tags=["Android endpoints"])

# #////////////////////////////////////
# # scheduler = BackgroundScheduler()
# scheduler.start()


# @app.on_event("startup")
# async def startup_event():
#     asyncio.create_task(bot_instance.start_bot())


# # for route in app.routes:
# #     print(f"Route: {route.path}, Methods: {route.methods if hasattr(route, 'methods') else 'WebSocket'}")

# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)


# main.py
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from routes.routes import router
from routes.login import login_router
from routes.devices import devices_router
from routes.passwordReset import reset_router
from routes.deviceRegistration import device_router
import uvicorn
from routes.bots import bots_router
from routes.tasks import tasks_router
from scheduler import scheduler
import asyncio
from connection_registry import (
    WORKER_ID,
    cleanup_stale_workers,
    release_discord_bot_lock,
)
from redis_client import get_redis_client
# from Bot.discord_bot import main as discord_bot_main
from Bot.discord_bot import bot_instance
from logger import logger

redis_client = get_redis_client()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Perform stale worker cleanup at startup of every worker
    logger.info(f"Starting application with worker ID: {WORKER_ID}")
    cleanup_stale_workers()

    logger.info("Starting scheduler...")
    scheduler.start()  # Start the scheduler
    logger.info("Started worker scheduler")
    
    asyncio.create_task(bot_instance.start_bot())

    # Only start the Discord bot if this worker acquires the lock
    # discord_bot_lock_key = "discord_bot_lock"
    # acquired = redis_client.set(discord_bot_lock_key, WORKER_ID, ex=60, nx=True)

    # discord_task = None
    # if acquired:
    #     logger.info(
    #         f"Worker {WORKER_ID} acquired Discord bot lock - starting Discord bot"
    #     )
    #     # Start Discord bot in a background task
    #     discord_task = asyncio.create_task(discord_bot_main())
    #     logger.info("Discord bot starting in background")

    #     # Refresh the lock periodically to maintain ownership
    #     async def refresh_discord_bot_lock():
    #         while True:
    #             try:
    #                 redis_client.expire(discord_bot_lock_key, 60)
    #                 await asyncio.sleep(30)  # Refresh every 30 seconds
    #             except Exception as e:
    #                 logger.error(f"Error refreshing Discord bot lock: {str(e)}")
    #                 await asyncio.sleep(5)  # Retry after a short delay

    #     # Start lock refresh task
    #     asyncio.create_task(refresh_discord_bot_lock())
    # else:
    #     logger.info(
    #         f"Worker {WORKER_ID} did not acquire Discord bot lock - another worker is running the bot"
    #     )

    yield

    # Cleanup code for shutdown
    logger.info(f"Shutting down worker {WORKER_ID}...")
    # Add code here to clean up this worker's devices on shutdown
    from connection_registry import cleanup_worker_devices

    cleanup_worker_devices()

    # Release Discord bot lock if we have it
    # if acquired:
    #     try:
    #         # Only delete the lock if we still own it
    #         current_owner = redis_client.get(discord_bot_lock_key)
    #         if current_owner == WORKER_ID:
    #             release_discord_bot_lock()
    #     except Exception as e:
    #         logger.error(f"Error releasing Discord bot lock: {str(e)}")

    # # Cancel Discord bot task if we started it
    # if discord_task:
    #     discord_task.cancel()
    #     try:
    #         await discord_task
    #     except asyncio.CancelledError:
    #         logger.info("Discord bot task cancelled")





app = FastAPI(lifespan=lifespan)


# Add request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Request: {request.method} {request.url.path}")
    response = await call_next(request)
    logger.info(
        f"Response: {request.method} {request.url.path} - Status: {response.status_code}"
    )
    return response


allowed_origins = [
    "http://localhost:5173",
    "https://appilot-console.vercel.app/",
    "https://appilot-console-4v67eq436-abdullahnoor-codes-projects.vercel.app/",
    "https://appilot-console-git-main-abdullahnoor-codes-projects.vercel.app/",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Using * for now, but consider restricting to allowed_origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Set-Cookie"],
)


@app.get("/")
def index():
    logger.info("Health check endpoint hit")
    return JSONResponse(content={"message": "running"}, status_code=200)


app.include_router(router, tags=["Signup endpoints"])
app.include_router(login_router, tags=["Login endpoints"])
app.include_router(reset_router, tags=["Reset endpoints"])
app.include_router(devices_router, tags=["devices endpoints"])
app.include_router(bots_router, tags=["bot endpoints"])
app.include_router(tasks_router, tags=["Task endpoints"])
app.include_router(device_router, tags=["Android endpoints"])

if __name__ == "__main__":
    logger.info("Starting application with Uvicorn...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
