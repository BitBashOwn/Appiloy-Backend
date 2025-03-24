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




# from contextlib import asynccontextmanager
# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import JSONResponse
# from routes.routes import router
# from routes.login import login_router
# from routes.devices import devices_router
# from routes.passwordReset import reset_router
# from routes.deviceRegistration import device_router
# import uvicorn
# from routes.bots import bots_router
# from routes.tasks import tasks_router
# from Bot.discord_bot import bot_instance
# import asyncio
# from scheduler import scheduler

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     print("Starting scheduler...")
#     scheduler.start()  # Start the scheduler 
#     print("started worker scheduler")
#     yield


# # Use the lifespan parameter when creating the FastAPI app
# app = FastAPI(lifespan=lifespan)

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
#     allow_methods=["*"],
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

# # Remove these as they're duplicates of what's in the lifespan
# # scheduler.start()
# @app.on_event("startup")
# async def startup_event():
#     try:
#         print("Starting Discord bot...")
#         asyncio.create_task(bot_instance.start_bot())
#         print("Discord bot started successfully.")
#     except Exception as e:
#         print(f"Error starting Discord bot: {e}")


    
# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)









# # main.py
# import logging
# from contextlib import asynccontextmanager
# from fastapi import FastAPI, Request
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import JSONResponse
# from routes.routes import router
# from routes.login import login_router
# from routes.devices import devices_router
# from routes.passwordReset import reset_router
# from routes.deviceRegistration import device_router
# import uvicorn
# from routes.bots import bots_router
# from routes.tasks import tasks_router
# from scheduler import scheduler

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
# )
# logger = logging.getLogger(__name__)

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     logger.info("Starting scheduler...")
#     scheduler.start()  # Start the scheduler 
#     logger.info("Started worker scheduler")
    
#     yield
    
#     # Cleanup code if needed
#     logger.info("Shutting down application...")



# # Use the lifespan parameter when creating the FastAPI app
# app = FastAPI(lifespan=lifespan)

# # Add request logging middleware
# @app.middleware("http")
# async def log_requests(request: Request, call_next):
#     logger.info(f"Request: {request.method} {request.url.path}")
#     response = await call_next(request)
#     logger.info(f"Response: {request.method} {request.url.path} - Status: {response.status_code}")
#     return response

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
#     allow_methods=["*"],
#     allow_headers=["*"],
#     expose_headers=["Set-Cookie"]
# )

# @app.get("/")
# def index():
#     logger.info("Health check endpoint hit")
#     return JSONResponse(content={"message": "running"}, status_code=200)

# app.include_router(router)
# app.include_router(login_router)
# app.include_router(reset_router)
# app.include_router(devices_router)
# app.include_router(bots_router)
# app.include_router(tasks_router)
# app.include_router(device_router, tags=["Android endpoints"])
    
# if __name__ == "__main__":
#     logger.info("Starting application with Uvicorn...")
#     uvicorn.run(app, host="0.0.0.0", port=8000)








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

# Import connection_registry for worker and device management
from connection_registry import redis_client, WORKER_ID, log_all_connected_devices

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Function to clean up stale workers in Redis - runs only once
def cleanup_stale_workers():
    try:
        # Create a lock key with short TTL to ensure only one worker performs cleanup
        lock_key = "worker_cleanup_lock"
        # Try to acquire the lock with a 10-second expiry (in case the process crashes)
        acquired = redis_client.set(lock_key, WORKER_ID, ex=10, nx=True)
        
        if acquired:
            logger.info(f"Worker {WORKER_ID} acquired cleanup lock - performing stale worker cleanup")
            
            # Log current state before cleanup
            logger.info("Current workers and devices before cleanup:")
            log_all_connected_devices()
            
            # Get all device connections to identify worker IDs
            all_connections = redis_client.hgetall("device_connections")
            
            # Extract unique worker IDs
            worker_ids = set()
            for device_id, worker_id in all_connections.items():
                worker_ids.add(worker_id)
            
            # For each worker, check if it's active (we'll consider all as stale during startup)
            for worker_id in worker_ids:
                # Skip current worker
                if worker_id == WORKER_ID:
                    continue
                
                logger.info(f"Cleaning up stale worker: {worker_id}")
                
                # Find all devices connected to this worker
                devices_to_remove = []
                for device_id, w_id in all_connections.items():
                    if w_id == worker_id:
                        devices_to_remove.append(device_id)
                
                # Remove each device
                for device_id in devices_to_remove:
                    logger.info(f"Removing stale connection for device {device_id} from worker {worker_id}")
                    redis_client.hdel("device_connections", device_id)
                    redis_client.hdel("device_status", device_id)
                    redis_client.hdel("device_timestamps", device_id)
            
            # Log after cleanup
            logger.info("Workers and devices after cleanup:")
            log_all_connected_devices()
            
            logger.info("Stale worker cleanup completed successfully")
        else:
            logger.info(f"Worker {WORKER_ID} did not acquire cleanup lock - another worker is handling cleanup")
    
    except Exception as e:
        logger.error(f"Error during stale worker cleanup: {str(e)}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Perform stale worker cleanup at startup
    logger.info(f"Starting application with worker ID: {WORKER_ID}")
    cleanup_stale_workers()
    
    logger.info("Starting scheduler...")
    scheduler.start()  # Start the scheduler 
    logger.info("Started worker scheduler")
    
    yield
    
    # Cleanup code for shutdown
    logger.info(f"Shutting down worker {WORKER_ID}...")
    # Add code here to clean up this worker's devices on shutdown
    from connection_registry import cleanup_worker_devices
    cleanup_worker_devices()

# Use the lifespan parameter when creating the FastAPI app
app = FastAPI(lifespan=lifespan)

# Add request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Request: {request.method} {request.url.path}")
    response = await call_next(request)
    logger.info(f"Response: {request.method} {request.url.path} - Status: {response.status_code}")
    return response

allowed_origins = [
    "http://localhost:5173",  
    "https://appilot-console.vercel.app/",  
    "https://appilot-console-4v67eq436-abdullahnoor-codes-projects.vercel.app/",  
    "https://appilot-console-git-main-abdullahnoor-codes-projects.vercel.app/"  
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Using * for now, but consider restricting to allowed_origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Set-Cookie"]
)

@app.get("/")
def index():
    logger.info("Health check endpoint hit")
    return JSONResponse(content={"message": "running"}, status_code=200)

app.include_router(router)
app.include_router(login_router)
app.include_router(reset_router)
app.include_router(devices_router)
app.include_router(bots_router)
app.include_router(tasks_router)
app.include_router(device_router, tags=["Android endpoints"])
    
if __name__ == "__main__":
    logger.info("Starting application with Uvicorn...")
    uvicorn.run(app, host="0.0.0.0", port=8000)