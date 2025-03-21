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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting scheduler...")
    scheduler.start()  # Start the scheduler 
    logger.info("Started worker scheduler")
    
    yield
    
    # Cleanup code if needed
    logger.info("Shutting down application...")


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
    allow_origins=["*"],  
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