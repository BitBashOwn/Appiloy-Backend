


# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import JSONResponse
# from routes.routes import router
# from routes.login import login_router
# from routes.devices import devices_router
# from routes.passwordReset import reset_router
# from routes.bots import bots_router
# from routes.tasks import tasks_router

# app = FastAPI()

# # Add CORS middleware
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["http://localhost:5173"],  # Frontend URL
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
#     expose_headers=["Set-Cookie"]  # Explicitly expose Set-Cookie if using cookies
# )

# @app.get("/")
# def index():
#     return JSONResponse(content={"message": "running"}, status_code=200)

# # Register routers after setting up CORS middleware
# app.include_router(router)
# app.include_router(login_router)
# app.include_router(reset_router)
# app.include_router(devices_router)
# app.include_router(bots_router)
# app.include_router(tasks_router)











from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from routes.routes import router
from routes.login import login_router
from routes.devices import devices_router
from routes.passwordReset import reset_router
from routes.deviceRegistration import device_router
from apscheduler.schedulers.background import BackgroundScheduler
import uvicorn
from routes.bots import bots_router
from routes.tasks import tasks_router

app = FastAPI()

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
    return JSONResponse(content={"message": "running"}, status_code=200)

app.include_router(router)
app.include_router(login_router)
app.include_router(reset_router)
app.include_router(devices_router)
app.include_router(bots_router)
app.include_router(tasks_router)
app.include_router(device_router, tags=["Android endpoints"])  # Added by "Sohaib"


#////////////////////////////////////
scheduler = BackgroundScheduler()

# Event to start the scheduler when the app starts
@app.on_event("startup")
async def start_scheduler():
    print("Starting scheduler...")
    scheduler.start()
    print("Scheduler started successfully.")

# Event to shut down the scheduler when the app stops
@app.on_event("shutdown")
async def shutdown_scheduler():
    print("Shutting down scheduler...")
    scheduler.shutdown()
    print("Scheduler shutdown complete.")
    
#//////////////////////////////////////


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)