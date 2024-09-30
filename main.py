from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from routes.routes import router
from routes.login import login_router
from routes.devices import devices_router
from routes.passwordReset import reset_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173","https://appilot-console.vercel.app/"],  # Frontend URL
    allow_credentials=True,  # Important for cookies
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Set-Cookie"]  # Explicitly expose the Set-Cookie header
)

@app.get("/")
def index():
    return JSONResponse(content={"message":"running"}, status_code=200)

app.include_router(router)
app.include_router(login_router)
app.include_router(reset_router)
app.include_router(devices_router)