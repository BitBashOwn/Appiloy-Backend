from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from routes.routes import router
from routes.login import login_router
from routes.devices import devices_router
from routes.passwordReset import reset_router
import os
import uvicorn

app = FastAPI()


# # Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Set to specific origins for security
    allow_credentials=True,            # Important for cookies
    allow_methods=["*"],               # Allow all methods
    allow_headers=["*"],               # Allow all headers
    expose_headers=["Set-Cookie"]      # Explicitly expose the Set-Cookie header
)

@app.get("/")
def index():
    return JSONResponse(content={"message": "running"}, status_code=200)

app.include_router(router)
app.include_router(login_router)
app.include_router(reset_router)
app.include_router(devices_router)

# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
