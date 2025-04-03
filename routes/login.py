from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from models.users import user_collection
from utils.utils import create_access_token
from passlib.context import CryptContext
from datetime import timedelta
import os
from dotenv import load_dotenv

load_dotenv()

# Secret key and algorithm for JWT creation
secret_key = os.getenv('SECRET_KEY')
algo = os.getenv('ALGORITHM')

# Initialize FastAPI router
login_router = APIRouter()

# Initialize CryptContext for bcrypt
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Create a Pydantic model for the request body
class LoginRequest(BaseModel):
    email: str
    password: str


@login_router.post("/login")
async def login(request: LoginRequest):
    # Find the user by email
    existing_user = user_collection.find_one({"email": request.email})

    if existing_user and pwd_context.verify(request.password, existing_user["password"]):
    # if existing_user:
        user_id = str(existing_user["_id"])

        # Create a JWT token
        access_token = create_access_token(
            email=existing_user["email"],
            user_id=user_id
        )

        # Return JSON response
        return JSONResponse(content={
            "email": existing_user["email"],
            "id": user_id,
            "access_token": access_token,
            "token_type": "bearer"
        }, status_code=200)

    return JSONResponse(content={"message": "Invalid Email or Password"}, status_code=401)
