from fastapi import APIRouter
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from models.users import user_collection
from utils.utils import create_confirmation_token, send_password_email_email
from models.users import User
from jose import jwt, JWTError
from dotenv import load_dotenv
from passlib.hash import bcrypt
from bson import ObjectId
import os

load_dotenv()

secret_key = os.getenv('SECRET_KEY')
algo = os.getenv('ALGORITHM')


reset_router = APIRouter()


class ResetRequest(BaseModel):
    email: str


class updatedPassword(BaseModel):
    password: str


@reset_router.post('/reset-password')
def reset_password(request: ResetRequest):
    existing_user = user_collection.find_one({"email": request.email})
    if existing_user:
        user_model = User(**existing_user)  # Convert dict to Pydantic model
        confirmation_token = create_confirmation_token(user_model)
        send_password_email_email(request.email, confirmation_token)
        return JSONResponse(content={"message": "Please check your email to reset password."}, status_code=200)

    return JSONResponse(content={"message": "Invalid Email"}, status_code=401)


@reset_router.put('/update-password/')
async def update_password(token: str, password: updatedPassword):
    try:
        # Decode the JWT token
        payload = jwt.decode(token, secret_key, algorithms=[algo])
        data = payload.get("data")
        user_email = data.get("email")

        if user_email is None:
            return JSONResponse(content={"message": "Invalid Token"}, status_code=400)
        
        hashed_password = bcrypt.hash(password.password)  

        result = user_collection.update_one(
            {"email": user_email}, 
            {"$set": {"password": hashed_password}}
        )

        if result.matched_count == 0:
            return JSONResponse(content={"message": "User not found or invalid token"}, status_code=404)

        return JSONResponse(content={"message": "Password updated successfully", "data": {"email": user_email}}, status_code=200)

    except JWTError:
        return JSONResponse(content={"message": "Invalid or Expired Token"}, status_code=400)
