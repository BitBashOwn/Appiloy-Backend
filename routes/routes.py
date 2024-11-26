from fastapi import APIRouter
from fastapi.responses import JSONResponse
from models.users import User
from models.users import user_collection
from utils.utils import send_confirmation_email, create_confirmation_token,send_account_creation_success_email
from jose import jwt, JWTError
from passlib.hash import bcrypt
import os
from dotenv import load_dotenv

load_dotenv()

secret_key = os.getenv('SECRET_KEY')
algo = os.getenv('ALGORITHM')


router = APIRouter()


@router.get("/")
async def get_users():
    return JSONResponse(content={"status": "running"})


@router.post("/signup")
async def sign_Up(user: User):
    existing_user = user_collection.find_one({"email": user.email})
    if existing_user:
        # sending 404 response if user already exists
        return JSONResponse(content={"message": "Email already Exist"}, status_code=404)

    confirmation_token = create_confirmation_token(user)
    # Sending confirmation email
    send_confirmation_email(user.email, confirmation_token)
    # print("sending mail")

    return JSONResponse(content={"message": "Please check your email to confirm. Token expires in 2 minutes."})

# Endpoint to confirm email


@router.get("/confirm-email/")
async def confirm_email(token: str):
    try:
        payload = jwt.decode(token, secret_key, algorithms=[algo])
        data = payload.get("data")
        email = data.get("email")
        password = data.get("password")
        
        if email is None or password is None:
            return JSONResponse(content={"message": "invalid Token "}, status_code=400)        
        
        existing_user = user_collection.find_one({"email": email})

        if existing_user:
            return JSONResponse(content={"message": "Email already Created"}, status_code=200)

        # Hash the password and store it in the database
        hashed_password = bcrypt.hash(data["password"])
        data['password'] = hashed_password

        # Insert the user data into the database
        user_collection.insert_one(data)
        send_account_creation_success_email(email)
        return JSONResponse(content={"message": "Email confirmed successfully!"}, status_code=200)

    except JWTError:
        return JSONResponse(content={"message": "invalid or Expired Token "}, status_code=400)
