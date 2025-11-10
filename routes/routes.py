from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from models.users import User
from models.users import user_collection
from utils.utils import send_confirmation_email, create_confirmation_token, send_account_creation_success_email, get_country_from_ip
from jose import jwt, JWTError
from passlib.hash import bcrypt
import os
from dotenv import load_dotenv
from datetime import datetime
from logger import logger

load_dotenv()

secret_key = os.getenv('SECRET_KEY')
algo = os.getenv('ALGORITHM')


router = APIRouter()


@router.get("/")
async def get_users():
    return JSONResponse(content={"status": "running"})


@router.post("/signup")
async def sign_Up(user: User, request: Request):
    existing_user = user_collection.find_one({"email": user.email})
    if existing_user:
        # sending 404 response if user already exists
        return JSONResponse(content={"message": "Email already Exist"}, status_code=404)

    # Validate and normalize the new fields if provided
    if user.createdAt:
        try:
            # Validate that createdAt is a valid ISO timestamp
            datetime.fromisoformat(user.createdAt.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return JSONResponse(
                content={"message": "Invalid createdAt timestamp format. Expected ISO 8601 format."},
                status_code=400
            )
    
    # Validate country fields if provided
    if user.originCountryCode is not None:
        if not user.originCountryCode.strip():
            return JSONResponse(
                content={"message": "originCountryCode cannot be empty if provided."},
                status_code=400
            )
    
    if user.originCountryName is not None:
        if not user.originCountryName.strip():
            return JSONResponse(
                content={"message": "originCountryName cannot be empty if provided."},
                status_code=400
            )

    # ========== NEW: Detect country from IP address ==========
    client_ip = request.client.host if request.client else None
    
    # Check for real IP behind proxies (common in production with nginx/load balancers)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # X-Forwarded-For can contain multiple IPs, get the first one (original client)
        client_ip = forwarded_for.split(",")[0].strip()
    
    logger.info(f"🔍 Signup attempt from IP: {client_ip} | Email: {user.email}")
    
    # Detect country from IP
    ip_country_data = await get_country_from_ip(client_ip) if client_ip else {"countryCode": None, "countryName": None}
    
    logger.info(f"📍 IP Geolocation Result: {ip_country_data}")
    
    # Add IP-detected country to user object
    user.ipCountryCode = ip_country_data.get("countryCode")
    user.ipCountryName = ip_country_data.get("countryName")
    
    logger.info(f"💾 User data prepared: ipCountryCode={user.ipCountryCode}, ipCountryName={user.ipCountryName}")

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

        # ========== NEW LOGIC: Handle new fields from JWT token ==========
        # Normalize and set defaults for createdAt
        if data.get('createdAt'):
            # Keep the ISO string as-is if valid
            try:
                datetime.fromisoformat(data['createdAt'].replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                # Fallback to current timestamp if invalid
                data['createdAt'] = datetime.utcnow().isoformat() + 'Z'
        else:
            # Default to current timestamp if not provided by frontend
            data['createdAt'] = datetime.utcnow().isoformat() + 'Z'
        
        # Normalize browser locale country code to uppercase if provided
        if data.get('originCountryCode'):
            data['originCountryCode'] = data['originCountryCode'].strip().upper()
        else:
            # Set to None if not provided (will be null in DB)
            data['originCountryCode'] = None
        
        # Capitalize browser locale country name if provided
        if data.get('originCountryName'):
            data['originCountryName'] = data['originCountryName'].strip().title()
        else:
            # Set to None if not provided (will be null in DB)
            data['originCountryName'] = None
        
        # ========== NEW: Handle IP-detected country fields ==========
        # Normalize IP-detected country code to uppercase if provided
        if data.get('ipCountryCode'):
            data['ipCountryCode'] = data['ipCountryCode'].strip().upper()
        else:
            data['ipCountryCode'] = None
        
        # Capitalize IP-detected country name if provided
        if data.get('ipCountryName'):
            data['ipCountryName'] = data['ipCountryName'].strip().title()
        else:
            data['ipCountryName'] = None

        # Insert the user data into the database
        user_collection.insert_one(data)
        send_account_creation_success_email(email)
        return JSONResponse(content={"message": "Email confirmed successfully!"}, status_code=200)

    except JWTError:
        return JSONResponse(content={"message": "invalid or Expired Token "}, status_code=400)
