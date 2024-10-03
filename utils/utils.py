import smtplib
from fastapi import Request
from fastapi import Depends, HTTPException, status
from email.mime.text import MIMEText
from jose import jwt, JWTError
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordBearer
from models.users import User
from jose import JWTError, jwt
import os
from dotenv import load_dotenv

load_dotenv()

expire_time = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES'))
secret_key = os.getenv('SECRET_KEY')
algo = os.getenv('ALGORITHM')
email = os.getenv('EMAIL')
email_password = os.getenv('EMAIL_PASSWORD')
oauth2_bearer = OAuth2PasswordBearer(tokenUrl="token")


def create_confirmation_token(user: User):
    expire = datetime.utcnow() + timedelta(minutes=expire_time)
    user_data = user.dict()
    to_encode = {"data": user_data, "exp": expire}
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=algo)
    return encoded_jwt


def create_reset_password_token(user: User):
    expire = datetime.utcnow() + timedelta(minutes=expire_time)
    user_data = user.dict()
    to_encode = {"data": {"id": user_data.id,
                          "email": user_data.email}, "exp": expire}
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=algo)
    return encoded_jwt


def send_confirmation_email(to_email: str, token: str):
    subject = "Confirm your Email"
    body = f"Click the following link to confirm your email: http://localhost:5173/verify-email/{
        token}"

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = "appilot"
    msg['To'] = to_email

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(email, email_password)
        server.send_message(msg)


def send_password_email_email(to_email: str, token: str):
    subject = "Reset Password"
    body = f"Click the following link to resest your password: http://localhost:5173/reset-password/{
        token}"

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = "appilot"
    msg['To'] = to_email

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(email, email_password)
        server.send_message(msg)


def create_access_token(email: str, user_id: str, expires_delta: timedelta):
    # Set expiration time
    expire = datetime.utcnow() + expires_delta
    to_encode = {"sub": email, "id": user_id, "exp": expire}  # Payload data
    # Encode the token using JWT
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=algo)
    return encoded_jwt


async def get_current_user(request: Request):
    token = request.headers.get('Authorization')
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        # Decode the token
        payload = jwt.decode(token.replace("Bearer ", ""),
                             secret_key, algorithms=[algo])

        email: str = payload.get("sub")
        user_id: str = payload.get("id")

        if email is None or user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return {"email": email, "id": user_id}

    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
