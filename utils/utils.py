from email.mime.multipart import MIMEMultipart
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
import uuid

load_dotenv()

expire_time = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES'))
secret_key = os.getenv('SECRET_KEY')
algo = os.getenv('ALGORITHM')
email = os.getenv('EMAIL')
email_password = os.getenv('EMAIL_PASSWORD')
oauth2_bearer = OAuth2PasswordBearer(tokenUrl="token")


def generate_unique_id():
    return str(uuid.uuid4())


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


# def send_confirmation_email(to_email: str, token: str):
#     subject = "Confirm your Email"
#     body = f"Click the following link to confirm your email: http://localhost:5173/verify-email/{
#         token}"

#     msg = MIMEText(body)
#     msg['Subject'] = subject
#     msg['From'] = "appilot"
#     msg['To'] = to_email

#     with smtplib.SMTP("smtp.gmail.com", 587) as server:
#         server.starttls()
#         server.login(email, email_password)
#         server.send_message(msg)


# def send_password_email_email(to_email: str, token: str):
#     subject = "Reset Password"
#     body = f"Click the following link to resest your password: http://localhost:5173/reset-password/{
#         token}"

#     msg = MIMEText(body)
#     msg['Subject'] = subject
#     msg['From'] = "appilot"
#     msg['To'] = to_email

#     with smtplib.SMTP("smtp.gmail.com", 587) as server:
#         server.starttls()
#         server.login(email, email_password)
#         server.send_message(msg)
#####################################################
# def send_confirmation_email(to_email: str, token: str):
#     subject = "Confirm your Email"
#     body = f"Click the following link to confirm your email: https://console.appilot.app/verify-email/{token}"

#     msg = MIMEText(body)
#     msg['Subject'] = subject
#     msg['From'] = "appilot"
#     msg['To'] = to_email
#     with smtplib.SMTP("smtp.gmail.com", 587) as server:
#         server.starttls()
#         server.login(email, email_password)
#         server.send_message(msg)
#####################################################


def send_confirmation_email(to_email: str, token: str):
    # Email subject
    subject = "Confirm your Email"

    # HTML email body with styling
    body = f"""
    <html>
    <head>
        <style>
            /* Add custom styles here */
            body {{
                font-family: Arial, sans-serif;
                background-color: #f4f4f4;
                margin: 0;
                padding: 0;
            }}
            .container {{
                background-color: #ffffff;
                padding: 20px;
                margin: 0 auto;
                width: 600px;
                border-radius: 8px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }}
            h1 {{
                color: #333333;
            }}
            p {{
                color: #555555;
            }}
            a {{
                background-color: #1a73e8;
                color: white;
                padding: 10px 20px;
                text-decoration: none;
                border-radius: 5px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Confirm your Email</h1>
            <p>Thank you for signing up! Please confirm your email by clicking the button below:</p>
            <a href="https://console.appilot.app/verify-email/{token}">Confirm Email</a>
            <p>If you did not sign up, you can ignore this email.</p>
        </div>
    </body>
    </html>
    """

    # Create the email message container (MIMEMultipart)
    msg = MIMEMultipart("alternative")
    msg['Subject'] = subject
    msg['From'] = "appilot"
    msg['To'] = to_email

    # Attach the HTML body to the email
    html_body = MIMEText(body, "html")
    msg.attach(html_body)

    # Send the email
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(email, email_password)
        server.send_message(msg)


def send_password_email_email(to_email: str, token: str):
    subject = "Reset Password"
    body = f"Click the following link to reset your password: https://console.appilot.app/reset-password/{token}"

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
    # expire = datetime.utcnow() + expires_delta
    to_encode = {"sub": email, "id": user_id}  # Payload data
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
