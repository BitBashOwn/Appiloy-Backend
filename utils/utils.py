from email.mime.multipart import MIMEMultipart
import resend
from typing import List
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
import pytz
from models.users import user_collection

load_dotenv()

expire_time = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES'))
secret_key = os.getenv('SECRET_KEY')
algo = os.getenv('ALGORITHM')
resend.api_key = os.getenv('RESEND_API_KEY')
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


#####################################################
def send_confirmation_email(to_email: str, token: str):
    subject = "Confirm your Email"
    body = f"""<!DOCTYPE html>
    <html>
  <head>
    <style>
      body {{
        font-family: 'Arial', sans-serif;
        background-color: #f0f2f5;
        margin: 0;
        padding: 0;
      }}
      .container {{
        max-width: 600px;
        margin: 40px auto;
        background-color: #ffffff;
        border-radius: 12px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
        overflow: hidden;
      }}
      .header {{
        background: linear-gradient(135deg, #80aafd, #4378e1);
        color: #ffffff;
        padding: 30px 20px;
        text-align: center;
      }}
      .header h2 {{
        margin: 0;
        font-size: 24px;
      }}
      .content {{
        padding: 30px;
        background-color: #bfc9da;
        color: #333;
      }}
      .content p {{
        line-height: 1.6;
        margin-bottom: 15px;
      }}
      .cta-button {{
        display: inline-block;
        padding: 12px 24px;
        margin: 20px 0;
        background: linear-gradient(135deg, #80aafd, #4378e1);
        color: #ffffff;
        border-radius: 8px;
        text-align: center;
        text-decoration: none;
        font-weight: bold;
        transition: background-color 0.3s ease;
      }}
      .cta-button:hover {{
        background-color: #7486bd;
      }}
    </style>
  </head>
  <body>
    <div class="container">
      <div class="header">
        <h2>Activate Your Appilot Account</h2>
      </div>
      <div class="content">
        <p>Hi <a href="mailto:{to_email}">{to_email}</a>,</p>
        <p>
          Thank you for signing up with Appilot! Please confirm your email
          address to activate your account.
        </p>
        <p>Click the button below to verify your email:</p>
        <a
          href="https://console.appilot.app/verify-email/{token}"
          class="cta-button"
          >Activate Account</a
        >
        <p>If you did not sign up for Appilot, please ignore this email.</p>
        <p>Thank you, <br />The Appilot Team</p>
      </div>
    </div>
  </body>
</html>
"""

    params: resend.Emails.SendParams = ({
        "from": "support@appilot.app",
        "to": [to_email],
        "subject": subject,
        "html": body
    })
    email: resend.Email = resend.Emails.send(params)
    return email
#####################################################


def send_password_email_email(to_email: str, token: str):
    subject = "Reset Password"
    body = f"""<!DOCTYPE html>
      <html>
  <head>
    <style>
      body {{
        font-family: 'Arial', sans-serif;
        background-color: #f0f2f5;
        margin: 0;
        padding: 0;
      }}
      .container {{
        max-width: 600px;
        margin: 40px auto;
        background-color: #ffffff;
        border-radius: 12px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
        overflow: hidden;
      }}
      .header {{
        background: linear-gradient(135deg, #80aafd, #4378e1);
        color: #ffffff;
        padding: 30px 20px;
        text-align: center;
      }}
      .header h2 {{
        margin: 0;
        font-size: 24px;
      }}
      .content {{
        padding: 30px;
        background-color: #bfc9da;
        color: #333;
      }}
      .content p {{
        line-height: 1.6;
        margin-bottom: 15px;
      }}
      .cta-button {{
        display: inline-block;
        padding: 12px 24px;
        margin: 20px 0;
        background: linear-gradient(135deg, #80aafd, #4378e1);
        color: #ffffff;
        border-radius: 8px;
        text-align: center;
        text-decoration: none;
        font-weight: bold;
        transition: background-color 0.3s ease;
      }}
      .cta-button:hover {{
        background-color: #7486bd;
      }}
    </style>
  </head>
  <body>
    <div class="container">
      <div class="header">
        <h2>Password Reset Request</h2>
      </div>
      <div class="content">
        <p>Hello,</p>
        <p>We received a request to reset your password for your Appilot account. If you made this request, click the button below to reset your password:</p>
        <a href="https://console.appilot.app/reset-password/{token}" class="cta-button">Reset Password</a>
        <p><strong>Note:</strong> This password reset link is valid for only 2 minutes. If it expires, you’ll need to request a new one.</p>
        <p>If you didn’t request a password reset, you can safely ignore this email.</p>
        <p>Thank you, <br>The Appilot Team</p>
        
      </div>
    </div>
  </body>
</html>
"""

    params: resend.Emails.SendParams = ({
        "from": "support@appilot.app",
        "to": [to_email],
        "subject": subject,
        "html": body
    })
    email: resend.Email = resend.Emails.send(params)
    return email
  
  
  
def send_account_creation_success_email(to_email: str):
    subject = "Account Created Successfully"
    body = f"""<!DOCTYPE html>
        <html lang="en">
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Welcome to Appilot</title>
    <style>
      body {{
        font-family: 'Arial', sans-serif;
        background-color: #f0f2f5;
        margin: 0;
        padding: 0;
      }}
      .container {{
        max-width: 600px;
        margin: 40px auto;
        background-color: #ffffff;
        border-radius: 12px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
        overflow: hidden;
      }}
      .header {{
        background: linear-gradient(135deg, #80aafd, #4378e1);
        color: #ffffff;
        padding: 30px 20px;
        text-align: center;
      }}
      .header h2 {{
        margin: 0;
        font-size: 24px;
      }}
      .content {{
        padding: 30px;
        background-color: #bfc9da;
        color: #333;
      }}
      .content p {{
        line-height: 1.6;
        margin-bottom: 15px;
      }}
      .content a {{
        color: #5990ff;
        text-decoration: none;
      }}
      .cta-button {{
        display: inline-block;
        padding: 12px 24px;
        margin: 20px auto;
        background: linear-gradient(135deg, #80aafd, #4378e1);
        color: #f3f4fa !important;
        border-radius: 8px;
        text-align: center;
        text-decoration: none;
        font-weight: bold;
        transition: background-color 0.3s ease;
      }}
      ul {{
        margin: 0;
        padding-left: 20px;
      }}
      ul li {{
        margin-bottom: 10px;
      }}
    </style>
  </head>
  <body>
    <div class="container">
      <div class="header">
        <h2>Welcome to Appilot! Your Account is Successfully Created</h2>
      </div>
      <div class="content">
        <p>Hello <a href="mailto:{to_email}">{to_email}</a>,</p>
        <p>Welcome to Appilot – your all-in-one automation suite for mobile devices!</p>
        <p>We currently offer two automation tools:</p>
        <ul>
          <li>Reddit Karma Bot</li>
          <li>Instagram Bot</li>
        </ul>
        <p>In addition, we provide custom freelance services for automating any social media app of your choice.</p>
        <p>
          Learn more about our services here:<br>
          <a href="https://appilot.app/professional-services">Freelance Services</a>
        </p>
        <h4>Getting Started:</h4>
        <p>
          Check out our onboarding videos and guides:<br>
          <a href="https://youtube.com/@appilot-app?si=y2C5CP4MsEcLzk7G">Video Guides</a>
        </p>
        <p>
          For detailed technical information, visit:<br>
          <a href="https://appilot.gitbook.io/appilot-docs/">Technical Documentation</a>
        </p>
        <p>
          If you need help, feel free to contact our support team or request a live demo:<br>
          <a href="https://appilot.app/contact-us">Support & Contact Us</a>
        </p>
        <p>
          Join our <strong>Discord community</strong> to stay updated, share ideas, and connect with other marketers:<br>
          <a href="https://discord.gg/3CZ5muJdF2">Join Discord</a>
        </p>
        <p>Thank you for choosing Appilot! By creating an account, you agree to our terms and conditions.</p>
        <p>Best regards,<br>The Appilot Team</p>
      </div>
    </div>
</body>
</html>

"""

    params: resend.Emails.SendParams = ({
        "from": "support@appilot.app",
        "to": [to_email],
        "subject": subject,
        "html": body
    })
    email: resend.Email = resend.Emails.send(params)
    return email
  
  
  
  
def create_access_token(email: str, user_id: str):
    to_encode = {"sub": email, "id": user_id}  # Payload data
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=algo)
    return encoded_jwt


async def get_current_user(request: Request):
        token = request.headers.get("Authorization")
        if not token:
          print("No Token")
          raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
          )
        print("Token: " + token)
        
        try:
          filtered_token = token.split("Bearer ")[1].split(";")[0]
          print("Filtered Token: " + filtered_token)
        # Decode the token without checking expiration
          payload = jwt.decode(filtered_token, secret_key, algorithms=[algo])
          email: str = payload.get("sub")
          user_id: str = payload.get("id")

          if email is None or user_id is None:
            print("No Email or password in token")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token",
                headers={"WWW-Authenticate": "Bearer"},
            )
          print("user_Id: " + user_id)
          print("Email: " + email)
          existing_user = user_collection.find_one({"email": email})

          if existing_user:
            print('valid Token')
            return {"email": email, "id": user_id}
          else:
            print('Email not in DB')
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token",
                headers={"WWW-Authenticate": "Bearer"},
            )
          

        except JWTError:
          raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

# def create_access_token(email: str, user_id: str, expires_delta: timedelta):
#     # Set expiration time
#     # expire = datetime.utcnow() + expires_delta
#     to_encode = {"sub": email, "id": user_id}  # Payload data
#     # Encode the token using JWT
#     encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=algo)
#     return encoded_jwt


# async def get_current_user(request: Request):
#     token = request.headers.get('Authorization')
#     if not token:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Not authenticated",
#             headers={"WWW-Authenticate": "Bearer"},
#         )

#     try:
#         # Decode the token
#         payload = jwt.decode(token.replace("Bearer ", ""),
#                              secret_key, algorithms=[algo])

#         email: str = payload.get("sub")
#         user_id: str = payload.get("id")

#         if email is None or user_id is None:
#             raise HTTPException(
#                 status_code=status.HTTP_401_UNAUTHORIZED,
#                 detail="Invalid token",
#                 headers={"WWW-Authenticate": "Bearer"},
#             )

#         return {"email": email, "id": user_id}

#     except JWTError:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Could not validate credentials",
#             headers={"WWW-Authenticate": "Bearer"},
#         )


def get_Running_Tasks(
    tasks: List[dict],
) -> List[dict]:
  current_Time = datetime.now(pytz.UTC)
  runningTasks = []
  for task in tasks:
    activeJobs = task.get("activeJobs")
    for job in activeJobs:
      startTime = job.get("startTime")
      endTime = job.get("endTime")
      if isinstance(startTime, str):
          startTime = datetime.fromisoformat(startTime)
      if isinstance(endTime, str):
          endTime = datetime.fromisoformat(endTime)

      if startTime and startTime.tzinfo is None:  # If it's naive, localize to UTC
        startTime = pytz.UTC.localize(startTime)
      if endTime and endTime.tzinfo is None:  # If it's naive, localize to UTC
        endTime = pytz.UTC.localize(endTime)
      if current_Time > startTime and current_Time < endTime:
        runningTasks.append(task)
  return runningTasks