from email.mime.multipart import MIMEMultipart
import resend
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
      /* Global styles */
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
        background: linear-gradient(135deg, #5990ff, #0048b6);
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
        background-color: #1a1b21;
        color: #f3f4fa;
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
        background-color: #5990ff;
        color: #f3f4fa !important;
        border-radius: 8px;
        text-align: center;
        text-decoration: none;
        font-weight: bold;
        transition: background-color 0.3s ease;
      }}
      .cta-button:hover {{
        background-color: #80a9ff;
      }}
      .list-item {{
        background-color: #282c34;
        border-radius: 8px;
        padding: 10px 20px;
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
        <p>Please activate your account by clicking the link below:</p>
        <a href="https://console.appilot.app/verify-email/{token}" class="cta-button">Activate</a>
        <p>We currently offer two automation tools:</p>
        <div class="list-item">1. Reddit Karma Bot</div>
        <div class="list-item">2. Instagram Bot</div>
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
#####################################################


def send_password_email_email(to_email: str, token: str):
    subject = "Reset Password"
    body = f"""<!DOCTYPE html>
    <html>
  <head>
    <style>
      /* Global styles */
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
        background: linear-gradient(135deg, #5990ff, #0048b6);
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
        background-color: #1a1b21;
        color: #f3f4fa;
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
        background-color: #5990ff;
        color: #f3f4fa !important;
        border-radius: 8px;
        text-align: center;
        text-decoration: none;
        font-weight: bold;
        transition: background-color 0.3s ease;
      }}
      .cta-button:hover {{
        background-color: #80a9ff;
      }}
      .list-item {{
        background-color: #282c34;
        border-radius: 8px;
        padding: 10px 20px;
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
        <p>Please Reset your password by clicking the link below:</p>
        <a href="https://console.appilot.app/reset-password/{token}" class="cta-button">Reset</a>
        <p>We currently offer two automation tools:</p>
        <div class="list-item">1. Reddit Karma Bot</div>
        <div class="list-item">2. Instagram Bot</div>
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


def get_Running_Tasks(
    tasks: list[dict],
) -> list[dict]:
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