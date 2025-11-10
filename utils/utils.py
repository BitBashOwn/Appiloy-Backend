import resend
from typing import List
from fastapi import Request
from fastapi import HTTPException, status
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
from models.tasks import tasks_collection
import random
from Bot.discord_bot import bot_instance
import httpx
from logger import logger

load_dotenv()

expire_time = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES'))
secret_key = os.getenv('SECRET_KEY')
algo = os.getenv('ALGORITHM')
resend.api_key = os.getenv('RESEND_API_KEY')
oauth2_bearer = OAuth2PasswordBearer(tokenUrl="token")


def generate_unique_id():
    return str(uuid.uuid4())


async def get_country_from_ip(ip_address: str) -> dict:
    """
    Detect country from IP address using ip-api.com (free, no API key needed).
    Returns dict with countryCode and countryName, or None values if detection fails.
    
    Rate limit: 45 requests per minute from an IP address.
    """
    logger.info(f"🌍 Attempting IP geolocation for IP: {ip_address}")
    
    try:
        # Skip private/local IPs
        if ip_address in ["127.0.0.1", "localhost", "::1"] or ip_address.startswith(("192.168.", "10.", "172.")):
            logger.warning(f"⚠️ Skipping geolocation for private/local IP: {ip_address}")
            return {"countryCode": None, "countryName": None}
        
        async with httpx.AsyncClient(timeout=5.0) as client:
            logger.info(f"📡 Calling ip-api.com for IP: {ip_address}")
            response = await client.get(f"http://ip-api.com/json/{ip_address}?fields=status,country,countryCode,message")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✅ IP-API Response: {data}")
                
                if data.get("status") == "success":
                    country_code = data.get("countryCode")
                    country_name = data.get("country")
                    logger.info(f"✅ Detected country: {country_name} ({country_code})")
                    return {
                        "countryCode": country_code,
                        "countryName": country_name
                    }
                else:
                    logger.warning(f"⚠️ IP-API returned failure status: {data.get('message')}")
                    return {"countryCode": None, "countryName": None}
            else:
                logger.error(f"❌ IP-API returned status code: {response.status_code}")
                return {"countryCode": None, "countryName": None}
        
    except httpx.TimeoutException:
        logger.error(f"⏱️ IP geolocation timeout for IP: {ip_address}")
        return {"countryCode": None, "countryName": None}
    except Exception as e:
        logger.error(f"❌ Error detecting country from IP {ip_address}: {str(e)}")
        return {"countryCode": None, "countryName": None}


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
        color: #ffffff !important;
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
        color: #ffffff !important;
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
    print("token")
    print(token)
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




def check_for_Job_clashes(start_time: datetime, end_time: datetime, task_id: str, device_ids: List[str]) -> bool:
    task = tasks_collection.find_one(
        {"id": task_id}, {"_id": 0, "activeJobs": 1}
    )
    
    if not task or 'activeJobs' not in task:
        return False
    
    for active_job in task.get('activeJobs', []):
        devices_list = active_job.get('device_ids', [])
        
        if set(devices_list) & set(device_ids):
            job_start_time = active_job.get("startTime")
            job_end_time = active_job.get("endTime")
            
            if not job_start_time or not job_end_time:
                continue

            try:
                # Convert start_time and end_time to naive datetime for comparison
                start_time_naive = start_time.replace(tzinfo=None)
                end_time_naive = end_time.replace(tzinfo=None)
                
                if isinstance(job_start_time, str):
                    job_start_time = datetime.fromisoformat(job_start_time)
                if isinstance(job_end_time, str):
                    job_end_time = datetime.fromisoformat(job_end_time)
                
                if (start_time_naive >= job_start_time and start_time_naive <= job_end_time) or \
                   (end_time_naive >= job_start_time and end_time_naive <= job_end_time) or \
                   (start_time_naive <= job_start_time and end_time_naive >= job_end_time):
                    print("found clash")
                    return True
                    
            except ValueError as e:
                print(f"DateTime parsing error: {e}")
                continue
    print("no clash found")            
    return False
  
  
  
def split_message(message, max_length=1000):
    """Splits a long message into chunks of max_length characters without breaking lines."""
    chunks = []
    lines = message.split("\n")
    current_chunk = ""

    for line in lines:
        if len(current_chunk) + len(line) + 1 > max_length:
            chunks.append(current_chunk)
            current_chunk = line  # Start new chunk
        else:
            current_chunk += "\n" + line if current_chunk else line  # Append line
    
    if current_chunk:
        chunks.append(current_chunk)  # Append the last chunk
    
    return chunks



def get_random_start_times(
    start_time: datetime, end_time: datetime, durations: List[int], min_gap: float = 1.5
) -> List[datetime]:
    """
    Generate random start times for each duration, ensuring minimum gap between sessions.
    If end_time is less than or equal to start_time, end_time is considered the next day.
    Returns list of start times in chronological order.
    """

    # If end_time is less than or equal to start_time, treat it as next day's time
    if end_time <= start_time:
        end_time += timedelta(days=1)

    total_time_needed = sum(durations) + (len(durations) - 1) * min_gap
    available_time = (end_time - start_time).total_seconds() / 60  # Convert to minutes

    # Check if there is enough time in the window
    if total_time_needed > available_time:
        raise ValueError(
            "Not enough time in the window for all sessions with minimum gaps"
        )

    start_times = []
    current_time = start_time

    # Calculate maximum gap possible
    remaining_gaps = len(durations) - 1
    for i, duration in enumerate(durations):
        start_times.append(current_time)

        if remaining_gaps > 0:
            # Calculate the remaining time and the maximum possible gap
            time_left = (end_time - current_time).total_seconds() / 60
            time_needed = sum(durations[i + 1 :]) + remaining_gaps * min_gap
            max_gap = (time_left - time_needed) / remaining_gaps

            # Add a random gap after the session, but ensure it's at least min_gap
            gap = random.uniform(min_gap, max_gap) if max_gap > min_gap else min_gap
            current_time += timedelta(minutes=duration + gap)
            remaining_gaps -= 1
        else:
            # No more gaps to add, just adjust the current time
            current_time += timedelta(minutes=duration)

    return start_times



def generate_random_durations(total_duration: int, min_duration: int = 30) -> List[int]:
    """
    Generate 2 to 4 random durations that sum up to the total duration.
    Each duration will be at least min_duration minutes.
    """
    num_durations = random.randint(2, 4)  # Limit the number of partitions to 2, 3, or 4

    if total_duration <= min_duration:
        return [total_duration]

    durations = []
    remaining = total_duration

    for _ in range(num_durations - 1):
        max_possible = min(
            remaining - min_duration, remaining // (num_durations - len(durations))
        )
        if max_possible <= min_duration:
            break
        duration = random.randint(min_duration, max_possible)
        durations.append(duration)
        remaining -= duration

    durations.append(remaining)  # Add the remaining time to the last partition

    return durations


async def send_split_schedule_notification(
    task, device_names, start_times, durations, time_zone
):
    """Send a notification to Discord about split scheduled tasks."""
    if not task or not task.get("serverId") or not task.get("channelId"):
        print("Skipping notification. Missing serverId/channelId for task")
        return

    try:
        task_name = task.get("taskName", "Unknown Task")
        device_list = ", ".join(device_names) if device_names else "No devices"

        # Create summary of split sessions
        total_duration = sum(durations)
        session_count = len(start_times)
        timespan_start = min(start_times).strftime("%Y-%m-%d %H:%M")
        timespan_end = (max(start_times) + timedelta(minutes=durations[-1])).strftime(
            "%Y-%m-%d %H:%M"
        )

        message = (
            f"📅 **Split Task Scheduled**: {task_name}\n"
            f"⏰ **Timespan**: {timespan_start} to {timespan_end} ({time_zone})\n"
            f"🔢 **Sessions**: {session_count} sessions (total {total_duration} minutes)\n"
            f"🔌 **Devices**: {device_list}"
        )

        server_id = (
            int(task["serverId"])
            if isinstance(task["serverId"], str) and task["serverId"].isdigit()
            else task["serverId"]
        )
        channel_id = (
            int(task["channelId"])
            if isinstance(task["channelId"], str) and task["channelId"].isdigit()
            else task["channelId"]
        )

        # Send message to Discord
        await bot_instance.send_message(
            {
                "message": message,
                "task_id": task.get("id"),
                "job_id": f"split_{uuid.uuid4()}",  # Generate a unique ID for this notification
                "server_id": server_id,
                "channel_id": channel_id,
                "type": "info",
            }
        )
    except Exception as e:
        print(f"Error sending split schedule notification: {str(e)}")


def generate_random_durations_and_start_times(
    duration: int, start_time: datetime, end_time: datetime
) -> tuple:
    """Generate random durations and start times for split jobs."""
    random_durations = generate_random_durations(duration)
    start_times = get_random_start_times(start_time, end_time, random_durations)
    return random_durations, start_times


def parse_time(time_str: str) -> tuple:
    """Parse time string in 'HH:MM' format to a tuple of integers (hour, minute)."""
    hour, minute = map(int, time_str.split(":"))
    return hour, minute