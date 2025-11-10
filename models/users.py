from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from config.database import db
from datetime import datetime

class User(BaseModel):
    # id: Optional[str] = Field(None, alias="_id")
    devicesToAutomate: Optional[int] = Field(0)  
    DiscordUsername: str
    email: EmailStr
    password: str
    subscription_tier: Optional[str] = Field("free")  # Add tier field with default 'free'
    createdAt: Optional[str] = None  # ISO timestamp string from frontend
    originCountryCode: Optional[str] = None  # Browser locale country code (e.g. "US")
    originCountryName: Optional[str] = None  # Browser locale country name (e.g. "United States")
    ipCountryCode: Optional[str] = None  # IP-detected country code (actual location)
    ipCountryName: Optional[str] = None  # IP-detected country name (actual location)
    
user_collection = db['Users']
