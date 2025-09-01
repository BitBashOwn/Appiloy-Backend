from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from config.database import db

class User(BaseModel):
    # id: Optional[str] = Field(None, alias="_id")
    devicesToAutomate: Optional[int] = Field(0)  
    DiscordUsername: str
    email: EmailStr
    password: str
    subscription_tier: Optional[str] = Field("free")  # Add tier field with default 'free'
    
user_collection = db['Users']
