from pydantic import BaseModel, Field
from typing import Optional, List
from config.database import db
from datetime import datetime


class User(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    deviceName: str
    model: str
    botName: List[str]
    status: bool
    activationDate: datetime


devices_collection = db['devices']
