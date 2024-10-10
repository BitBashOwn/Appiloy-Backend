from pydantic import BaseModel, Field, UUID4
from typing import Optional, List
from config.database import db
from datetime import datetime


class User(BaseModel):
    deviceId: Optional[UUID4] = Field(None, description="Unique identifier generated as UUID")
    deviceName: str
    email: str
    model: str
    botName: List[str]
    status: bool
    activationDate: datetime


devices_collection = db['devices']
