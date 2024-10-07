from pydantic import BaseModel, Field, HttpUrl, UUID4
from typing import Optional, List, Dict, Any
from config.database import db
from bson import ObjectId
from datetime import datetime


class taskModel(BaseModel):
    id: Optional[UUID4] = Field(
        None, description="Unique identifier generated as UUID")
    email: str
    taskName: str
    status: str
    bot: str
    inputs: Optional[Dict[str, Any]] = None
    LastModifiedDate: Optional[datetime] = None
    activationDate: Optional[datetime] = None


tasks_collection = db['tasks']
