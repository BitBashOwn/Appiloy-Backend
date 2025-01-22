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
    isScheduled: Optional[bool] = Field(default=False)
    activeJobs: List[dict] = Field(default_factory=list)
    inputs: Optional[Dict[str, Any]] = None
    LastModifiedDate: Optional[datetime] = None
    activationDate: Optional[datetime] = None
    deviceIds: Optional[List[str]] = Field(default_factory=list)
    serverId: Optional[str] = None
    channelId: Optional[str] = None
    


tasks_collection = db['tasks']
