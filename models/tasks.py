from pydantic import BaseModel, Field, HttpUrl
from typing import Optional, List
from config.database import db
from bson import ObjectId


class taskModel(BaseModel):
    taskName: str
    bot: str


tasks_collection = db['tasks']
