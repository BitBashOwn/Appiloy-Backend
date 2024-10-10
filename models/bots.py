from pydantic import BaseModel, Field, HttpUrl, UUID4
from typing import Optional, List, Dict, Any
from config.database import db


class FeatureModel(BaseModel):
    heading: str
    description: str


class FAQModel(BaseModel):
    question: str
    answer: str


class BotModel(BaseModel):
    id: Optional[UUID4] = Field(
        None, description="Unique identifier generated as UUID")
    botName: str
    description: str
    noOfUsers: int
    os: List[str]
    readme: str
    feature: List[FeatureModel]
    faqs: List[FAQModel]
    imagePath: str
    platform: str
    inputs: List[Dict[str, Any]]
    schedules: List[Dict[str, Any]]
    development: bool


bots_collection = db['bots']
