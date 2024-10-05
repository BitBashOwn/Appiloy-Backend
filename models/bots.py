from pydantic import BaseModel, Field, HttpUrl
from typing import Optional, List
from config.database import db

class FeatureModel(BaseModel):
    heading: str
    description: str

class FAQModel(BaseModel):
    question: str
    answer: str

class BotModel(BaseModel):
    id: Optional[str] = Field(None, alias="_id")  
    botName: str
    description: str
    noOfUsers: int
    os: List[str]  
    readme: str
    feature: List[FeatureModel]  
    demoLink: HttpUrl  
    DocumentationLink: HttpUrl  
    faqs: List[FAQModel]  
    issues: HttpUrl
    imagePath: str


bots_collection = db['bots']
