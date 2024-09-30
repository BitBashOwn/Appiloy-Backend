import os
import pymongo 
from dotenv import load_dotenv
import pymongo.mongo_client

load_dotenv()

uri = os.getenv("db_uri")
if not uri:
    print("MongoDB URI not found in environment variables!")
    

client = pymongo.MongoClient(uri)

db = client.Appilot

