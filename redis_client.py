# redis_client.py
import redis
import os

# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Initialize the Redis client only once
redis_client = redis.Redis(
    host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=None, decode_responses=True
)


def get_redis_client():
    return redis_client
