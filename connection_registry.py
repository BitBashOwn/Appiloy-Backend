# connection_registry.py
import time
import redis
import os
import uuid

# Configuration
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))
REDIS_DB = int(os.getenv('REDIS_DB'))

# Initialize Redis client
redis_client = redis.Redis(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    db=REDIS_DB,
    password=None,
    decode_responses=True
)

# Generate a unique worker ID
WORKER_ID = os.getenv('WORKER_ID', f"worker-{uuid.uuid4().hex[:8]}")

# Channel for all workers to listen on
GLOBAL_COMMAND_CHANNEL = "device_commands"

# In connection_registry.py
def register_device_connection(device_id):
    """Register which worker has this device connection"""
    # Store the worker ID in a hash
    timestamp = int(time.time())
    redis_client.hset("device_connections", device_id, WORKER_ID)
    # Set device as online in a separate hash for quick status checks
    redis_client.hset("device_status", device_id, "1")
    redis_client.hset("device_timestamps", device_id, timestamp)
    return True

def unregister_device_connection(device_id):
    """Remove a device connection"""
    redis_client.hdel("device_connections", device_id)
    redis_client.hdel("device_status", device_id)
    return True

def get_device_worker(device_id):
    """Find which worker has this device connected"""
    return redis_client.hget("device_connections", device_id)

def is_device_connected(device_id):
    """Check if device is connected to any worker"""
    return redis_client.hexists("device_status", device_id)

def get_connected_device_count():
    """Get count of connected devices"""
    return redis_client.hlen("device_status")


def track_reconnection(device_id):
    """Track device reconnection attempts"""
    key = f"reconnections:{device_id}"
    count = redis_client.incr(key)
    # Set expiry to reset count after 1 hour
    redis_client.expire(key, 3600)
    return count