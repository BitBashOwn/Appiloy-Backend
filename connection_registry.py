import time
import uuid
import os
from redis_client import get_redis_client
from logger import logger

# Get the singleton Redis client
redis_client = get_redis_client()

# Generate a unique worker ID
WORKER_ID = os.getenv("WORKER_ID", f"worker-{uuid.uuid4().hex[:8]}")

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


def get_all_connected_devices():
    """
    Get a dictionary of all connected devices with their worker IDs and connection timestamps.

    Returns:
        dict: A dictionary with device_id as key and a dict containing worker_id and timestamp as value.
    """
    result = {}

    # Get all device connections
    all_connections = redis_client.hgetall("device_connections")

    # Get all timestamps
    all_timestamps = redis_client.hgetall("device_timestamps")

    # Combine the information
    for device_id, worker_id in all_connections.items():
        timestamp = all_timestamps.get(device_id, 0)

        # Convert timestamp to human-readable format
        connection_time = (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(timestamp)))
            if timestamp
            else "Unknown"
        )

        # Add to result
        result[device_id] = {
            "worker_id": worker_id,
            "connected_since": connection_time,
            "timestamp": timestamp,
        }

    return result


def log_all_connected_devices():
    """
    Log all connected devices with their worker IDs and connection times.
    """
    devices = get_all_connected_devices()
    device_count = len(devices)

    print(f"===== {device_count} Connected Devices =====")
    print(f"Current worker: {WORKER_ID}")

    # Group devices by worker
    workers = {}
    for device_id, info in devices.items():
        worker_id = info["worker_id"]
        if worker_id not in workers:
            workers[worker_id] = []
        workers[worker_id].append((device_id, info["connected_since"]))

    # Print devices grouped by worker
    for worker_id, device_list in workers.items():
        is_current = " (current)" if worker_id == WORKER_ID else ""
        print(f"\nWorker: {worker_id}{is_current} - {len(device_list)} devices")

        for device_id, connected_since in device_list:
            print(f"  - {device_id}: connected since {connected_since}")

    print("\n===============================")

    return devices


def cleanup_worker_devices():
    """
    Remove all devices associated with the current worker from Redis.
    """
    print(f"Cleaning up devices for worker: {WORKER_ID}")

    # Get all device connections
    all_connections = redis_client.hgetall("device_connections")

    # Iterate over the connections and remove devices linked to the current worker
    for device_id, worker_id in all_connections.items():
        if worker_id == WORKER_ID:
            print(f"Removing device {device_id} from worker {WORKER_ID}")
            unregister_device_connection(device_id)


def cleanup_stale_workers():
    try:
        # Create a lock key with short TTL to ensure only one worker performs cleanup
        lock_key = "worker_cleanup_lock"
        # Try to acquire the lock with a 10-second expiry (in case the process crashes)
        acquired = redis_client.set(lock_key, WORKER_ID, ex=10, nx=True)

        if acquired:
            logger.info(
                f"Worker {WORKER_ID} acquired cleanup lock - performing stale worker cleanup"
            )

            # Log current state before cleanup
            logger.info("Current workers and devices before cleanup:")
            log_all_connected_devices()

            # Get all device connections to identify worker IDs
            all_connections = redis_client.hgetall("device_connections")

            # Extract unique worker IDs
            worker_ids = set()
            for device_id, worker_id in all_connections.items():
                worker_ids.add(worker_id)

            # For each worker, check if it's active (we'll consider all as stale during startup)
            for worker_id in worker_ids:
                # Skip current worker
                if worker_id == WORKER_ID:
                    continue

                logger.info(f"Cleaning up stale worker: {worker_id}")

                # Find all devices connected to this worker
                devices_to_remove = []
                for device_id, w_id in all_connections.items():
                    if w_id == worker_id:
                        devices_to_remove.append(device_id)

                # Remove each device
                for device_id in devices_to_remove:
                    logger.info(
                        f"Removing stale connection for device {device_id} from worker {worker_id}"
                    )
                    redis_client.hdel("device_connections", device_id)
                    redis_client.hdel("device_status", device_id)
                    redis_client.hdel("device_timestamps", device_id)

            # Log after cleanup
            logger.info("Workers and devices after cleanup:")
            log_all_connected_devices()

            logger.info("Stale worker cleanup completed successfully")
        else:
            logger.info(
                f"Worker {WORKER_ID} did not acquire cleanup lock - another worker is handling cleanup"
            )

    except Exception as e:
        logger.error(f"Error during stale worker cleanup: {str(e)}")
