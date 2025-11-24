import time
import uuid
import os
import asyncio
from redis_client import get_redis_client
from logger import logger

# Get the singleton Redis client
redis_client = get_redis_client()

# Generate a unique worker ID
WORKER_ID = os.getenv("WORKER_ID", f"worker-{uuid.uuid4().hex[:8]}")

# Threshold (in seconds) before a device connection is considered stale
DEVICE_STALE_THRESHOLD_SECONDS = int(os.getenv("DEVICE_STALE_THRESHOLD_SECONDS", "300"))

# Channel for all workers to listen on
GLOBAL_COMMAND_CHANNEL = "device_commands"


# In connection_registry.py
async def register_device_connection(device_id):
    """Register which worker has this device connection"""
    # Store the worker ID in a hash
    timestamp = int(time.time())
    await asyncio.to_thread(redis_client.hset, "device_connections", device_id, WORKER_ID)
    # Set device as online in a separate hash for quick status checks
    await asyncio.to_thread(redis_client.hset, "device_status", device_id, "1")
    await asyncio.to_thread(redis_client.hset, "device_timestamps", device_id, timestamp)
    return True


async def unregister_device_connection(device_id):
    """Remove a device connection"""
    await asyncio.to_thread(redis_client.hdel, "device_connections", device_id)
    await asyncio.to_thread(redis_client.hdel, "device_status", device_id)
    return True


async def get_device_worker(device_id):
    """Find which worker has this device connected"""
    return await asyncio.to_thread(redis_client.hget, "device_connections", device_id)


async def is_device_connected(device_id):
    """Check if device is connected to any worker"""
    return await asyncio.to_thread(redis_client.hexists, "device_status", device_id)


async def get_connected_device_count():
    """Get count of connected devices"""
    return await asyncio.to_thread(redis_client.hlen, "device_status")


async def track_reconnection(device_id):
    """Track device reconnection attempts"""
    key = f"reconnections:{device_id}"
    count = await asyncio.to_thread(redis_client.incr, key)
    # Set expiry to reset count after 1 hour
    await asyncio.to_thread(redis_client.expire, key, 3600)
    return count


async def update_device_timestamp(device_id):
    """Update the last activity timestamp for a device"""
    timestamp = int(time.time())
    await asyncio.to_thread(redis_client.hset, "device_timestamps", device_id, timestamp)
    return True


async def mark_device_offline(device_id):
    """Mark a device as offline in MongoDB and Redis"""
    from models.devices import device_collection
    
    # Update MongoDB status
    await asyncio.to_thread(
        device_collection.update_one,
        {"deviceId": device_id},
        {"$set": {"status": False}},
    )
    
    # Unregister from Redis
    await unregister_device_connection(device_id)
    
    logger.info(f"[HEALTH] Marked device {device_id} as offline due to health check failure")
    return True


async def get_devices_for_this_worker():
    """Get all device IDs connected to this worker"""
    all_connections = await asyncio.to_thread(redis_client.hgetall, "device_connections")
    device_ids = []
    for device_id, worker_id in all_connections.items():
        # Redis returns bytes, so decode before comparing
        decoded_worker = (
            worker_id.decode("utf-8") if isinstance(worker_id, (bytes, bytearray)) else worker_id
        )
        decoded_device = (
            device_id.decode("utf-8") if isinstance(device_id, (bytes, bytearray)) else device_id
        )
        if decoded_worker == WORKER_ID:
            device_ids.append(decoded_device)
    return device_ids


async def get_all_connected_devices():
    """
    Get a dictionary of all connected devices with their worker IDs and connection timestamps.

    Returns:
        dict: A dictionary with device_id as key and a dict containing worker_id and timestamp as value.
    """
    result = {}

    # Get all device connections
    all_connections = await asyncio.to_thread(redis_client.hgetall, "device_connections")

    # Get all timestamps
    all_timestamps = await asyncio.to_thread(redis_client.hgetall, "device_timestamps")

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


async def log_all_connected_devices():
    """
    Log all connected devices with their worker IDs and connection times.
    """
    devices = await get_all_connected_devices()
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


async def cleanup_worker_devices():
    """
    Remove all devices associated with the current worker from Redis.
    """
    print(f"Cleaning up devices for worker: {WORKER_ID}")

    # Get all device connections
    all_connections = await asyncio.to_thread(redis_client.hgetall, "device_connections")

    # Iterate over the connections and remove devices linked to the current worker
    for device_id, worker_id in all_connections.items():
        if worker_id == WORKER_ID:
            print(f"Removing device {device_id} from worker {WORKER_ID}")
            await unregister_device_connection(device_id)


async def cleanup_stale_workers():
    try:
        # Create a lock key with short TTL to ensure only one worker performs cleanup
        lock_key = "worker_cleanup_lock"
        # Try to acquire the lock with a 10-second expiry (in case the process crashes)
        acquired = await asyncio.to_thread(redis_client.set, lock_key, WORKER_ID, ex=10, nx=True)

        if acquired:
            logger.info(
                f"Worker {WORKER_ID} acquired cleanup lock - performing stale worker cleanup"
            )

            # Log current state before cleanup
            logger.info("Current workers and devices before cleanup:")
            await log_all_connected_devices()

            # Get all device connections to identify worker IDs
            all_connections = await asyncio.to_thread(redis_client.hgetall, "device_connections")
            all_timestamps = await asyncio.to_thread(redis_client.hgetall, "device_timestamps")
            now_ts = int(time.time())

            # Extract unique worker IDs
            worker_ids = set()
            for device_id, worker_id in all_connections.items():
                decoded_worker = (
                    worker_id.decode("utf-8") if isinstance(worker_id, (bytes, bytearray)) else worker_id
                )
                worker_ids.add(decoded_worker)

            # For each worker, check if it's active based on device heartbeat timestamps
            for worker_id in worker_ids:
                # Skip current worker
                if worker_id == WORKER_ID:
                    continue

                logger.info(f"Cleaning up potentially stale connections for worker: {worker_id}")

                # Find all devices connected to this worker
                for device_id, w_id in all_connections.items():
                    decoded_device = (
                        device_id.decode("utf-8") if isinstance(device_id, (bytes, bytearray)) else device_id
                    )
                    decoded_wid = (
                        w_id.decode("utf-8") if isinstance(w_id, (bytes, bytearray)) else w_id
                    )

                    if decoded_wid != worker_id:
                        continue

                    timestamp_raw = all_timestamps.get(device_id)
                    is_stale = True
                    last_seen = "unknown"

                    if timestamp_raw:
                        try:
                            decoded_timestamp = (
                                timestamp_raw.decode("utf-8") if isinstance(timestamp_raw, (bytes, bytearray)) else timestamp_raw
                            )
                            ts_val = int(decoded_timestamp)
                            last_seen = ts_val
                            is_stale = (now_ts - ts_val) > DEVICE_STALE_THRESHOLD_SECONDS
                        except (ValueError, TypeError):
                            is_stale = True

                    if not is_stale:
                        logger.info(
                            f"[CLEANUP] Skipping active device {decoded_device} on worker {worker_id} "
                            f"(last activity {(now_ts - last_seen) if isinstance(last_seen, int) else 'recent'} seconds ago)"
                        )
                        continue

                    logger.info(
                        f"Removing stale connection for device {decoded_device} from worker {worker_id} "
                        f"(last activity {last_seen})."
                    )
                    await asyncio.to_thread(redis_client.hdel, "device_connections", decoded_device)
                    await asyncio.to_thread(redis_client.hdel, "device_status", decoded_device)
                    await asyncio.to_thread(redis_client.hdel, "device_timestamps", decoded_device)

            # Log after cleanup
            logger.info("Workers and devices after cleanup:")
            await log_all_connected_devices()

            logger.info("Stale worker cleanup completed successfully")
        else:
            logger.info(
                f"Worker {WORKER_ID} did not acquire cleanup lock - another worker is handling cleanup"
            )

    except Exception as e:
        logger.error(f"Error during stale worker cleanup: {str(e)}")
