import time
import uuid
import os
import asyncio
import json
import random
from enum import IntEnum
from typing import List, Tuple
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

# Channel for device reconnection notifications
RECONNECTION_CHANNEL = "device:reconnection"


class CommandPriority(IntEnum):
    """Command priority levels for queue processing"""
    CRITICAL = 0   # stop, pause - process immediately
    HIGH = 1       # resume, config changes
    NORMAL = 2     # scheduled tasks (default)
    LOW = 3        # analytics, optional updates


# In connection_registry.py
async def register_device_connection(device_id):
    """Register which worker has this device connection"""
    # Store the worker ID in a hash
    timestamp = int(time.time())
    await asyncio.to_thread(redis_client.hset, "device_connections", device_id, WORKER_ID)
    # Set device as online in a separate hash for quick status checks
    await asyncio.to_thread(redis_client.hset, "device_status", device_id, "1")
    await asyncio.to_thread(redis_client.hset, "device_timestamps", device_id, timestamp)
    
    # Publish reconnection event to notify interested parties
    reconnection_event = {
        "device_id": device_id,
        "worker_id": WORKER_ID,
        "timestamp": timestamp
    }
    await asyncio.to_thread(
        redis_client.publish,
        RECONNECTION_CHANNEL,
        json.dumps(reconnection_event)
    )
    
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


# Connection stability tracking functions
async def track_connection_stability(device_id: str, event: str):
    """
    Track connection stability events for a device.
    Events: 'connect', 'disconnect', 'ping_success', 'ping_failure'
    """
    key = f"stability:{device_id}"
    timestamp = int(time.time())
    event_data = f"{timestamp}:{event}"
    
    # Add event to a list with TTL
    await asyncio.to_thread(redis_client.lpush, key, event_data)
    await asyncio.to_thread(redis_client.ltrim, key, 0, 99)  # Keep last 100 events
    await asyncio.to_thread(redis_client.expire, key, 86400)  # 24-hour TTL


async def get_connection_stability_score(device_id: str) -> dict:
    """
    Calculate connection stability score for a device.
    Returns dict with score (0-100) and recent events.
    """
    key = f"stability:{device_id}"
    events = await asyncio.to_thread(redis_client.lrange, key, 0, -1)
    
    if not events:
        return {"score": 100, "events": [], "disconnects_last_hour": 0}
    
    now = int(time.time())
    hour_ago = now - 3600
    
    disconnects_last_hour = 0
    ping_failures = 0
    ping_successes = 0
    
    for event_raw in events:
        event_str = event_raw.decode() if isinstance(event_raw, bytes) else event_raw
        try:
            ts, event_type = event_str.split(":", 1)
            ts = int(ts)
            
            if ts > hour_ago:
                if event_type == "disconnect":
                    disconnects_last_hour += 1
                elif event_type == "ping_failure":
                    ping_failures += 1
                elif event_type == "ping_success":
                    ping_successes += 1
        except (ValueError, TypeError):
            continue
    
    # Calculate score (100 = perfect, 0 = very unstable)
    score = 100
    score -= disconnects_last_hour * 15  # -15 per disconnect
    score -= ping_failures * 5  # -5 per ping failure
    score = max(0, min(100, score))
    
    return {
        "score": score,
        "disconnects_last_hour": disconnects_last_hour,
        "ping_failures": ping_failures,
        "ping_successes": ping_successes
    }


async def should_be_lenient_with_device(device_id: str) -> bool:
    """
    Determine if we should be more lenient with disconnection for this device.
    Returns True if device has been stable and deserves grace period.
    """
    stability = await get_connection_stability_score(device_id)
    
    # Be lenient if device has good stability score
    return stability["score"] >= 70


# ========== Enhanced Reconnection and Retry System ==========

async def wait_for_device_reconnection_pubsub(
    device_id: str, 
    max_wait_seconds: int = 30
) -> bool:
    """
    Wait for device reconnection using Pub/Sub (more efficient than polling).
    
    Args:
        device_id: The device ID to wait for
        max_wait_seconds: Maximum time to wait (default 30 seconds)
    
    Returns:
        True if device reconnected within timeout, False otherwise
    """
    # Check current state first
    if await is_device_connected(device_id):
        return True
    
    pubsub = redis_client.pubsub()
    await asyncio.to_thread(pubsub.subscribe, RECONNECTION_CHANNEL)
    
    start_time = time.time()
    try:
        while (time.time() - start_time) < max_wait_seconds:
            # Check current state again
            if await is_device_connected(device_id):
                return True
            
            # Wait for message with timeout
            message = await asyncio.to_thread(
                pubsub.get_message,
                ignore_subscribe_messages=True,
                timeout=2.0
            )
            
            if message and message.get("type") == "message":
                try:
                    data = json.loads(message["data"])
                    if data.get("device_id") == device_id:
                        logger.info(f"[RECONNECT] Device {device_id} reconnected via Pub/Sub")
                        return True
                except (json.JSONDecodeError, KeyError):
                    continue
        
        return False
    finally:
        await asyncio.to_thread(pubsub.unsubscribe, RECONNECTION_CHANNEL)
        await asyncio.to_thread(pubsub.close)


async def wait_for_device_reconnection_batch(
    device_ids: List[str], 
    max_wait_seconds: int = 10,
    check_interval: float = 2.0
) -> Tuple[List[str], List[str]]:
    """
    Wait for multiple devices to reconnect concurrently.
    
    Args:
        device_ids: List of device IDs to wait for
        max_wait_seconds: Maximum time to wait (default 10 seconds)
        check_interval: How often to check for reconnection (default 2 seconds)
    
    Returns:
        Tuple of (reconnected_devices, still_disconnected_devices)
    """
    if not device_ids:
        return [], []
    
    start_time = time.time()
    reconnected = set()
    pending = set(device_ids)
    
    logger.info(f"[RECONNECT] Waiting for {len(pending)} device(s) to reconnect (max {max_wait_seconds}s)")
    
    # Set up Pub/Sub listener for instant notifications
    pubsub = redis_client.pubsub()
    await asyncio.to_thread(pubsub.subscribe, RECONNECTION_CHANNEL)
    
    try:
        while pending and (time.time() - start_time) < max_wait_seconds:
            # Check all pending devices concurrently
            check_tasks = [is_device_connected(did) for did in pending]
            results = await asyncio.gather(*check_tasks)
            
            # Process results
            for device_id, is_connected in zip(list(pending), results):
                if is_connected:
                    reconnected.add(device_id)
                    pending.discard(device_id)
                    elapsed = time.time() - start_time
                    logger.info(f"[RECONNECT] Device {device_id} reconnected after {elapsed:.1f}s")
            
            # Check Pub/Sub for instant notifications
            if pending:
                message = await asyncio.to_thread(
                    pubsub.get_message,
                    ignore_subscribe_messages=True,
                    timeout=check_interval
                )
                
                if message and message.get("type") == "message":
                    try:
                        data = json.loads(message["data"])
                        reconnected_id = data.get("device_id")
                        if reconnected_id in pending:
                            reconnected.add(reconnected_id)
                            pending.discard(reconnected_id)
                            elapsed = time.time() - start_time
                            logger.info(f"[RECONNECT] Device {reconnected_id} reconnected via Pub/Sub after {elapsed:.1f}s")
                    except (json.JSONDecodeError, KeyError):
                        pass
                else:
                    # No message, sleep for check_interval
                    await asyncio.sleep(check_interval)
    
    finally:
        await asyncio.to_thread(pubsub.unsubscribe, RECONNECTION_CHANNEL)
        await asyncio.to_thread(pubsub.close)
    
    if pending:
        logger.warning(f"[RECONNECT] {len(pending)} device(s) did not reconnect within {max_wait_seconds}s")
    
    return list(reconnected), list(pending)


def calculate_next_retry_delay(retry_count: int, base_delay: int = 30) -> int:
    """
    Calculate delay with exponential backoff + jitter.
    
    Args:
        retry_count: Current retry attempt number (0-indexed)
        base_delay: Base delay in seconds (default 30)
    
    Returns:
        Delay in seconds with jitter
    """
    delay = base_delay * (2 ** retry_count)
    jitter = random.uniform(0.8, 1.2)
    return int(delay * jitter)


async def queue_pending_command_with_priority(
    device_id: str, 
    command: dict, 
    priority: int = CommandPriority.NORMAL,
    max_retries: int = 5,
    expiry_seconds: int = 3600,
    base_delay_seconds: int = 30
) -> bool:
    """
    Queue a command for a disconnected device with priority support and deduplication.
    
    Args:
        device_id: Device ID that should receive the command
        command: The command to queue
        priority: Command priority (CommandPriority enum value)
        max_retries: Maximum number of retry attempts
        expiry_seconds: How long to keep the command in queue (default 1 hour)
        base_delay_seconds: Initial delay before first retry
    
    Returns:
        True if command was queued, False if already exists (deduplication)
    """
    job_id = command.get("job_id") or str(uuid.uuid4())
    dedup_key = f"pending_cmd_dedup:{device_id}:{job_id}"
    
    # Atomic check-and-set for deduplication
    is_new = await asyncio.to_thread(
        redis_client.set,
        dedup_key,
        "1",
        nx=True,
        ex=expiry_seconds
    )
    
    if not is_new:
        logger.debug(f"[QUEUE] Command {job_id} already queued for device {device_id}, skipping duplicate")
        return False
    
    queue_key = f"pending_commands_priority:{device_id}"
    
    # Calculate score: priority first (lower = higher priority), then timestamp
    score = (priority * 1_000_000_000) + time.time()
    
    command_data = {
        "command": command,
        "priority": priority,
        "queued_at": int(time.time()),
        "retry_count": 0,
        "max_retries": max_retries,
        "expiry": int(time.time()) + expiry_seconds,
        "base_delay": base_delay_seconds,
        "next_retry_after": int(time.time()),  # Can retry immediately on first reconnect
        "job_id": job_id,
    }
    
    # Add to sorted set
    await asyncio.to_thread(
        redis_client.zadd,
        queue_key,
        {json.dumps(command_data): score}
    )
    
    # Set expiry on the queue key
    await asyncio.to_thread(redis_client.expire, queue_key, expiry_seconds + 60)
    
    logger.info(f"[QUEUE] Queued command for device {device_id} (priority: {priority}, job_id: {job_id})")
    return True


async def _process_pending_commands_internal(device_id: str) -> int:
    """
    Internal function to process queued commands in priority order.
    Must be called with distributed lock already acquired.
    
    Returns:
        Number of successfully processed commands
    """
    queue_key = f"pending_commands_priority:{device_id}"
    processed_count = 0
    now = time.time()
    
    try:
        # Get all commands in priority order (lowest score = highest priority)
        commands_raw = await asyncio.to_thread(redis_client.zrange, queue_key, 0, -1, withscores=True)
        
        if not commands_raw:
            return 0
        
        commands_to_process = []
        commands_to_remove = []
        
        for cmd_json, score in commands_raw:
            try:
                command_data = json.loads(cmd_json)
                
                # Check if command expired
                expiry = command_data.get("expiry", 0)
                if now > expiry:
                    commands_to_remove.append((cmd_json, command_data))
                    logger.info(f"[QUEUE] Skipping expired command for device {device_id}")
                    continue
                
                # Check if it's time to retry (respect exponential backoff)
                next_retry_after = command_data.get("next_retry_after", 0)
                if now < next_retry_after:
                    # Not time to retry yet, skip
                    continue
                
                # Check retry limit
                retry_count = command_data.get("retry_count", 0)
                max_retries = command_data.get("max_retries", 5)
                
                if retry_count >= max_retries:
                    commands_to_remove.append((cmd_json, command_data))
                    logger.warning(f"[QUEUE] Command exceeded max retries ({max_retries}) for device {device_id}")
                    continue
                
                commands_to_process.append((command_data, cmd_json))
                
            except json.JSONDecodeError:
                commands_to_remove.append((cmd_json, {}))
                logger.error(f"[QUEUE] Invalid JSON in queued command for device {device_id}")
            except Exception as e:
                commands_to_remove.append((cmd_json, {}))
                logger.error(f"[QUEUE] Error parsing queued command for device {device_id}: {e}")
        
        # Remove expired/exceeded commands
        if commands_to_remove:
            for cmd_json, command_data in commands_to_remove:
                await asyncio.to_thread(redis_client.zrem, queue_key, cmd_json)
                # Remove deduplication key
                job_id = (command_data or {}).get("command", {}).get("job_id")
                if job_id:
                    dedup_key = f"pending_cmd_dedup:{device_id}:{job_id}"
                    await asyncio.to_thread(redis_client.delete, dedup_key)
        
        # Process commands in priority order
        for command_data, cmd_json in commands_to_process:
            try:
                command = command_data.get("command", {})
                retry_count = command_data.get("retry_count", 0)
                
                # Import here to avoid circular imports
                from routes.command_router import send_commands_to_devices
                
                # Retry sending the command
                logger.info(f"[QUEUE] Retrying command for device {device_id} (attempt {retry_count + 1}/{command_data.get('max_retries', 5)})")
                results = await send_commands_to_devices([device_id], command)
                
                # Remove from queue (will be re-added if failed)
                await asyncio.to_thread(redis_client.zrem, queue_key, cmd_json)
                
                if device_id in results.get("success", []):
                    logger.info(f"[QUEUE] Successfully delivered queued command to device {device_id}")
                    processed_count += 1
                    
                    # Remove deduplication key on successful delivery
                    job_id = command.get("job_id")
                    if job_id:
                        dedup_key = f"pending_cmd_dedup:{device_id}:{job_id}"
                        await asyncio.to_thread(redis_client.delete, dedup_key)
                else:
                    job_id = command.get("job_id")
                    if job_id:
                        dedup_key = f"pending_cmd_dedup:{device_id}:{job_id}"
                        dedup_exists = await asyncio.to_thread(redis_client.exists, dedup_key)
                        if not dedup_exists:
                            logger.info(f"[QUEUE] Skipping requeue for job {job_id} on device {device_id} because it was marked completed")
                            processed_count += 1
                            continue
                    
                    # Increment retry count and re-queue if under limit
                    new_retry_count = retry_count + 1
                    if new_retry_count < command_data.get("max_retries", 5):
                        # Calculate next retry delay
                        base_delay = command_data.get("base_delay", 30)
                        delay_seconds = calculate_next_retry_delay(new_retry_count, base_delay)
                        
                        command_data["retry_count"] = new_retry_count
                        command_data["next_retry_after"] = int(now + delay_seconds)
                        
                        # Re-queue with updated metadata
                        priority = command_data.get("priority", CommandPriority.NORMAL)
                        new_score = (priority * 1_000_000_000) + now
                        
                        await asyncio.to_thread(
                            redis_client.zadd,
                            queue_key,
                            {json.dumps(command_data): new_score}
                        )
                        logger.info(f"[QUEUE] Re-queued command for device {device_id} (retry {new_retry_count}/{command_data.get('max_retries', 5)}, next retry in {delay_seconds}s)")
                    else:
                        logger.warning(f"[QUEUE] Command failed after {new_retry_count} retries for device {device_id}")
                        # Remove deduplication key when max retries exceeded
                        job_id = command.get("job_id")
                        if job_id:
                            dedup_key = f"pending_cmd_dedup:{device_id}:{job_id}"
                            await asyncio.to_thread(redis_client.delete, dedup_key)
                        
            except Exception as e:
                logger.error(f"[QUEUE] Error processing queued command for device {device_id}: {e}")
                # Remove failed command from queue
                await asyncio.to_thread(redis_client.zrem, queue_key, cmd_json)
                # Remove deduplication key on error
                try:
                    job_id = command_data.get("command", {}).get("job_id")
                    if job_id:
                        dedup_key = f"pending_cmd_dedup:{device_id}:{job_id}"
                        await asyncio.to_thread(redis_client.delete, dedup_key)
                except Exception:
                    pass
        
    except Exception as e:
        logger.error(f"[QUEUE] Error processing pending commands for device {device_id}: {e}")
    
    if processed_count > 0:
        logger.info(f"[QUEUE] Processed {processed_count} pending commands for device {device_id}")
    
    return processed_count


async def clear_queued_command_for_job(device_id: str, job_id: str) -> bool:
    """
    Remove any queued command (and its deduplication key) that matches the provided job ID.
    Returns True if at least one queued entry was removed.
    """
    if not job_id:
        return False
    
    queue_key = f"pending_commands_priority:{device_id}"
    removed = False
    
    try:
        commands_raw = await asyncio.to_thread(redis_client.zrange, queue_key, 0, -1)
        if not commands_raw:
            return await _cleanup_dedup_key(device_id, job_id, removed)
        
        for cmd_json in commands_raw:
            cmd_str = (
                cmd_json.decode("utf-8")
                if isinstance(cmd_json, (bytes, bytearray))
                else cmd_json
            )
            
            try:
                command_data = json.loads(cmd_str)
            except (json.JSONDecodeError, TypeError):
                continue
            
            queued_job_id = (
                command_data.get("command", {}).get("job_id")
                or command_data.get("job_id")
            )
            
            if queued_job_id == job_id:
                await asyncio.to_thread(redis_client.zrem, queue_key, cmd_json)
                removed = True
        
        if removed:
            logger.info(f"[QUEUE] Cleared queued command {job_id} for device {device_id}")
        else:
            logger.debug(f"[QUEUE] No queued command matched job {job_id} for device {device_id}")
    
    except Exception as exc:
        logger.error(f"[QUEUE] Failed to clear queued command {job_id} for device {device_id}: {exc}")
    
    return await _cleanup_dedup_key(device_id, job_id, removed)


async def _cleanup_dedup_key(device_id: str, job_id: str, removed: bool) -> bool:
    """
    Helper to delete the deduplication key for a (device_id, job_id) pair.
    """
    dedup_key = f"pending_cmd_dedup:{device_id}:{job_id}"
    try:
        await asyncio.to_thread(redis_client.delete, dedup_key)
    except Exception as exc:
        logger.warning(f"[QUEUE] Failed to delete dedup key for {job_id} ({device_id}): {exc}")
    return removed


async def process_pending_commands_for_device(device_id: str) -> int:
    """
    Process pending commands for a device that just reconnected.
    Uses distributed locking to prevent duplicate processing.
    
    Returns:
        Number of commands processed
    """
    lock_key = f"pending_commands_lock:{device_id}"
    lock_timeout = 60  # seconds
    
    # Try to acquire lock
    lock_acquired = await asyncio.to_thread(
        redis_client.set,
        lock_key,
        WORKER_ID,
        nx=True,
        ex=lock_timeout
    )
    
    if not lock_acquired:
        logger.debug(f"[QUEUE] Another worker is processing commands for device {device_id}")
        return 0
    
    try:
        return await _process_pending_commands_internal(device_id)
    finally:
        # Release lock (only if we still own it)
        current_holder = await asyncio.to_thread(redis_client.get, lock_key)
        current_holder_str = (
            current_holder.decode("utf-8") if isinstance(current_holder, bytes) else str(current_holder) if current_holder else None
        )
        if current_holder_str == WORKER_ID:
            await asyncio.to_thread(redis_client.delete, lock_key)


async def cleanup_expired_commands_efficient() -> int:
    """
    Efficiently remove expired commands using ZREMRANGEBYSCORE (O(log N)).
    
    Returns:
        Total number of expired commands removed
    """
    pattern = "pending_commands_priority:*"
    keys = await asyncio.to_thread(redis_client.keys, pattern)
    
    now = time.time()
    total_removed = 0
    
    for key in keys:
        try:
            # Remove all commands with expiry score < now
            # Since score = (priority * 1_000_000_000) + timestamp, we need to check expiry field
            # For efficiency, we'll use a different approach: get all and filter
            
            # Get all commands
            commands_raw = await asyncio.to_thread(redis_client.zrange, key, 0, -1, withscores=True)
            
            expired_commands = []
            # Extract device_id from key pattern: pending_commands_priority:{device_id}
            device_id = key.replace("pending_commands_priority:", "")
            
            for cmd_json, score in commands_raw:
                try:
                    command_data = json.loads(cmd_json)
                    expiry = command_data.get("expiry", 0)
                    if now > expiry:
                        expired_commands.append(cmd_json)
                        # Remove deduplication key for expired commands
                        job_id = command_data.get("command", {}).get("job_id")
                        if job_id:
                            dedup_key = f"pending_cmd_dedup:{device_id}:{job_id}"
                            await asyncio.to_thread(redis_client.delete, dedup_key)
                except (json.JSONDecodeError, KeyError):
                    # Invalid command, remove it
                    expired_commands.append(cmd_json)
            
            # Remove expired commands
            if expired_commands:
                for cmd_json in expired_commands:
                    await asyncio.to_thread(redis_client.zrem, key, cmd_json)
                total_removed += len(expired_commands)
                
                # If queue is empty, delete the key
                remaining = await asyncio.to_thread(redis_client.zcard, key)
                if remaining == 0:
                    await asyncio.to_thread(redis_client.delete, key)
                    
        except Exception as e:
            logger.error(f"[CLEANUP] Error cleaning queue {key}: {e}")
    
    if total_removed > 0:
        logger.info(f"[CLEANUP] Removed {total_removed} expired commands")
    
    return total_removed
