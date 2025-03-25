# command_router.py
import json
import asyncio
import threading
import time
from connection_registry import redis_client, WORKER_ID, GLOBAL_COMMAND_CHANNEL

# Local connections managed by this worker
device_connections = {}


def set_device_connections(connections):
    """Link to the application's device connections dictionary"""
    global device_connections
    device_connections = connections


async def handle_local_command(device_id, command, request_id=None):
    """Execute a command on a locally connected device"""
    websocket = device_connections.get(device_id)
    result = {"success": False, "device_id": device_id, "request_id": request_id}

    if websocket:
        try:
            await websocket.send_text(json.dumps(command))
            result["success"] = True
        except Exception as e:
            print(f"Error sending to device {device_id}: {str(e)}")

    # Send acknowledgment if request_id is provided
    if request_id:
        redis_client.publish(f"command_response:{request_id}", json.dumps(result))

    return result["success"]


def publish_command(device_id, command):
    """Send a command to a device, regardless of which worker it's connected to"""
    request_id = f"req_{int(time.time() * 1000)}_{device_id}"

    # Prepare message with request ID for tracking
    message = {
        "device_id": device_id,
        "command": command,
        "request_id": request_id,
        "sender": WORKER_ID,
        "timestamp": int(time.time()),
    }

    # Publish to the global channel
    redis_client.publish(GLOBAL_COMMAND_CHANNEL, json.dumps(message))
    return request_id


async def send_commands_to_devices(device_ids, command):
    """Send a command to multiple devices with confirmation tracking"""
    if not device_ids:
        return {"success": [], "failed": []}

    batch_request_id = f"batch_{int(time.time() * 1000)}"
    pending_devices = set(device_ids)
    results = {"success": [], "failed": []}

    # Set up response listener
    response_channel = f"batch_response:{batch_request_id}"
    pubsub = redis_client.pubsub()
    pubsub.subscribe(response_channel)

    # Send command to each device
    for device_id in device_ids:
        message = {
            "device_id": device_id,
            "command": command,
            "batch_id": batch_request_id,
            "sender": WORKER_ID,
            "timestamp": int(time.time()),
        }
        redis_client.publish(GLOBAL_COMMAND_CHANNEL, json.dumps(message))

    # Wait for responses with timeout
    start_time = time.time()
    timeout = 5.0  # 5 second timeout

    while pending_devices and (time.time() - start_time) < timeout:
        message = pubsub.get_message(timeout=0.1)
        if message and message["type"] == "message":
            try:
                response = json.loads(message["data"])
                device_id = response.get("device_id")

                if device_id in pending_devices:
                    pending_devices.remove(device_id)

                    if response.get("success"):
                        results["success"].append(device_id)
                    else:
                        results["failed"].append(device_id)
            except:  # noqa: E722
                pass

        # Small delay to prevent CPU spinning
        await asyncio.sleep(0.01)

    # Any devices that didn't respond are marked as failed
    results["failed"].extend(list(pending_devices))

    # Clean up
    pubsub.unsubscribe()

    return results


def command_listener():
    """Background thread that listens for commands on the Redis channel"""
    pubsub = redis_client.pubsub()
    pubsub.subscribe(GLOBAL_COMMAND_CHANNEL)

    print(f"Worker {WORKER_ID} listening for device commands")

    for message in pubsub.listen():
        if message["type"] == "message":
            try:
                data = json.loads(message["data"])
                device_id = data.get("device_id")
                command = data.get("command")
                request_id = data.get("request_id")
                batch_id = data.get("batch_id")

                # Only process if this worker has the connection
                if device_id in device_connections:
                    # Create a new event loop for this thread
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                    try:
                        success = loop.run_until_complete(
                            handle_local_command(device_id, command, request_id)
                        )

                        # If part of a batch, send response to batch channel
                        if batch_id:
                            response = {
                                "device_id": device_id,
                                "success": success,
                                "batch_id": batch_id,
                            }
                            redis_client.publish(
                                f"batch_response:{batch_id}", json.dumps(response)
                            )
                    finally:
                        loop.close()
            except json.JSONDecodeError:
                print(f"Invalid JSON in command: {message['data']}")
            except Exception as e:
                print(f"Error processing command: {str(e)}")


def start_command_listener():
    """Start the background thread for listening to commands"""
    thread = threading.Thread(target=command_listener, daemon=True)
    thread.start()
    return thread
