# command_router.py
import json
import asyncio
import threading
import time
import uuid
from connection_registry import redis_client, WORKER_ID, GLOBAL_COMMAND_CHANNEL

# Local connections managed by this worker
device_connections = {}

# ACK tracking infrastructure
ack_responses = {}  # Maps ack_id -> {"event": asyncio.Event(), "response": dict}


def set_device_connections(connections):
    """Link to the application's device connections dictionary"""
    global device_connections
    device_connections = connections


async def handle_local_command(device_id, command, request_id=None, ack_timeout: float = 10.0):
    """
    Execute a command on a locally connected device with ACK tracking.
    
    Args:
        device_id: Device ID to send command to
        command: Command dictionary to send
        request_id: Optional request ID for tracking
        ack_timeout: Timeout in seconds to wait for ACK (default 10.0)
    
    Returns:
        True if command sent and ACK received, False otherwise
    """
    websocket = device_connections.get(device_id)
    result = {"success": False, "device_id": device_id, "request_id": request_id}

    if not websocket:
        if request_id:
            redis_client.publish(f"command_response:{request_id}", json.dumps(result))
        return False

    # Generate ACK ID for tracking
    ack_id = str(uuid.uuid4())
    
    # Add ack_id to command payload
    command_with_ack = {**command, "ack_id": ack_id}
    
    # Create event for ACK response
    ack_event = asyncio.Event()
    ack_responses[ack_id] = {"event": ack_event, "response": None}
    
    try:
        # Send command
        await websocket.send_text(json.dumps(command_with_ack))
        result["success"] = True
        
        # Wait for ACK with timeout
        try:
            await asyncio.wait_for(ack_event.wait(), timeout=ack_timeout)
            ack_response = ack_responses[ack_id].get("response")
            
            if ack_response:
                result["success"] = ack_response.get("success", False)
                if not result["success"]:
                    error_msg = ack_response.get("error", "Unknown error")
                    print(f"Device {device_id} ACK indicated failure: {error_msg}")
            else:
                # ACK event fired but no payload was supplied; treat as failure
                result["success"] = False
        except asyncio.TimeoutError:
            print(f"ACK timeout for device {device_id} (ack_id: {ack_id})")
            result["success"] = False
        
    except Exception as e:
        print(f"Error sending to device {device_id}: {str(e)}")
        result["success"] = False
    finally:
        # Clean up ACK tracking
        ack_responses.pop(ack_id, None)

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
        # LOG: Verify command fields before sending to Redis
        print(f"[COMMAND-ROUTER-DEBUG] Sending to {device_id} - method: {command.get('method')}, dailyTarget: {command.get('dailyTarget')}, dayIndex: {command.get('dayIndex')}")
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
