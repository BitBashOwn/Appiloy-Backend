# gunicorn_conf.py
import multiprocessing
import os

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
accesslog = "-"  # Log to stdout
errorlog = "-"   # Log to stderr
capture_output = True
loglevel = "info"

# ============================================================================
# WORKER CONFIGURATION
# ============================================================================
worker_class = "uvicorn.workers.UvicornWorker"

# ⚠️ IMPORTANT: Fixed worker count to prevent memory exhaustion
# Formula (cpu_count * 2 + 1) can create too many workers on multi-core servers
# With 452 scheduled jobs + WebSockets + Redis + MongoDB, each worker is memory-intensive
# 
# Recommended: 4-8 workers for most production servers
# Override via environment: GUNICORN_WORKERS=6 gunicorn ...
workers = int(os.getenv("GUNICORN_WORKERS", "4"))

# Worker lifecycle management (prevents memory leaks)
max_requests = 1000  # Restart worker after 1000 requests
max_requests_jitter = 50  # Add randomness to avoid all workers restarting simultaneously

# ============================================================================
# TIMEOUT CONFIGURATION (Critical for preventing SIGKILL)
# ============================================================================
# Increased from 120s to 300s because:
# - Leader election can take 5-20 seconds
# - Loading 452 jobs from Redis takes time
# - Multiple workers starting simultaneously can slow things down
timeout = 300  # 5 minutes - worker must respond within this time

# Graceful shutdown timeout (time to finish current requests before hard kill)
graceful_timeout = 120  # 2 minutes

# ============================================================================
# CONNECTION CONFIGURATION
# ============================================================================
# Increased keepalive for WebSocket stability
keepalive = 75  # seconds (default 5 is too short for WebSockets)

# Max simultaneous connections per worker
worker_connections = 1000

# ============================================================================
# NETWORK CONFIGURATION
# ============================================================================
forwarded_allow_ips = "*"
proxy_allow_ips = "*"

# Bind configuration (when running directly, not via systemd)
# bind = "127.0.0.1:8000"  # Uncomment if not using systemd

# ============================================================================
# ENVIRONMENT CONFIGURATION
# ============================================================================
raw_env = [
    "PYTHONUNBUFFERED=1",  # Disable output buffering
]

# ============================================================================
# LIFECYCLE HOOKS (For monitoring and debugging)
# ============================================================================
def on_starting(server):
    """Called just before the master process is initialized"""
    server.log.info("=" * 80)
    server.log.info("🚀 Gunicorn master starting...")
    server.log.info(f"   Workers: {workers}")
    server.log.info(f"   Timeout: {timeout}s")
    server.log.info(f"   Worker class: {worker_class}")
    server.log.info("=" * 80)

def when_ready(server):
    """Called just after the server is started"""
    server.log.info("✅ Gunicorn master ready. Workers starting...")

def on_exit(server):
    """Called just before exiting Gunicorn"""
    server.log.info("👋 Gunicorn master exiting...")

def worker_int(worker):
    """Called when worker receives SIGINT or SIGQUIT"""
    worker.log.info(f"⚠️ Worker {worker.pid} received interrupt signal")

def worker_abort(worker):
    """Called when worker is killed (SIGKILL/timeout)"""
    worker.log.error(f"❌ Worker {worker.pid} was ABORTED!")
    worker.log.error(f"   Likely cause: Timeout ({timeout}s exceeded) or Out of Memory")
    worker.log.error(f"   Check: 1) Increase timeout, 2) Reduce workers, 3) Check memory usage")

def pre_fork(server, worker):
    """Called just before a worker is forked"""
    server.log.info(f"🔄 Forking worker...")

def post_fork(server, worker):
    """Called just after a worker has been forked"""
    server.log.info(f"✅ Worker {worker.pid} spawned")

def worker_exit(server, worker):
    """Called when a worker is about to exit"""
    server.log.info(f"💤 Worker {worker.pid} exiting")