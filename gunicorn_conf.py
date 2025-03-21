# gunicorn_conf.py
import multiprocessing

# Capture stderr and stdout and synchronize output
accesslog = "-"
errorlog = "-"
capture_output = True
loglevel = "info"
worker_class = "uvicorn.workers.UvicornWorker"

# Number of workers - adjust based on your server's capacity
workers = multiprocessing.cpu_count() * 2 + 1

# Ensure output is not buffered
forwarded_allow_ips = "*"
proxy_allow_ips = "*"

# Timeouts
timeout = 120
keepalive = 5

# Add environment variable to disable output buffering
raw_env = ["PYTHONUNBUFFERED=1"]