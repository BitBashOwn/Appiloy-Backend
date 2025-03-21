cd /var/www/Appilot

git reset --hard origin/main
git clean -f -d

git pull origin main

sudo systemctl restart fastapi.service
sudo systemctl reload nginx




# run command
# python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000

# deploy commands
# cd /var/www/Appilot
# chmod +x deploy.sh
# sudo ./deploy.sh

# to see logs 
# sudo journalctl -u fastapi.service -f



# [Unit]
# Description=FastAPI application
# After=network.target

# [Service]
# User=root
# Group=www-data
# WorkingDirectory=/var/www/Appilot
# ExecStart=/var/www/Appilot/venv/bin/gunicorn main:app -w 3 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8000
# Restart=always

# [Install]
# WantedBy=multi-user.target













# fastAPi.service file:
# sudo nano /etc/systemd/system/fastapi.service

# [Unit]
# Description=FastAPI application
# After=network.target

# [Service]
# User=root
# Group=www-data
# WorkingDirectory=/var/www/Appilot
# ExecStart=/var/www/Appilot/venv/bin/uvicorn main:app --host 127.0.0.1 --port 8000
# Restart=always

# [Install]
# WantedBy=multi-user.target
