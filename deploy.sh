# cd /var/www/Appilot

# git reset --hard origin/main
# git clean -f -d

# git pull origin main

# sudo systemctl restart fastapi.service
# sudo systemctl reload nginx



cd /var/www/Appilot

git reset --hard origin/main
git clean -f -d

git pull origin main

# Add this line to reload systemd configuration
sudo systemctl daemon-reload

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






# old nginx configuration

# server {
#     server_name server.appilot.app;

#     location / {
#         proxy_pass http://127.0.0.1:8000;  # Adjust port if your FastAPI app runs on a different port
#         proxy_http_version 1.1;  # Important for WebSockets
#         proxy_set_header Upgrade $http_upgrade;  # Support for WebSocket upgrade
#         proxy_set_header Connection "upgrade";  # Keep the connection open for WebSockets
#         proxy_set_header Host $host;
#         proxy_set_header X-Real-IP $remote_addr;
#         proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
#         proxy_set_header X-Forwarded-Proto $scheme;
#         proxy_redirect off;
#     }

#     listen 443 ssl; # managed by Certbot
#     ssl_certificate /etc/letsencrypt/live/server.appilot.app/fullchain.pem; # managed by Certbot
#     ssl_certificate_key /etc/letsencrypt/live/server.appilot.app/privkey.pem; # managed by Certbot
#     include /etc/letsencrypt/options-ssl-nginx.conf; # managed by Certbot
#     ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem; # managed by Certbot
# }

# server {
#     if ($host = server.appilot.app) {
#         return 301 https://$host$request_uri;
#     } # managed by Certbot

#     listen 80;
#     server_name server.appilot.app;
#     return 404; # managed by Certbot
# }










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
