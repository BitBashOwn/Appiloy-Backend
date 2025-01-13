cd /var/www/Appilot

git reset --hard origin/main
git clean -f -d

git pull origin main

sudo systemctl restart fastapi.service
sudo systemctl reload nginx




# run command
# python -m uvicorn main:app --reload

# deploy commands
# cd /var/www/Appilot
# chmod +x deploy.sh
# sudo ./deploy.sh

# to see logs 
# sudo journalctl -u fastapi.service -f
