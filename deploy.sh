cd /var/www/Appilot

git reset --hard origin/main
git clean -f -d

git pull origin main

sudo systemctl restart fastapi.service
sudo systemctl reload nginx




# run command
# python -m uvicorn main:app --reload