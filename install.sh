sudo apt update;
sudo apt install docker.io;
docker run -d -p 6379:6379 redis;
python3 -m venv venv;
source venv/bin/activate;
pip install -r requirements.txt;
celery -A tasks worker --loglevel=info;