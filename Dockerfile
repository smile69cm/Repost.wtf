# 1. Use stable Python 3.11
FROM python:3.11-slim

# 2. Install FFmpeg into the machine
RUN apt-get update && apt-get install -y ffmpeg

# 3. Copy your files into the machine
WORKDIR /app
COPY requirements.txt .

# 4. Install your Python packages
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# 5. Start the server using Render's automatic $PORT
CMD gunicorn app:app --bind 0.0.0.0:$PORT
