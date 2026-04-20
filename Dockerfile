FROM python:3.11-slim

# Install FFmpeg (optional but recommended for preview thumbnails)
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Expose the port your app runs on
EXPOSE 5050

# Command to run the app (use gunicorn for production)
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:5050", "--workers", "2", "--threads", "4", "--timeout", "120"]
