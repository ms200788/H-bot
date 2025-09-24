# Dockerfile for Render deployment
FROM python:3.11-slim

WORKDIR /app

# Install system deps (sqlite3 useful for debug)
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy project files
COPY . /app

# create data dir
RUN mkdir -p /data

ENV DB_PATH=/data/database.sqlite3
ENV JOB_DB_PATH=/data/jobs.sqlite
ENV PORT=10000
ENV PYTHONUNBUFFERED=1

CMD ["python", "bot.py"]