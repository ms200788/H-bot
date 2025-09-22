FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY bot.py /app/bot.py

RUN mkdir -p /data

ENV DB_PATH=/data/database.sqlite3
ENV JOB_DB_PATH=/data/jobs.sqlite
ENV PORT=10000

CMD ["python", "bot.py"]
