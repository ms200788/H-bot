FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY bot.py /app/bot.py
ENV PYTHONIOENCODING=UTF-8
ENV LANG=C.UTF-8
ENV PORT=10000
CMD ["python", "bot.py"]