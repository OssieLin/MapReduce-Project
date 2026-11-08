FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY mapreduce ./mapreduce
ENV PYTHONUNBUFFERED=1 SHARED_DIR=/shared
