FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 1. Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 2. Pin build tooling safely before installing dependencies
RUN pip install --no-cache-dir \
    "pip==23.3.2" \
    "setuptools==67.8.0" \
    "wheel==0.41.3"

# 3. Install your lean requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

ENV PORT=5005
EXPOSE 5005

CMD rasa run --enable-api --cors "*" --port ${PORT} -i 0.0.0.0