FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Rasa
RUN pip install --no-cache-dir rasa==3.6.20

COPY . /app

EXPOSE 5005

CMD ["rasa", "run", "--enable-api", "--cors", "*", "--port", "5005"]