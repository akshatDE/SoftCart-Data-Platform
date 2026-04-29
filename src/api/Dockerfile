FROM python:3.12-slim

WORKDIR /app

# Install system deps for mysql-connector-python and psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    default-libmysqlclient-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code is mounted via volume in dev — no COPY needed
EXPOSE 8000

CMD ["uvicorn", "src.api.api_main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

