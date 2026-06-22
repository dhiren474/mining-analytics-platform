# api/Dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    fastapi==0.109.0 \
    uvicorn==0.27.0 \
    asyncpg==0.29.0 \
    sqlalchemy==2.0.25 \
    redis==5.0.1 \
    psycopg2-binary==2.9.9 \
    python-dotenv==1.0.0 \
    loguru==0.7.2 \
    httpx==0.26.0

COPY . .

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
