# kafka/Dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    faker==22.0.0 \
    pydantic==2.5.0 \
    python-dotenv==1.0.0 \
    loguru==0.7.2 \
    psycopg2-binary==2.9.9

COPY . .

CMD ["python", "producer.py"]
