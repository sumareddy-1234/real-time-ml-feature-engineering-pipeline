FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --default-timeout=1000 --retries 5 --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "producer.py"]