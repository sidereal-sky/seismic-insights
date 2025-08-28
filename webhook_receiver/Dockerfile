FROM python:3.11-slim

WORKDIR /app

COPY webhook_receiver/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY webhook_receiver/webhook_receiver.py .
COPY webhook_receiver/templates/ ./templates/

EXPOSE 5001

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5001/health || exit 1

CMD ["python", "webhook_receiver.py"]