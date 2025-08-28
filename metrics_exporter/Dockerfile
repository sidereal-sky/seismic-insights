FROM python:3.11-slim

WORKDIR /app

COPY metrics_exporter/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY metrics_exporter/seismic_metrics_exporter.py .

EXPOSE 9101

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:9101/health || exit 1

CMD ["python", "seismic_metrics_exporter.py"]