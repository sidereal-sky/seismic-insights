FROM python:3.11.9-slim

RUN apt update -y && apt install -y openjdk-17-jdk openjdk-17-jre

WORKDIR /batch_pipeline

COPY batch_pipeline /batch_pipeline

COPY timeseries_db /timeseries_db

RUN pip install --no-cache-dir -r /batch_pipeline/requirements.txt

CMD ["python", "/batch_pipeline/main.py"]
