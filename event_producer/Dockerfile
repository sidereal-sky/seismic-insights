FROM python:3.11.9-slim

WORKDIR /event_producer

COPY event_producer/ /event_producer

RUN pip install --no-cache-dir -r /event_producer/requirements.txt

CMD ["python", "main.py"]
