# Kubernetes Deployment

## Kind
Create the cluster:
```bash
kind create cluster --config kind-config.yaml
kubectl cluster-info --context kind-seismic-insights
kubectl get nodes
```

## Docker Images
Build the required Docker images (for Batch Pipeline, Streaming Pipeline, Event Producer, Webhook Receiver, and Metrics Exporter):
```bash
docker compose build
```

Tag the images with the correct namespace format expected by Kubernetes deployments:
```bash
docker tag seismic-insights-batch-pipeline:latest seismic-insights/batch-pipeline:latest
docker tag seismic-insights-event-producer:latest seismic-insights/event-producer:latest
docker tag seismic-insights-streaming-pipeline:latest seismic-insights/streaming-pipeline:latest
docker tag seismic-insights-webhook-receiver:latest seismic-insights/webhook-receiver:latest
docker tag seismic-insights-metrics-exporter:latest seismic-insights/metrics-exporter:latest
```

Load the correctly tagged images into the Kind cluster:
```bash
kind load docker-image seismic-insights/batch-pipeline:latest --name seismic-insights
kind load docker-image seismic-insights/streaming-pipeline:latest --name seismic-insights
kind load docker-image seismic-insights/event-producer:latest --name seismic-insights
kind load docker-image seismic-insights/webhook-receiver:latest --name seismic-insights
kind load docker-image seismic-insights/metrics-exporter:latest --name seismic-insights
```

## Kubernetes
```bash
kubectl apply --recursive -f kubernetes
```

## External Access
After deployment, the following services are accessible via host ports (mapped from NodePorts):
- **Spark UI (Batch Pipeline)**: http://localhost:4040
- **Flink UI (Streaming Pipeline)**: http://localhost:8081
- **InfluxDB Admin**: http://localhost:8086
- **Grafana Dashboards**: http://localhost:3001
- **Prometheus Monitoring**: http://localhost:9090
- **Webhook Receiver (Alert Dashboard)**: http://localhost:5001
- **AlertManager**: http://localhost:9093

## Destroy the infrastructure
```bash
kind delete cluster --name seismic-insights
```
