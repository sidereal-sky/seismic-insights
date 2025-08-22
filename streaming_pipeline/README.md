# Streaming Pipeline

This module contains the Apache Flink streaming pipeline for ingesting and processing real-time seismic events.

## Components
- `consumer.py`: Flink job that ingests, processes, and filters seismic events from Kafka.
	- Filters events (e.g., by magnitude)
	- Prints processed events (DB integration is a future step)

## Setup
See `requirements.txt` for dependencies.
