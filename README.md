# SeismicInsight

## Overview

SeismicInsight is a data processing pipeline for analyzing real-time and historical earthquake data using open datasets. The system combines stream processing with Apache Flink and batch processing with Apache Spark, storing the results in a time-series database connected to Grafana dashboards.

## Objectives

- Ingest real-time seismic events from public APIs
- Perform stream processing (filtering, windowing, enrichment)
- Analyze historical earthquake data in batch
- Store processed data in a time-series database
- Visualize trends, analytics, and live data using Grafana

## Architecture

- **Data Sources**: USGS Earthquake API (or other open seismic datasets)
- **Stream Processing**: Apache Flink
- **Batch Processing**: Apache Spark
- **Storage**: InfluxDB or TimescaleDB
- **Visualization**: Grafana

## Components

### 1. Streaming Pipeline (Apache Flink)

- Ingests real-time events
- Processes and filters seismic data
- Writes results to the time-series database

### 2. Batch Pipeline (Apache Spark)

- Loads historical seismic data (CSV, JSON, etc.)
- Computes aggregates and trends
- Stores output in the same database for unified visualization

### 3. Time-Series Database

- Stores both real-time and batch-processed data
- Acts as a source for Grafana dashboards

### 4. Dashboards (Grafana)

- Live event monitoring
- Historical trends and comparisons
- Aggregated insights
