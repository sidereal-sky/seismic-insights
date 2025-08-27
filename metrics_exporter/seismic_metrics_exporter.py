#!/usr/bin/env python3

import os
import time
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta

from flask import Flask, Response
from influxdb_client import InfluxDBClient
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INFLUXDB_URL = os.getenv('INFLUXDB_URL', 'http://seismic-influxdb:8086')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', 'seismic-org')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', 'earthquakes')

app = Flask(__name__)

earthquake_magnitude = Gauge('earthquake_magnitude', 'Latest earthquake magnitude', ['region', 'depth_bin'])
earthquake_count_5min = Gauge('earthquake_count_5min', 'Number of earthquakes in last 5 minutes', ['region'])
earthquake_count_hourly = Gauge('earthquake_count_hourly', 'Hourly earthquake count', ['region'])
earthquake_swarm_detected = Gauge('earthquake_swarm_detected', 'Earthquake swarm detection flag', ['region'])
significant_event_count = Counter('significant_events_total', 'Total significant seismic events', ['region', 'magnitude_bin'])
sequence_detection_count = Gauge('earthquake_sequence_count', 'Number of earthquakes in detected sequences', ['region'])
regional_activity_level = Gauge('regional_seismic_activity', 'Regional seismic activity level', ['region'])
max_magnitude_24h = Gauge('max_earthquake_magnitude_24h', 'Maximum earthquake magnitude in last 24 hours', ['region'])
pipeline_last_update = Gauge('seismic_pipeline_last_update_timestamp', 'Last time seismic data was updated')
influxdb_connection_status = Gauge('influxdb_connection_status', 'InfluxDB connection status (1=connected, 0=disconnected)')
data_points_processed = Counter('seismic_data_points_processed_total', 'Total seismic data points processed')

class SeismicMetricsExporter:
    def __init__(self):
        self.client = None
        self.query_api = None
        self.connect_to_influxdb()
        
    def connect_to_influxdb(self):
        try:
            self.client = InfluxDBClient(
                url=INFLUXDB_URL,
                token=INFLUXDB_TOKEN,
                org=INFLUXDB_ORG
            )
            self.query_api = self.client.query_api()
            influxdb_connection_status.set(1)
            logger.info("Connected to InfluxDB successfully")
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")
            influxdb_connection_status.set(0)
            
    def query_latest_earthquakes(self) -> List[Dict[str, Any]]:
        try:
            query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: -5m)
              |> filter(fn: (r) => r["_measurement"] == "earthquakes")
              |> filter(fn: (r) => r["_field"] == "magnitude")
              |> keep(columns: ["_time", "_value", "region", "depth_bin", "latitude", "longitude"])
              |> sort(columns: ["_time"], desc: true)
            '''
            
            result = self.query_api.query(query)
            events = []
            
            for table in result:
                for record in table.records:
                    events.append({
                        'time': record.get_time(),
                        'magnitude': record.get_value(),
                        'region': record.values.get('region', 'unknown'),
                        'depth_bin': record.values.get('depth_bin', 'unknown'),
                        'latitude': record.values.get('latitude', 0),
                        'longitude': record.values.get('longitude', 0)
                    })
                    
            data_points_processed.inc(len(events))
            return events
            
        except Exception as e:
            logger.error(f"Error querying latest earthquakes: {e}")
            return []
    
    def query_hourly_stats(self) -> Dict[str, Any]:
        try:
            query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: -1h)
              |> filter(fn: (r) => r["_measurement"] == "hourly_global_stats")
              |> filter(fn: (r) => r["_field"] == "eq_count" or r["_field"] == "avg_mag" or r["_field"] == "max_mag")
              |> last()
            '''
            
            result = self.query_api.query(query)
            stats = {}
            
            for table in result:
                for record in table.records:
                    field = record.get_field()
                    region = record.values.get('region', 'global')
                    
                    if region not in stats:
                        stats[region] = {}
                    stats[region][field] = record.get_value()
                    
            return stats
            
        except Exception as e:
            logger.error(f"Error querying hourly stats: {e}")
            return {}
    
    def query_sequence_detections(self) -> List[Dict[str, Any]]:
        try:
            query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: -1h)
              |> filter(fn: (r) => r["_measurement"] == "sequence_detection")
              |> filter(fn: (r) => r["_field"] == "eq_count")
              |> filter(fn: (r) => r["_value"] >= 3)
              |> keep(columns: ["_time", "_value", "region", "max_mag", "avg_mag"])
            '''
            
            result = self.query_api.query(query)
            sequences = []
            
            for table in result:
                for record in table.records:
                    sequences.append({
                        'time': record.get_time(),
                        'count': record.get_value(),
                        'region': record.values.get('region', 'unknown'),
                        'max_mag': record.values.get('max_mag', 0),
                        'avg_mag': record.values.get('avg_mag', 0)
                    })
                    
            return sequences
            
        except Exception as e:
            logger.error(f"Error querying sequence detections: {e}")
            return []
    
    def query_24h_max_magnitude(self) -> Dict[str, float]:
        try:
            query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: -24h)
              |> filter(fn: (r) => r["_measurement"] == "earthquakes")
              |> filter(fn: (r) => r["_field"] == "magnitude")
              |> group(columns: ["region"])
              |> max()
            '''
            
            result = self.query_api.query(query)
            max_mags = {}
            
            for table in result:
                for record in table.records:
                    region = record.values.get('region', 'unknown')
                    max_mags[region] = record.get_value()
                    
            return max_mags
            
        except Exception as e:
            logger.error(f"Error querying 24h max magnitude: {e}")
            return {}
    
    def update_prometheus_metrics(self):
        try:
            logger.info("Updating Prometheus metrics from InfluxDB...")
            
            latest_events = self.query_latest_earthquakes()
            
            earthquake_count_5min._metrics.clear()
            earthquake_magnitude._metrics.clear()
            
            region_counts = {}
            latest_magnitudes = {}
            
            for event in latest_events:
                region = event['region']
                magnitude = event['magnitude']
                depth_bin = event['depth_bin']
                
                region_counts[region] = region_counts.get(region, 0) + 1
                
                if region not in latest_magnitudes or magnitude > latest_magnitudes[region]['magnitude']:
                    latest_magnitudes[region] = {'magnitude': magnitude, 'depth_bin': depth_bin}
            
            for region, count in region_counts.items():
                earthquake_count_5min.labels(region=region).set(count)
                
            for region, data in latest_magnitudes.items():
                earthquake_magnitude.labels(
                    region=region, 
                    depth_bin=data['depth_bin']
                ).set(data['magnitude'])
            
            hourly_stats = self.query_hourly_stats()
            for region, stats in hourly_stats.items():
                if 'eq_count' in stats:
                    earthquake_count_hourly.labels(region=region).set(stats['eq_count'])
            
            sequences = self.query_sequence_detections()
            earthquake_swarm_detected._metrics.clear()
            
            for seq in sequences:
                region = seq['region']
                count = seq['count']
                earthquake_swarm_detected.labels(region=region).set(1)
                sequence_detection_count.labels(region=region).set(count)
            
            max_mags_24h = self.query_24h_max_magnitude()
            max_magnitude_24h._metrics.clear()
            
            for region, max_mag in max_mags_24h.items():
                max_magnitude_24h.labels(region=region).set(max_mag)
            
            pipeline_last_update.set(time.time())
            
            logger.info(f"Updated metrics: {len(latest_events)} recent events, {len(hourly_stats)} regional stats, {len(sequences)} sequences detected")
            
        except Exception as e:
            logger.error(f"Error updating Prometheus metrics: {e}")
            influxdb_connection_status.set(0)

exporter = SeismicMetricsExporter()

@app.route('/metrics')
def metrics():
    try:
        exporter.update_prometheus_metrics()
        
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)
        
    except Exception as e:
        logger.error(f"Error generating metrics: {e}")
        return Response("# Error generating metrics\n", mimetype=CONTENT_TYPE_LATEST), 500

@app.route('/health')
def health():
    return {
        "status": "healthy",
        "service": "seismic-metrics-exporter",
        "influxdb_connected": bool(exporter.client),
        "timestamp": datetime.now().isoformat()
    }

@app.route('/')
def info():
    return {
        "service": "Seismic Metrics Exporter",
        "description": "Exports seismic data from InfluxDB as Prometheus metrics",
        "endpoints": {
            "GET /metrics": "Prometheus metrics",
            "GET /health": "Health check",
            "GET /": "This info"
        },
        "metrics_exposed": [
            "earthquake_magnitude",
            "earthquake_count_5min", 
            "earthquake_count_hourly",
            "earthquake_swarm_detected",
            "earthquake_sequence_count",
            "max_earthquake_magnitude_24h",
            "seismic_pipeline_last_update_timestamp"
        ]
    }

if __name__ == '__main__':
    logger.info("Starting Seismic Metrics Exporter...")
    logger.info(f"InfluxDB URL: {INFLUXDB_URL}")
    logger.info(f"InfluxDB Org: {INFLUXDB_ORG}")
    logger.info(f"InfluxDB Bucket: {INFLUXDB_BUCKET}")
    
    app.run(host='0.0.0.0', port=9101, debug=True)