import logging
from datetime import datetime
from typing import Dict, Any
from .client import SeismicInfluxDBClient

from .models import (
    EarthquakeEvent,
    StreamingGlobalStats,
    StreamingRegionalStats,
    StreamingSignificantAlert,
    StreamingMagnitudeDistribution,
    StreamingDepthPattern,
    SequenceDetection
)

if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO)

class FlinkInfluxDBAdapter:

    def __init__(self, url: str, token: str, org: str, bucket: str, timeout: int = 30000):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.timeout = timeout

    def write_raw_events(self, stream):
        """Writes a PyFlink DataStream of raw earthquake events."""
        def sink_event(event: Dict[str, Any]):
            try:
                logging.info(f"Writing raw event to InfluxDB: {event['id']}")

                # Parse timestamp from string to datetime if needed
                if isinstance(event["event_time"], str):
                    from datetime import datetime
                    timestamp = datetime.fromisoformat(event["event_time"].replace('Z', '+00:00'))
                else:
                    timestamp = event["event_time"]

                point = EarthquakeEvent(
                    event_id=event["id"],
                    timestamp=timestamp,
                    latitude=float(event["latitude"]),
                    longitude=float(event["longitude"]),
                    magnitude=float(event["mag"]),
                    depth=float(event.get("depth")) if event.get("depth") is not None else None,
                    mag_type=event.get("magType"),
                    place=event.get("place"),
                    region=event.get("region"),
                    mag_bin=event.get("mag_bin"),
                    depth_bin=event.get("depth_bin"),
                    source="stream"
                ).to_influx_point("earthquake_events")

                with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
                    client.write_points([point])
            except Exception as e:
                logging.error(f"Error writing raw event: {e}")

        stream.map(sink_event).print()

    def write_daily_global_stats(self, stream):
        def sink_stat(stats: Dict[str, Any]):
            try:
                logging.info(f"Writing daily global stats: {stats}")

                min_event_time = datetime.fromisoformat(stats["min_event_time"].replace('Z', '+00:00'))
                max_event_time = datetime.fromisoformat(stats["max_event_time"].replace('Z', '+00:00'))

                point = StreamingGlobalStats(
                    min_event_time=min_event_time,
                    max_event_time=max_event_time,
                    eq_count=int(stats["eq_count"]),
                    avg_mag=float(stats.get("avg_mag", "0")),
                    max_mag=float(stats.get("max_mag", "0"))
                ).to_influx_point("daily_global_stats")

                with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
                    client.write_points([point])
            except Exception as e:
                logging.error(f"Error writing daily global stats: {e}")

        stream.map(sink_stat).print()

    def write_hourly_global_stats(self, stream):
        def sink_stat(stats: Dict[str, Any]):
            try:
                logging.info(f"Writing hourly global stats: {stats}")

                min_event_time = datetime.fromisoformat(stats["min_event_time"].replace('Z', '+00:00'))
                max_event_time = datetime.fromisoformat(stats["max_event_time"].replace('Z', '+00:00'))

                point = StreamingGlobalStats(
                    min_event_time=min_event_time,
                    max_event_time=max_event_time,
                    eq_count=int(stats["eq_count"]),
                    avg_mag=float(stats.get("avg_mag", "0")),
                    max_mag=float(stats.get("max_mag", "0"))
                ).to_influx_point("hourly_global_stats")

                with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
                    client.write_points([point])
            except Exception as e:
                logging.error(f"Error writing hourly global stats: {e}")

        stream.map(sink_stat).print()

    def write_rolling_regional_stats(self, stream, window_type: str):
        def sink_stat(stats: Dict[str, Any]):
            try:
                logging.info(f"Writing rolling regional stats ({window_type}): {stats}")

                min_event_time = datetime.fromisoformat(stats["min_event_time"].replace('Z', '+00:00'))
                max_event_time = datetime.fromisoformat(stats["max_event_time"].replace('Z', '+00:00'))

                point = StreamingRegionalStats(
                    min_event_time=min_event_time,
                    max_event_time=max_event_time,
                    region=stats.get("region", "unknown"),
                    eq_count=int(stats["eq_count"]),
                    avg_mag=float(stats.get("avg_mag", "0")),
                    max_mag=float(stats.get("max_mag", "0")),
                    window_type=window_type
                ).to_influx_point("rolling_regional_stats")

                with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
                    client.write_points([point])
            except Exception as e:
                logging.error(f"Error writing rolling regional stats: {e}")

        stream.map(sink_stat).print()

    def write_significant_alerts(self, stream):
        def sink_stat(stats: Dict[str, Any]):
            try:
                logging.info(f"Writing significant alert: {stats}")

                min_event_time = datetime.fromisoformat(stats["min_event_time"].replace('Z', '+00:00'))
                max_event_time = datetime.fromisoformat(stats["max_event_time"].replace('Z', '+00:00'))
                max_mag = float(stats.get("max_mag", "0"))
                alert_level = "critical" if max_mag >= 7.0 else "high" if max_mag >= 6.5 else "moderate"

                point = StreamingSignificantAlert(
                    min_event_time=min_event_time,
                    max_event_time=max_event_time,
                    region=stats.get("region", "unknown"),
                    eq_count=int(stats["eq_count"]),
                    avg_mag=float(stats.get("avg_mag", "0")),
                    max_mag=max_mag,
                    alert_level=alert_level
                ).to_influx_point("significant_event_alerts")

                with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
                    client.write_points([point])
            except Exception as e:
                logging.error(f"Error writing significant alerts: {e}")

        stream.map(sink_stat).print()

    def write_magnitude_distribution(self, stream):
        def sink_stat(stats: Dict[str, Any]):
            try:
                logging.info(f"Writing magnitude distribution: {stats}")

                min_event_time = datetime.fromisoformat(stats["min_event_time"].replace('Z', '+00:00'))
                max_event_time = datetime.fromisoformat(stats["max_event_time"].replace('Z', '+00:00'))

                point = StreamingMagnitudeDistribution(
                    min_event_time=min_event_time,
                    max_event_time=max_event_time,
                    mag_bin=stats.get("mag_bin", "unknown"),
                    eq_count=int(stats["eq_count"])
                ).to_influx_point("realtime_magnitude_distribution")

                with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
                    client.write_points([point])
            except Exception as e:
                logging.error(f"Error writing magnitude distribution: {e}")

        stream.map(sink_stat).print()

    def write_depth_patterns(self, stream):
        def sink_stat(stats: Dict[str, Any]):
            try:
                logging.info(f"Writing depth pattern: {stats}")

                min_event_time = datetime.fromisoformat(stats["min_event_time"].replace('Z', '+00:00'))
                max_event_time = datetime.fromisoformat(stats["max_event_time"].replace('Z', '+00:00'))

                point = StreamingDepthPattern(
                    min_event_time=min_event_time,
                    max_event_time=max_event_time,
                    region=stats.get("region", "unknown"),
                    depth_bin=stats.get("depth_bin", "unknown"),
                    eq_count=int(stats["eq_count"])
                ).to_influx_point("depth_pattern_analysis")

                with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
                    client.write_points([point])
            except Exception as e:
                logging.error(f"Error writing depth patterns: {e}")

        stream.map(sink_stat).print()

    def write_sequence_detection(self, stream):
        def sink_stat(stats: Dict[str, Any]):
            try:
                logging.info(f"Writing sequence detection: {stats}")

                first_event_time = datetime.fromisoformat(stats["min_event_time"].replace('Z', '+00:00'))
                last_event_time = datetime.fromisoformat(stats["max_event_time"].replace('Z', '+00:00'))

                point = SequenceDetection(
                    region=stats.get("region", "unknown"),
                    eq_count=int(stats["eq_count"]),
                    avg_mag=float(stats.get("avg_mag", "0")),
                    max_mag=float(stats.get("max_mag", "0")),
                    first_event_time=first_event_time,
                    last_event_time=last_event_time,
                    lat_grid=float(stats.get("lat_grid", "0")),
                    lon_grid=float(stats.get("lon_grid", "0")),
                    sequence_type=stats.get("sequence_type", "unknown")
                ).to_influx_point("sequence_detection")

                with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
                    client.write_points([point])
            except Exception as e:
                logging.error(f"Error writing sequence detection: {e}")

        stream.map(sink_stat).print()
