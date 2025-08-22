from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction
from datetime import datetime

import logging
import sys
import os

from aggregates import *
from stats import *
from helpers import *

# Add parent directory to path for timeseries_db imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from timeseries_db.flink_adapter import FlinkInfluxDBAdapter

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPICS = os.getenv('KAFKA_TOPICS')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID')
KAFKA_SOURCE_NAME = os.getenv('KAFKA_SOURCE_NAME')

# InfluxDB config
INFLUXDB_URL = os.getenv('INFLUXDB_URL')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')

# Filtering config
MIN_MAGNITUDE = 4.0

JOB_NAME = 'Seismic Streaming Pipeline'

logging.basicConfig(level=logging.INFO)

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add Kafka connector JAR to the environment
    kafka_jar_path = "/opt/flink-sql-connector-kafka.jar"
    env.add_jars(f"file://{kafka_jar_path}")

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics(KAFKA_TOPICS) \
        .set_group_id(KAFKA_GROUP_ID) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name=KAFKA_SOURCE_NAME
    )

    # Initialize InfluxDB adapter
    influx_adapter = FlinkInfluxDBAdapter(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
        bucket=INFLUXDB_BUCKET
    )

    events = ds.map(
        parse_event,
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )

    filtered_events = events \
        .filter(lambda x: x is not None and x['mag'] is not None and x['mag'] >= MIN_MAGNITUDE)

    deduped_events = filtered_events \
        .key_by(lambda x: x['id']) \
        .process(DeduplicateById(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # Write raw (filtered and deduped) events to InfluxDB
    influx_adapter.write_raw_events(deduped_events)

    # Generate streaming statistics
    hourly_global = hourly_global_stats(deduped_events)
    influx_adapter.write_hourly_global_stats(hourly_global)

    rolling_24h_regional = rolling_24h_regional_stats(deduped_events)
    influx_adapter.write_rolling_regional_stats(rolling_24h_regional, "24h")

    rolling_7day_regional = rolling_7day_regional_stats(deduped_events)
    influx_adapter.write_rolling_regional_stats(rolling_7day_regional, "7day")

    significant_alerts = significant_event_alerts(deduped_events)
    influx_adapter.write_significant_alerts(significant_alerts)

    magnitude_distribution = magnitude_distribution_realtime(deduped_events)
    influx_adapter.write_magnitude_distribution(magnitude_distribution)

    depth_patterns = depth_pattern_analysis(deduped_events)
    influx_adapter.write_depth_patterns(depth_patterns)

    sequence_detection = rapid_sequence_detection(deduped_events)
    influx_adapter.write_sequence_detection(sequence_detection)

    daily_global = daily_global_stats(deduped_events)
    influx_adapter.write_daily_global_stats(daily_global)

    env.execute(JOB_NAME)

if __name__ == '__main__':
    main()
