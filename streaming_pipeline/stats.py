from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.common import Time
from pyflink.common.typeinfo import Types
from aggregates import *

def hourly_global_stats(datastream):
    """
    Real-time global earthquake activity - updates every hour.
    Use case: Show current global seismic activity levels
    """
    return datastream \
        .key_by(lambda e: "global") \
        .window(TumblingEventTimeWindows.of(Time.hours(1))) \
        .aggregate(CountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

def rolling_24h_regional_stats(datastream):
    """
    Rolling 24-hour statistics per region, updated every hour.
    Use case: "What's the seismic activity in California over the last 24 hours?"
    """
    return datastream \
        .key_by(lambda e: e["region"]) \
        .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1))) \
        .aggregate(CountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

def rolling_7day_regional_stats(datastream):
    """
    Rolling 7-day statistics per region, updated daily.
    Use case: Weekly seismic activity patterns by region
    """
    return datastream \
        .key_by(lambda e: e["region"]) \
        .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1))) \
        .aggregate(CountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

def significant_event_alerts(datastream):
    """
    Real-time alerting for significant earthquakes (magnitude >= 6.0).
    Immediate processing with 5-minute windows for clustering.
    Use case: Emergency response and alert systems
    """
    return datastream \
        .filter(lambda e: float(e["mag"]) >= 6.0) \
        .key_by(lambda e: e["region"]) \
        .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
        .aggregate(CountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

def magnitude_distribution_realtime(datastream):
    """
    Real-time magnitude distribution, updated every 2 hours.
    Shows how earthquake sizes are distributed globally.
    Use case: Monitor if there's an unusual pattern in earthquake magnitudes
    """
    return datastream \
        .key_by(lambda e: e["mag_bin"]) \
        .window(TumblingEventTimeWindows.of(Time.hours(2))) \
        .aggregate(CountOnlyAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

def depth_pattern_analysis(datastream):
    """
    Regional depth patterns over 12-hour windows.
    Helps identify geological patterns and potential volcanic activity.
    Use case: "Are we seeing more shallow earthquakes in this region lately?"
    """
    return datastream \
        .key_by(lambda e: (e["region"], e["depth_bin"])) \
        .window(TumblingEventTimeWindows.of(Time.hours(12))) \
        .aggregate(CountOnlyAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

def rapid_sequence_detection(datastream):
    """
    Detects rapid sequences of earthquakes (potential aftershocks/swarms).
    Uses 30-minute windows to identify clusters.
    Use case: Early detection of earthquake swarms or aftershock sequences
    """
    return datastream \
        .filter(lambda e: float(e["mag"]) >= 4.0) \
        .key_by(lambda e: (e["region"], 
                          int(float(e["latitude"]) * 10),  # Grid coordinates  
                          int(float(e["longitude"]) * 10))) \
        .window(TumblingEventTimeWindows.of(Time.minutes(30))) \
        .aggregate(SequenceDetectionAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING())) \
        .filter(lambda result: int(result["eq_count"]) >= 3)  # 3+ events = potential sequence

def daily_global_stats(datastream):
    """
    Daily global statistics - Uses 24-hour windows.
    Use case: "What's the global seismic activity over the last 24 hours?"
    """
    return datastream \
        .key_by(lambda e: "global") \
        .window(TumblingEventTimeWindows.of(Time.hours(24))) \
        .aggregate(CountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
