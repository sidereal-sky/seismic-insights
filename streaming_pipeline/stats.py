from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.common import Time
from pyflink.common.typeinfo import Types
from aggregates import *

def hourly_global_stats(datastream):
    """
    Real-time global earthquake activity - updates every hour.
    """
    return datastream \
        .key_by(lambda e: "global") \
        .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
        .aggregate(CountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(TumblingEventTimeWindows.of(Time.hours(1)))

def rolling_24h_regional_stats(datastream):
    """
    Rolling 24-hour statistics per region, updated every hour.
    """
    return datastream \
        .key_by(lambda e: e["region"]) \
        .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1))) \
        .aggregate(CountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))

def rolling_7day_regional_stats(datastream):
    """
    Rolling 7-day statistics per region, updated daily.
    """
    return datastream \
        .key_by(lambda e: e["region"]) \
        .window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(5))) \
        .aggregate(CountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))

def significant_event_alerts(datastream):
    """
    Real-time alerting for significant earthquakes (magnitude >= 6.0).
    """
    return datastream \
        .filter(lambda e: float(e["mag"]) >= 6.0) \
        .key_by(lambda e: e["region"]) \
        .window(TumblingEventTimeWindows.of(Time.seconds(30))) \
        .aggregate(CountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(TumblingEventTimeWindows.of(Time.minutes(5)))

def magnitude_distribution_realtime(datastream):
    """
    Real-time magnitude distribution, updated every 2 hours.
    """
    return datastream \
        .key_by(lambda e: e["mag_bin"]) \
        .window(TumblingEventTimeWindows.of(Time.minutes(3))) \
        .aggregate(CountOnlyAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(TumblingEventTimeWindows.of(Time.hours(2)))

def depth_pattern_analysis(datastream):
    """
    Regional depth patterns over 12-hour windows.
    """
    return datastream \
        .key_by(lambda e: (e["region"], e["depth_bin"])) \
        .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
        .aggregate(CountOnlyAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(TumblingEventTimeWindows.of(Time.hours(12)))

def rapid_sequence_detection(datastream):
    """
    Detects rapid sequences of earthquakes (potential aftershocks/swarms).
    """
    return datastream \
        .filter(lambda e: float(e["mag"]) >= 4.0) \
        .key_by(lambda e: (e["region"], 
                          int(float(e["latitude"]) * 10),  # Grid coordinates  
                          int(float(e["longitude"]) * 10))) \
        .window(TumblingEventTimeWindows.of(Time.minutes(2))) \
        .aggregate(SequenceDetectionAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING())) \
        .filter(lambda result: int(result["eq_count"]) >= 3)  # 3+ events = potential sequence
        # Original: .window(TumblingEventTimeWindows.of(Time.minutes(30)))

def daily_global_stats(datastream):
    """
    Daily global statistics - Uses 24-hour windows.
    """
    return datastream \
        .key_by(lambda e: "global") \
        .window(TumblingEventTimeWindows.of(Time.minutes(10))) \
        .aggregate(CountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(TumblingEventTimeWindows.of(Time.hours(24)))
