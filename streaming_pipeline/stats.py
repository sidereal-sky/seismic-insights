from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows
from pyflink.common import Time
from pyflink.common.typeinfo import Types
from aggregates import *

def hourly_global_stats(datastream):
    """
    Hourly global statistics.
    """
    return datastream \
        .key_by(lambda e: "global") \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(1))) \
        .aggregate(GlobalCountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(TumblingProcessingTimeWindows.of(Time.hours(1)))

def daily_global_stats(datastream):
    """
    Daily global statistics.
    """
    return datastream \
        .key_by(lambda e: "global") \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) \
        .aggregate(GlobalCountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(TumblingProcessingTimeWindows.of(Time.hours(24)))

def rolling_24h_regional_stats(datastream):
    """
    Rolling 24-hour statistics per region, updated every hour.
    """
    return datastream \
        .key_by(lambda e: e["region"]) \
        .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.minutes(1))) \
        .aggregate(RegionalCountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(SlidingProcessingTimeWindows.of(Time.hours(24), Time.hours(1)))

def rolling_7day_regional_stats(datastream):
    """
    Rolling 7-day statistics per region, updated daily.
    """
    return datastream \
        .key_by(lambda e: e["region"]) \
        .window(SlidingProcessingTimeWindows.of(Time.minutes(10), Time.minutes(5))) \
        .aggregate(RegionalCountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(SlidingProcessingTimeWindows.of(Time.days(7), Time.days(1)))

def significant_event_alerts(datastream):
    """
    Alerting for significant earthquakes (magnitude >= 6.0), updates every 5 minutes.
    """
    return datastream \
        .filter(lambda e: float(e["mag"]) >= 6.0) \
        .key_by(lambda e: e["region"]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
        .aggregate(RegionalCountAvgMaxAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))

def magnitude_distribution_realtime(datastream):
    """
    Magnitude distribution, updated every 2 hours.
    """
    return datastream \
        .key_by(lambda e: e["mag_bin"]) \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(2))) \
        .aggregate(MagnitudeBinCountAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(TumblingProcessingTimeWindows.of(Time.hours(2)))

def depth_pattern_analysis(datastream):
    """
    Regional depth patterns over 12-hour windows.
    """
    return datastream \
        .key_by(lambda e: (e["region"], e["depth_bin"])) \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) \
        .aggregate(DepthPatternAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        # Original: .window(TumblingProcessingTimeWindows.of(Time.hours(12)))

def rapid_sequence_detection(datastream):
    """
    Detects rapid sequences of earthquakes (potential aftershocks/swarms), over 30 minutes.
    """
    return datastream \
        .filter(lambda e: float(e["mag"]) >= 4.0) \
        .key_by(lambda e: (e["region"], 
                          int(float(e["latitude"]) / 10),  # Grid coordinates
                          int(float(e["longitude"]) / 10))) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
        .aggregate(SequenceDetectionAgg(), output_type=Types.MAP(Types.STRING(), Types.STRING())) \
        .filter(lambda result: int(result["eq_count"]) >= 3)  # >= 3 = potential sequence
        # Original: .window(TumblingProcessingTimeWindows.of(Time.minutes(30)))
