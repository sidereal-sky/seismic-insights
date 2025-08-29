from aggregates import *
from datetime import datetime, timedelta
from helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, count, to_date, floor, weekofyear
from pyspark.sql.types import DoubleType
from earthquake_data import download_data
import sys
import os

# Add parent directory to path for timeseries_db imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from timeseries_db.spark_adapter import SparkInfluxDBAdapter

# Constants for earthquake data
MIN_MAGNITUDE = 4.0

# Constants for file paths
INPUT_FILE = "usgs_earthquakes.csv"
OUTPUT_FILE = "output/stats"

def get_time_range():
    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")

    if start_date and end_date:
        print(f"Using provided date range: {start_date} to {end_date}")
        return start_date, end_date

    delta_seconds = int(os.getenv("DATA_DELTA_SECONDS"))

    now = datetime.utcnow()
    start_time = now - timedelta(seconds=delta_seconds)

    starttime = start_time.strftime('%Y-%m-%dT%H:%M:%S')
    endtime = now.strftime('%Y-%m-%dT%H:%M:%S')

    print(f"Using calculated time range: {starttime} to {endtime} (last {delta_seconds} seconds)")
    return starttime, endtime

def main():
    starttime, endtime = get_time_range()
    download_data(starttime=starttime, endtime=endtime, min_magnitude=MIN_MAGNITUDE, output_file=INPUT_FILE)

    # Configure Spark to run locally with proper networking settings
    spark = (SparkSession.builder
             .appName("EarthquakeBatchProcessing")
             .master("local[*]") 
             .config("spark.driver.host", "localhost")
             .config("spark.driver.bindAddress", "localhost")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())

    # Initialize InfluxDB adapter
    influx_adapter = SparkInfluxDBAdapter(
        url=os.getenv('INFLUXDB_URL'),
        token=os.getenv('INFLUXDB_TOKEN'),
        org=os.getenv('INFLUXDB_ORG'),
        bucket=os.getenv('INFLUXDB_BUCKET')
    )

    raw = spark.read.option("header", True).csv(INPUT_FILE)
    df = (raw
          .select("id", "time", "latitude", "longitude", "depth", "mag", "magType", "place")
          .withColumn("latitude", col("latitude").cast(DoubleType()))
          .withColumn("longitude", col("longitude").cast(DoubleType()))
          .withColumn("depth", col("depth").cast(DoubleType()))
          .withColumn("mag", col("mag").cast(DoubleType()))
    )

    # parse and convert time column
    df = parse_time_column(df)

    # filter out rows < MIN_MAGNITUDE and drop duplicates
    df = df.where(col("event_time").isNotNull() & col("mag").isNotNull() & (col("mag") >= MIN_MAGNITUDE))
    df = df.dropDuplicates(["id"])

    print("Sample data after filtering:")
    df.select("time", "event_time", "mag", "place").show(10, truncate=False)

    # add derived columns
    df = (df
          .withColumn("year", year("event_time"))
          .withColumn("month", month("event_time"))
          .withColumn("week", weekofyear("event_time"))
          .withColumn("date", to_date(col("event_time")))
          .withColumn("mag_bin", magnitude_bin_col())
          .withColumn("depth_bin", depth_bin_expr())
          .withColumn("region", get_region_from_place())
    )

    print("Sample data after adding derived columns:")
    df.select("event_time", "year", "month", "week", "date", "mag_bin", "depth_bin", "region").show(10, truncate=False)

    # Write raw events to InfluxDB
    influx_adapter.write_raw_events(df)

    # generate stats
    print("=============== Generating statistics ===============")

    monthly_region = monthly_region_stats(df, output_file=f"{OUTPUT_FILE}_monthly_region")
    print("Monthly region stats:")
    monthly_region.show(10, truncate=False)
    influx_adapter.write_monthly_regional_stats(monthly_region)

    weekly_region = weekly_region_stats(df, output_file=f"{OUTPUT_FILE}_weekly_region")
    print("Weekly region stats:")
    weekly_region.show(10, truncate=False)
    influx_adapter.write_weekly_regional_stats(weekly_region)

    daily_global = daily_global_stats(df, output_file=f"{OUTPUT_FILE}_daily_global")
    print("Daily global stats:")
    daily_global.show(10, truncate=False)
    influx_adapter.write_daily_global_stats(daily_global)

    monthly_magnitude = monthly_magnitude_stats(df, output_file=f"{OUTPUT_FILE}_monthly_magnitude")
    print("Monthly magnitude stats:")
    monthly_magnitude.show(10, truncate=False)
    influx_adapter.write_monthly_magnitude_stats(monthly_magnitude)

    monthly_depth = monthly_depth_stats(df, output_file=f"{OUTPUT_FILE}_monthly_depth")
    print("Monthly depth stats:")
    monthly_depth.show(10, truncate=False)
    influx_adapter.write_monthly_depth_stats(monthly_depth)

    print("=============== Statistics generation completed ===============")
    print("All the results are saved in the output directory.")

    spark.stop()

if __name__ == "__main__":
    main()
