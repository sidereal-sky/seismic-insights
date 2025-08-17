from aggregates import *
from helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, count, to_date, floor, weekofyear
from pyspark.sql.types import DoubleType
from earthquake_data import download_data

# Constants for earthquake data
START_DATE = "2024-08-03"
END_DATE = "2025-08-03"
MIN_MAGNITUDE = 4.0

# Constants for file paths
INPUT_FILE = "usgs_earthquakes.csv"
OUTPUT_FILE = "output/stats"

def main():
    download_data(starttime=START_DATE, endtime=END_DATE, min_magnitude=MIN_MAGNITUDE, output_file=INPUT_FILE)

    spark = SparkSession.builder.appName("EarthquakeBatchProcessing").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

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

    # generate stats
    print("=============== Generating statistics ===============")

    monthly_region = monthly_region_stats(df, output_file=f"{OUTPUT_FILE}_monthly_region")
    print("Monthly region stats:")
    monthly_region.show(10, truncate=False)

    weekly_region = weekly_region_stats(df, output_file=f"{OUTPUT_FILE}_weekly_region")
    print("Weekly region stats:")
    weekly_region.show(10, truncate=False)

    daily_global = daily_global_stats(df, output_file=f"{OUTPUT_FILE}_daily_global")
    print("Daily global stats:")
    daily_global.show(10, truncate=False)

    monthly_magnitude = monthly_magnitude_stats(df, output_file=f"{OUTPUT_FILE}_monthly_magnitude")
    print("Monthly magnitude stats:")
    monthly_magnitude.show(10, truncate=False)

    monthly_depth = monthly_depth_stats(df, output_file=f"{OUTPUT_FILE}_monthly_depth")
    print("Monthly depth stats:")
    monthly_depth.show(10, truncate=False)

    print("=============== Statistics generation completed ===============")
    print("All the results are saved in the output directory.")

    spark.stop()

if __name__ == "__main__":
    main()
