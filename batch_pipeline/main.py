from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, count
from earthquake_data import download_data

# Constants for earthquake data
START_DATE = "2024-08-03"
END_DATE = "2025-08-03"
MIN_MAGNITUDE = 4.0

# Constants for file paths
INPUT_FILE = "all_earthquakes.csv"
OUTPUT_FILE = "output/one_year_stats.csv"

def main():
    # Download earthquake data
    download_data(starttime=START_DATE, endtime=END_DATE, min_magnitude=MIN_MAGNITUDE, output_file=INPUT_FILE)

    spark = SparkSession.builder \
        .appName("EarthquakeBatchProcessing") \
        .getOrCreate()

    df = spark.read.csv(INPUT_FILE, header=True, inferSchema=True)

    df = df.withColumn("year", year("time")).withColumn("month", month("time"))

    df.select("time", "mag", "place").show(10, truncate=False)

    # Aggregate monthly earthquake statistics
    monthly_stats = df.groupBy("year", "month") \
        .agg(
            count("*").alias("earthquake_count"),
            avg("mag").alias("average_magnitude")
        ) \
        .orderBy("year", "month")

    monthly_stats.show()

    # Save the results to a CSV file
    monthly_stats.coalesce(1).write.csv(OUTPUT_FILE, header=True, mode="overwrite")

    spark.stop()

if __name__ == "__main__":
    main()
