from helpers import write_csv
from pyspark.sql.functions import (
    count, avg, max as smax,
    round as sround
)
from pyspark.sql import DataFrame

def monthly_region_stats(df: DataFrame, output_file: str) -> DataFrame:
    """
    Generates monthly statistics for earthquake data grouped by region.

    Args:
        df: Input DataFrame containing earthquake data.
        output_file: Path to save the output CSV file.
    """
    monthly_region = (df.groupBy("year","month","region")
                        .agg(
                            count("*").alias("eq_count"),
                            sround(avg("mag"), 3).alias("avg_mag"),
                            sround(smax("mag"), 3).alias("max_mag")
                        )
                        .orderBy("year","month","region"))
    write_csv(monthly_region, output_file, mode="overwrite")
    
    return monthly_region

def weekly_region_stats(df: DataFrame, output_file: str) -> DataFrame:
    """
    Generates weekly statistics for earthquake data grouped by region.

    Args:
        df: Input DataFrame containing earthquake data.
        output_file: Path to save the output CSV file.
    """
    weekly_region = (df.groupBy("year", "week", "region")
                        .agg(
                            count("*").alias("eq_count"),
                            sround(avg("mag"), 3).alias("avg_mag"),
                            sround(smax("mag"), 3).alias("max_mag")
                        )
                        .orderBy("year", "week", "region"))
    write_csv(weekly_region, output_file, mode="overwrite")

    return weekly_region

def daily_global_stats(df: DataFrame, output_file: str) -> DataFrame:
    """
    Generates daily statistics for earthquake data globally.

    Args:
        df: Input DataFrame containing earthquake data.
        output_file: Path to save the output CSV file.
    """
    daily_global = (df.groupBy("date")
                      .agg(
                          count("*").alias("eq_count"),
                          sround(avg("mag"), 3).alias("avg_mag"),
                          sround(smax("mag"), 3).alias("max_mag")
                      )
                      .orderBy("date"))
    write_csv(daily_global, output_file, mode="overwrite")

    return daily_global

def monthly_magnitude_stats(df: DataFrame, output_file: str) -> DataFrame:
    """
    Generates monthly statistics for earthquake data grouped by magnitude bins and region.

    Args:
        df: Input DataFrame containing earthquake data.
        output_file: Path to save the output CSV file.
    """
    monthly_mag_bins = (df.groupBy("year","month","region","mag_bin")
                          .agg(count("*").alias("eq_count"))
                          .orderBy("year","month","region","mag_bin"))
    write_csv(monthly_mag_bins, output_file, mode="overwrite")

    return monthly_mag_bins

def monthly_depth_stats(df: DataFrame, output_file: str) -> DataFrame:
    """
    Generates monthly statistics for earthquake data grouped by depth bins and region.

    Args:
        df: Input DataFrame containing earthquake data.
        output_file: Path to save the output CSV file.
    """
    monthly_depth_bins = (df.groupBy("year","month","region","depth_bin")
                            .agg(count("*").alias("eq_count"))
                            .orderBy("year","month","region","depth_bin"))
    write_csv(monthly_depth_bins, output_file, mode="overwrite")

    return monthly_depth_bins
