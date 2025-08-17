from pyspark.sql.types import StringType, DoubleType
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, floor, concat_ws, regexp_extract, trim, regexp_replace, to_timestamp, from_unixtime, round as sround
)

def parse_time_column(df: DataFrame) -> DataFrame:
    """
    Parses the 'time' column in the DataFrame to ensure it is in timestamp format.
    
    Args:
        df: Input DataFrame containing a 'time' column.

    Returns:
        DataFrame: DataFrame with the 'time' column converted to timestamp format.
    """
    if "time" not in df.columns:
        raise ValueError("DataFrame must contain a 'time' column")
    
    df = df.withColumn("time_str", col("time").cast(StringType()))
    df = df.withColumn(
        "event_time",
        when(
            col("time_str").rlike(r"^\d+$"),
            to_timestamp(from_unixtime(col("time_str").cast("long") / 1000.0))
        ).otherwise(
            to_timestamp(col("time_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")
        )
    ).drop("time_str")
    return df

def depth_bin_expr():
    """
    Categorizes earthquake depth into bins:
    - "shallow" for depths < 70 km
    - "intermediate" for depths between 70 km and 300 km
    - "deep" for depths >= 300 km
    """
    return when(col("depth").isNull(), lit("unknown")) \
        .when(col("depth") < 70, lit("shallow")) \
        .when((col("depth") >= 70) & (col("depth") < 300), lit("intermediate")) \
        .otherwise(lit("deep"))

def magnitude_bin_col():
    """
    Categorizes earthquake magnitude into bins:
    - "1.0-1.9" for magnitudes 1.0 to 1.9
    - "2.0-2.9" for magnitudes 2.0 to 2.9
    - ...
    """
    return concat_ws("-",
        sround(floor(col("mag")).cast(DoubleType()), 1),
        sround((floor(col("mag")) + lit(0.9)).cast(DoubleType()), 1)
    )

def get_region_from_place():
    """
    Extracts the region from the 'place' column in the DataFrame.
    It looks for the last part of the place string after a comma or "of".
    If no region is found, it defaults to "Unknown".
    """
    p = col("place")

    after_comma = regexp_extract(p, r",\s*([^,]+)$", 1)
    after_of = regexp_extract(p, r".*\bof\s+(.+)$", 1)

    candidate = when(p.isNull() | (trim(p) == ""), lit("Unknown")) \
        .otherwise(
            when(trim(after_comma) != "", after_comma)
            .otherwise(
                when(trim(after_of) != "", after_of)
                .otherwise(p)
            )
        )

    cleaned = trim(regexp_replace(candidate, r"(?i)\s+region$", ""))

    return when(cleaned == "", lit("Unknown")).otherwise(cleaned)

def write_csv(df, path, mode="overwrite"):
    (df.coalesce(1)
       .write.mode(mode)
       .option("header", True)
       .csv(path))
