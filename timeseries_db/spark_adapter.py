from datetime import datetime, timedelta
from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from .client import SeismicInfluxDBClient
from .models import (
    EarthquakeEvent, DailyGlobalStats, RegionalStats,
    MagnitudeDistributionStats, DepthDistributionStats
)

class SparkInfluxDBAdapter:
    
    def __init__(self, url: str, token: str, org: str, bucket: str, timeout: int = 30000):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.timeout = timeout
    
    def write_raw_events(self, df: DataFrame, batch_size: int = 1000):
        total_count = df.count()
        if total_count == 0:
            return
        
        df_coalesced = df.coalesce(1)
        
        with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
            all_rows = df_coalesced.collect()
            
            for i in range(0, len(all_rows), batch_size):
                batch_rows = all_rows[i:i + batch_size]
                points = []
                
                for row in batch_rows:
                    try:
                        event = EarthquakeEvent(
                            event_id=row.id,
                            timestamp=row.event_time,
                            latitude=float(row.latitude),
                            longitude=float(row.longitude),
                            magnitude=float(row.mag),
                            depth=float(row.depth) if row.depth is not None else None,
                            mag_type=row.magType,
                            place=row.place,
                            region=row.region,
                            mag_bin=row.mag_bin,
                            depth_bin=row.depth_bin,
                            source="batch"
                        )
                        points.append(event.to_influx_point("earthquake_events"))
                    except Exception:
                        continue
                
                if points:
                    client.write_points(points)
    
    def write_daily_global_stats(self, df: DataFrame):
        with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
            points = []
            
            for row in df.collect():
                try:
                    if isinstance(row.date, str):
                        date_obj = datetime.strptime(row.date, "%Y-%m-%d")
                    elif hasattr(row.date, 'strftime'):
                        if hasattr(row.date, 'date'):
                            date_obj = datetime.combine(row.date.date(), datetime.min.time())
                        else:
                            date_obj = datetime.combine(row.date, datetime.min.time())
                    else:
                        date_obj = datetime.strptime(str(row.date), "%Y-%m-%d")
                    
                    stats = DailyGlobalStats(
                        date=date_obj,
                        eq_count=int(row.eq_count),
                        avg_mag=float(row.avg_mag),
                        max_mag=float(row.max_mag),
                        source="batch"
                    )
                    points.append(stats.to_influx_point("daily_global_stats"))
                except Exception:
                    continue
            
            if points:
                client.write_points(points)
    
    def write_weekly_regional_stats(self, df: DataFrame):
        with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
            points = []
            
            for row in df.collect():
                try:
                    year = int(row.year)
                    week = int(row.week)
                    
                    date_string = f"{year}-W{week:02d}-1"
                    
                    try:
                        timestamp = datetime.strptime(date_string, "%Y-W%W-%w")
                    except ValueError:
                        jan_1 = datetime(year, 1, 1)
                        days_offset = (week - 1) * 7
                        timestamp = jan_1 + timedelta(days=days_offset)
                    
                    stats = RegionalStats(
                        timestamp=timestamp,
                        region=row.region,
                        eq_count=int(row.eq_count),
                        avg_mag=float(row.avg_mag),
                        max_mag=float(row.max_mag),
                        year=year,
                        period=week,
                        period_type="week",
                        source="batch"
                    )
                    points.append(stats.to_influx_point("weekly_regional_stats"))
                except Exception:
                    continue
            
            if points:
                client.write_points(points)
    
    def write_monthly_regional_stats(self, df: DataFrame):
        with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
            points = []
            
            for row in df.collect():
                try:
                    year = int(row.year)
                    month = int(row.month)
                    timestamp = datetime(year, month, 1)
                    
                    stats = RegionalStats(
                        timestamp=timestamp,
                        region=row.region,
                        eq_count=int(row.eq_count),
                        avg_mag=float(row.avg_mag),
                        max_mag=float(row.max_mag),
                        year=year,
                        period=month,
                        period_type="month",
                        source="batch"
                    )
                    points.append(stats.to_influx_point("monthly_regional_stats"))
                except Exception:
                    continue
            
            if points:
                client.write_points(points)
    
    def write_monthly_magnitude_stats(self, df: DataFrame):
        with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
            points = []
            
            for row in df.collect():
                try:
                    year = int(row.year)
                    month = int(row.month)
                    timestamp = datetime(year, month, 1)
                    
                    stats = MagnitudeDistributionStats(
                        timestamp=timestamp,
                        region=row.region,
                        mag_bin=row.mag_bin,
                        eq_count=int(row.eq_count),
                        year=year,
                        month=month,
                        source="batch"
                    )
                    points.append(stats.to_influx_point("monthly_magnitude_stats"))
                except Exception:
                    continue
            
            if points:
                client.write_points(points)
    
    def write_monthly_depth_stats(self, df: DataFrame):
        with SeismicInfluxDBClient(self.url, self.token, self.org, self.bucket, self.timeout) as client:
            points = []
            
            for row in df.collect():
                try:
                    year = int(row.year)
                    month = int(row.month)
                    timestamp = datetime(year, month, 1)
                    
                    stats = DepthDistributionStats(
                        timestamp=timestamp,
                        region=row.region,
                        depth_bin=row.depth_bin,
                        eq_count=int(row.eq_count),
                        year=year,
                        month=month,
                        source="batch"
                    )
                    points.append(stats.to_influx_point("monthly_depth_stats"))
                except Exception:
                    continue
            
            if points:
                client.write_points(points)
