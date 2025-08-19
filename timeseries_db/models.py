from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any

@dataclass
class EarthquakeEvent:
    event_id: str
    timestamp: datetime
    latitude: float
    longitude: float
    magnitude: float
    depth: Optional[float]
    mag_type: Optional[str]
    place: Optional[str]
    region: Optional[str]
    mag_bin: Optional[str]
    depth_bin: Optional[str]
    source: str = "batch"
    
    def to_influx_point(self, measurement: str) -> Dict[str, Any]:
        tags = {
            "region": self.region or "Unknown",
            "mag_bin": self.mag_bin or "Unknown",
            "depth_bin": self.depth_bin or "Unknown",
            "source": self.source
        }
        
        if self.mag_type:
            tags["mag_type"] = self.mag_type
            
        fields = {
            "event_id": self.event_id,
            "magnitude": self.magnitude,
            "latitude": self.latitude,
            "longitude": self.longitude
        }
        
        if self.depth is not None:
            fields["depth"] = self.depth
        if self.place:
            fields["place"] = self.place
            
        return {
            "measurement": measurement,
            "tags": tags,
            "fields": fields,
            "time": self.timestamp
        }

@dataclass 
class DailyGlobalStats:
    date: datetime
    eq_count: int
    avg_mag: float
    max_mag: float
    source: str = "batch"
    
    def to_influx_point(self, measurement: str) -> Dict[str, Any]:
        return {
            "measurement": measurement,
            "tags": {
                "source": self.source
            },
            "fields": {
                "eq_count": self.eq_count,
                "avg_mag": self.avg_mag,
                "max_mag": self.max_mag
            },
            "time": self.date
        }

@dataclass
class RegionalStats:
    timestamp: datetime
    region: str
    eq_count: int
    avg_mag: float
    max_mag: float
    year: int
    period: int
    period_type: str
    source: str = "batch"
    
    def to_influx_point(self, measurement: str) -> Dict[str, Any]:
        tags = {
            "region": self.region,
            "source": self.source,
            "year": str(self.year)
        }
        
        if self.period_type == "week":
            tags["week"] = str(self.period)
        else:
            tags["month"] = str(self.period)
            
        return {
            "measurement": measurement,
            "tags": tags,
            "fields": {
                "eq_count": self.eq_count,
                "avg_mag": self.avg_mag,
                "max_mag": self.max_mag
            },
            "time": self.timestamp
        }

@dataclass
class MagnitudeDistributionStats:
    timestamp: datetime
    region: str
    mag_bin: str
    eq_count: int
    year: int
    month: int
    source: str = "batch"
    
    def to_influx_point(self, measurement: str) -> Dict[str, Any]:
        return {
            "measurement": measurement,
            "tags": {
                "region": self.region,
                "mag_bin": self.mag_bin,
                "source": self.source,
                "year": str(self.year),
                "month": str(self.month)
            },
            "fields": {
                "eq_count": self.eq_count
            },
            "time": self.timestamp
        }

@dataclass
class DepthDistributionStats:
    timestamp: datetime
    region: str
    depth_bin: str
    eq_count: int
    year: int
    month: int
    source: str = "batch"
    
    def to_influx_point(self, measurement: str) -> Dict[str, Any]:
        return {
            "measurement": measurement,
            "tags": {
                "region": self.region,
                "depth_bin": self.depth_bin,
                "source": self.source,
                "year": str(self.year),
                "month": str(self.month)
            },
            "fields": {
                "eq_count": self.eq_count
            },
            "time": self.timestamp
        }
