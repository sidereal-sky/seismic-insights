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

@dataclass
class StreamingGlobalStats:
    min_event_time: datetime
    max_event_time: datetime
    eq_count: int
    avg_mag: float
    max_mag: float

    def to_influx_point(self, measurement: str) -> Dict[str, Any]:
        return {
            "measurement": measurement,
            "tags": {
                "region": "global",
                "source": "stream"
            },
            "fields": {
                "eq_count": self.eq_count,
                "avg_mag": self.avg_mag,
                "max_mag": self.max_mag,
                "min_event_time": self.min_event_time.isoformat(),
                "max_event_time": self.max_event_time.isoformat()
            },
            "time": self.min_event_time
        }

@dataclass
class StreamingRegionalStats:
    min_event_time: datetime
    max_event_time: datetime
    region: str
    eq_count: int
    avg_mag: float
    max_mag: float
    window_type: str  # "24h", "7day", etc.

    def to_influx_point(self, measurement: str) -> Dict[str, Any]:
        return {
            "measurement": measurement,
            "tags": {
                "region": self.region,
                "window_type": self.window_type,
                "source": "stream"
            },
            "fields": {
                "eq_count": self.eq_count,
                "avg_mag": self.avg_mag,
                "max_mag": self.max_mag,
                "min_event_time": self.min_event_time.isoformat(),
                "max_event_time": self.max_event_time.isoformat()
            },
            "time": self.min_event_time
        }

@dataclass
class StreamingSignificantAlert:
    min_event_time: datetime
    max_event_time: datetime
    region: str
    eq_count: int
    avg_mag: float
    max_mag: float
    alert_level: str

    def to_influx_point(self, measurement: str) -> Dict[str, Any]:
        return {
            "measurement": measurement,
            "tags": {
                "region": self.region,
                "alert_level": self.alert_level,
                "source": "stream"
            },
            "fields": {
                "eq_count": self.eq_count,
                "avg_mag": self.avg_mag,
                "max_mag": self.max_mag,
                "min_event_time": self.min_event_time.isoformat(),
                "max_event_time": self.max_event_time.isoformat()
            },
            "time": self.min_event_time
        }

@dataclass
class StreamingMagnitudeDistribution:
    min_event_time: datetime
    max_event_time: datetime
    mag_bin: str
    eq_count: int

    def to_influx_point(self, measurement: str) -> Dict[str, Any]:
        return {
            "measurement": measurement,
            "tags": {
                "mag_bin": self.mag_bin,
                "source": "stream"
            },
            "fields": {
                "eq_count": self.eq_count,
                "min_event_time": self.min_event_time.isoformat(),
                "max_event_time": self.max_event_time.isoformat()
            },
            "time": self.min_event_time
        }

@dataclass
class StreamingDepthPattern:
    min_event_time: datetime
    max_event_time: datetime
    region: str
    depth_bin: str
    eq_count: int

    def to_influx_point(self, measurement: str) -> Dict[str, Any]:
        return {
            "measurement": measurement,
            "tags": {
                "region": self.region,
                "depth_bin": self.depth_bin,
                "source": "stream"
            },
            "fields": {
                "eq_count": self.eq_count,
                "min_event_time": self.min_event_time.isoformat(),
                "max_event_time": self.max_event_time.isoformat()
            },
            "time": self.min_event_time
        }

@dataclass
class SequenceDetection:
    region: str
    eq_count: int
    avg_mag: float
    max_mag: float
    first_event_time: datetime
    last_event_time: datetime
    lat_grid: float
    lon_grid: float
    sequence_type: str

    def to_influx_point(self, measurement: str) -> Dict[str, Any]:
        return {
            "measurement": measurement,
            "tags": {
                "region": self.region,
                "sequence_type": self.sequence_type,
                "source": "stream"
            },
            "fields": {
                "eq_count": self.eq_count,
                "avg_mag": self.avg_mag,
                "max_mag": self.max_mag,
                "lat_grid": self.lat_grid,
                "lon_grid": self.lon_grid,
                "first_event_time": self.first_event_time.isoformat(),
                "last_event_time": self.last_event_time.isoformat()
            },
            "time": self.first_event_time
        }
