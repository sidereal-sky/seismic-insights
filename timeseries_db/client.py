from typing import List, Dict, Any, Optional
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

class SeismicInfluxDBClient:
    def __init__(self, url: str, token: str, org: str, bucket: str, timeout: int = 30000):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.timeout = timeout
        self._client = None
        self._write_api = None
        self._query_api = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def connect(self):
        self._client = InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org,
            timeout=self.timeout
        )
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        self._query_api = self._client.query_api()
    
    def close(self):
        if self._client:
            self._client.close()
    
    def write_points(self, points: List[Dict[str, Any]]):
        if not self._write_api:
            raise RuntimeError("Client not connected. Call connect() first.")
        
        influx_points = []
        for point_data in points:
            point = Point(point_data["measurement"])
            
            for tag_key, tag_value in point_data.get("tags", {}).items():
                if tag_value is not None:
                    point = point.tag(tag_key, str(tag_value))
            
            for field_key, field_value in point_data.get("fields", {}).items():
                if field_value is not None:
                    point = point.field(field_key, field_value)
            
            if "time" in point_data:
                point = point.time(point_data["time"])
            
            influx_points.append(point)
        
        self._write_api.write(bucket=self.bucket, record=influx_points)
    
    def write_point(self, point_data: Dict[str, Any]):
        self.write_points([point_data])
    
    def query(self, flux_query: str) -> List[Dict]:
        if not self._query_api:
            raise RuntimeError("Client not connected. Call connect() first.")
        
        result = self._query_api.query(flux_query)
        records = []
        for table in result:
            for record in table.records:
                records.append(record.values)
        return records
    
    def create_bucket_if_not_exists(self, bucket_name: Optional[str] = None) -> bool:
        bucket_name = bucket_name or self.bucket
        buckets_api = self._client.buckets_api()
        bucket = buckets_api.find_bucket_by_name(bucket_name)
        if bucket is None:
            buckets_api.create_bucket(bucket_name=bucket_name, org=self.org)
            return True
        return False
