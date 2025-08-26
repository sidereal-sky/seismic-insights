from pyflink.datastream.functions import AggregateFunction

class CountAvgMaxAgg(AggregateFunction):
    """AggregateFunction that computes count, avg(mag), max(mag)."""

    def create_accumulator(self):
        return {"count": 0, "sum_mag": 0.0, "max_mag": float("-inf"), "region": None, "date": None}

    def add(self, value, acc):
        acc["count"] += 1
        mag = float(value["mag"])  # Convert string back to float for calculations
        acc["sum_mag"] += mag
        acc["max_mag"] = max(acc["max_mag"], mag)
        if acc["region"] is None:
            acc["region"] = value.get("region", "unknown")
        if acc["date"] is None:
            acc["date"] = value.get("date", "unknown")
        return acc

    def get_result(self, acc):
        return {
            "eq_count": str(acc["count"]),
            "avg_mag": str(round(acc["sum_mag"] / acc["count"], 3)) if acc["count"] > 0 else "0",
            "max_mag": str(round(acc["max_mag"], 3)) if acc["max_mag"] > float("-inf") else "0",
            "region": acc["region"] or "unknown",
            "date": acc["date"] or "unknown"
        }

    def merge(self, acc1, acc2):
        return {
            "count": acc1["count"] + acc2["count"],
            "sum_mag": acc1["sum_mag"] + acc2["sum_mag"],
            "max_mag": max(acc1["max_mag"], acc2["max_mag"]),
            "region": acc1["region"] or acc2["region"],
            "date": acc1["date"] or acc2["date"]
        }


class CountOnlyAgg(AggregateFunction):
    """AggregateFunction that computes just the count."""

    def create_accumulator(self):
        return {"count": 0, "region": None, "depth_bin": None, "mag_bin": None}

    def add(self, value, acc):
        acc["count"] += 1
        if acc["region"] is None:
            acc["region"] = value.get("region", "unknown")
        if acc["depth_bin"] is None:
            acc["depth_bin"] = value.get("depth_bin", "unknown")
        if acc["mag_bin"] is None:
            acc["mag_bin"] = value.get("mag_bin", "unknown")
        return acc

    def get_result(self, acc):
        return {
            "eq_count": str(acc["count"]),
            "region": acc["region"] or "unknown",
            "depth_bin": acc["depth_bin"] or "unknown",
            "mag_bin": acc["mag_bin"] or "unknown"
        }

    def merge(self, acc1, acc2):
        return {
            "count": acc1["count"] + acc2["count"],
            "region": acc1["region"] or acc2["region"],
            "depth_bin": acc1["depth_bin"] or acc2["depth_bin"],
            "mag_bin": acc1["mag_bin"] or acc2["mag_bin"]
        }


class SequenceDetectionAgg(AggregateFunction):
    """
    AggregateFunction that collects events for sequence detection.
    Tracks count, first/last event times, and location details.
    """

    def create_accumulator(self):
        return {
            "count": 0,
            "first_time": None,
            "last_time": None,
            "max_mag": float("-inf"),
            "avg_mag": 0.0,
            "sum_mag": 0.0,
            "region": None,
            "lat_grid": None,
            "lon_grid": None
        }

    def add(self, value, acc):
        mag = float(value["mag"])
        event_time = value["event_time"]

        acc["count"] += 1
        acc["sum_mag"] += mag
        acc["avg_mag"] = acc["sum_mag"] / acc["count"]
        acc["max_mag"] = max(acc["max_mag"], mag)

        if acc["first_time"] is None or event_time < acc["first_time"]:
            acc["first_time"] = event_time
        if acc["last_time"] is None or event_time > acc["last_time"]:
            acc["last_time"] = event_time

        if acc["region"] is None:
            acc["region"] = value["region"]
            acc["lat_grid"] = int(float(value["latitude"]) * 10)
            acc["lon_grid"] = int(float(value["longitude"]) * 10)

        return acc

    def get_result(self, acc):
        return {
            "eq_count": str(acc["count"]),
            "avg_mag": str(round(acc["avg_mag"], 3)) if acc["count"] > 0 else "0",
            "max_mag": str(round(acc["max_mag"], 3)) if acc["max_mag"] > float("-inf") else "0",
            "first_event_time": acc["first_time"] if acc["first_time"] else "",
            "last_event_time": acc["last_time"] if acc["last_time"] else "",
            "duration_minutes": str(self._calculate_duration(acc["first_time"], acc["last_time"])),
            "region": acc["region"] if acc["region"] else "unknown",
            "lat_grid": str(acc["lat_grid"]) if acc["lat_grid"] is not None else "0",
            "lon_grid": str(acc["lon_grid"]) if acc["lon_grid"] is not None else "0",
            "sequence_type": self._classify_sequence(acc)
        }

    def merge(self, acc1, acc2):
        if acc1["count"] == 0:
            return acc2
        if acc2["count"] == 0:
            return acc1

        return {
            "count": acc1["count"] + acc2["count"],
            "sum_mag": acc1["sum_mag"] + acc2["sum_mag"],
            "avg_mag": (acc1["sum_mag"] + acc2["sum_mag"]) / (acc1["count"] + acc2["count"]),
            "max_mag": max(acc1["max_mag"], acc2["max_mag"]),
            "first_time": min(acc1["first_time"], acc2["first_time"]) if acc1["first_time"] and acc2["first_time"] else (acc1["first_time"] or acc2["first_time"]),
            "last_time": max(acc1["last_time"], acc2["last_time"]) if acc1["last_time"] and acc2["last_time"] else (acc1["last_time"] or acc2["last_time"]),
            "region": acc1["region"] or acc2["region"],
            "lat_grid": acc1["lat_grid"] or acc2["lat_grid"],
            "lon_grid": acc1["lon_grid"] or acc2["lon_grid"]
        }

    def _calculate_duration(self, first_time, last_time):
        """Calculate duration between first and last event in minutes."""
        if not first_time or not last_time:
            return 0
        try:
            from datetime import datetime
            if isinstance(first_time, str):
                first_dt = datetime.fromisoformat(first_time.replace('Z', '+00:00'))
                last_dt = datetime.fromisoformat(last_time.replace('Z', '+00:00'))
            else:
                first_dt = first_time
                last_dt = last_time
            return int((last_dt - first_dt).total_seconds() / 60)
        except Exception:
            return 0

    def _classify_sequence(self, acc):
        """Classify the type of earthquake sequence."""
        if acc["count"] < 3:
            return "isolated"
        elif acc["count"] >= 10:
            return "swarm"
        elif acc["max_mag"] > 6.0:
            return "mainshock_sequence"
        else:
            return "cluster"
