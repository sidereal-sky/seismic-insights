from pyflink.datastream.functions import AggregateFunction

class CountAvgMaxAgg(AggregateFunction):
    """AggregateFunction that computes count, avg(mag), max(mag)."""

    def create_accumulator(self):
        return {"count": 0, "sum_mag": 0.0, "max_mag": float("-inf")}

    def add(self, value, acc):
        acc["count"] += 1
        acc["sum_mag"] += value["mag"]
        acc["max_mag"] = max(acc["max_mag"], value["mag"])
        return acc

    def get_result(self, acc):
        return {
            "eq_count": acc["count"],
            "avg_mag": round(acc["sum_mag"] / acc["count"], 3) if acc["count"] > 0 else None,
            "max_mag": round(acc["max_mag"], 3) if acc["max_mag"] > float("-inf") else None,
        }

    def merge(self, acc1, acc2):
        return {
            "count": acc1["count"] + acc2["count"],
            "sum_mag": acc1["sum_mag"] + acc2["sum_mag"],
            "max_mag": max(acc1["max_mag"], acc2["max_mag"])
        }


class CountOnlyAgg(AggregateFunction):
    """AggregateFunction that computes just the count."""

    def create_accumulator(self):
        return {"count": 0}

    def add(self, value, acc):
        acc["count"] += 1
        return acc

    def get_result(self, acc):
        return {"eq_count": acc["count"]}

    def merge(self, acc1, acc2):
        return {"count": acc1["count"] + acc2["count"]}


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
            "eq_count": acc["count"],
            "avg_mag": round(acc["avg_mag"], 3) if acc["count"] > 0 else None,
            "max_mag": round(acc["max_mag"], 3) if acc["max_mag"] > float("-inf") else None,
            "first_event_time": acc["first_time"],
            "last_event_time": acc["last_time"],
            "duration_minutes": self._calculate_duration(acc["first_time"], acc["last_time"]),
            "region": acc["region"],
            "lat_grid": acc["lat_grid"],
            "lon_grid": acc["lon_grid"],
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
