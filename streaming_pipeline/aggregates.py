from pyflink.datastream.functions import AggregateFunction
from datetime import datetime

class GlobalCountAvgMaxAgg(AggregateFunction):
    """AggregateFunction for global statistics: count, avg(mag), max(mag)."""

    def create_accumulator(self):
        return {
            "count": 0,
            "sum_mag": 0.0,
            "max_mag": None,
            "min_event_time": None,
            "max_event_time": None
        }

    def add(self, value, acc):
        acc["count"] += 1
        mag = float(value["mag"])
        acc["sum_mag"] += mag
        acc["max_mag"] = max(acc["max_mag"], mag) if acc["max_mag"] is not None else mag

        event_time = value.get("event_time")
        if event_time:
            if acc["min_event_time"] is None or event_time < acc["min_event_time"]:
                acc["min_event_time"] = event_time
            if acc["max_event_time"] is None or event_time > acc["max_event_time"]:
                acc["max_event_time"] = event_time

        return acc

    def get_result(self, acc):
        result = {
            "eq_count": str(acc["count"]),
            "avg_mag": str(round(acc["sum_mag"] / acc["count"], 2)) if acc["count"] > 0 else "0",
            "max_mag": str(round(acc["max_mag"], 2)) if acc["max_mag"] is not None else "0",
            "region": "global",
            "min_event_time": acc["min_event_time"] or "unknown",
            "max_event_time": acc["max_event_time"] or "unknown"
        }

        return result

    def merge(self, acc1, acc2):
        merged_min = None
        if acc1["min_event_time"] and acc2["min_event_time"]:
            merged_min = min(acc1["min_event_time"], acc2["min_event_time"])
        elif acc1["min_event_time"]:
            merged_min = acc1["min_event_time"]
        elif acc2["min_event_time"]:
            merged_min = acc2["min_event_time"]

        merged_max = None
        if acc1["max_event_time"] and acc2["max_event_time"]:
            merged_max = max(acc1["max_event_time"], acc2["max_event_time"])
        elif acc1["max_event_time"]:
            merged_max = acc1["max_event_time"]
        elif acc2["max_event_time"]:
            merged_max = acc2["max_event_time"]

        return {
            "count": acc1["count"] + acc2["count"],
            "sum_mag": acc1["sum_mag"] + acc2["sum_mag"],
            "max_mag": max(acc1["max_mag"], acc2["max_mag"]),
            "min_event_time": merged_min,
            "max_event_time": merged_max
        }

class RegionalCountAvgMaxAgg(AggregateFunction):
    """AggregateFunction for regional statistics: count, avg(mag), max(mag)."""

    def create_accumulator(self):
        return {
            "count": 0,
            "sum_mag": 0.0,
            "max_mag": None,
            "region": None,
            "min_event_time": None,
            "max_event_time": None
        }

    def add(self, value, acc):
        acc["count"] += 1
        mag = float(value["mag"])
        acc["sum_mag"] += mag
        acc["max_mag"] = max(acc["max_mag"], mag) if acc["max_mag"] is not None else mag
        if acc["region"] is None:
            acc["region"] = value.get("region", "unknown")

        event_time = value.get("event_time")
        if event_time:
            if acc["min_event_time"] is None or event_time < acc["min_event_time"]:
                acc["min_event_time"] = event_time
            if acc["max_event_time"] is None or event_time > acc["max_event_time"]:
                acc["max_event_time"] = event_time

        return acc

    def get_result(self, acc):
        return {
            "eq_count": str(acc["count"]),
            "avg_mag": str(round(acc["sum_mag"] / acc["count"], 2)) if acc["count"] > 0 else "0",
            "max_mag": str(round(acc["max_mag"], 2)) if acc["max_mag"] is not None else "0",
            "region": acc["region"] or "unknown",
            "min_event_time": acc["min_event_time"] or "unknown",
            "max_event_time": acc["max_event_time"] or "unknown"
        }

    def merge(self, acc1, acc2):
        merged_min = None
        if acc1["min_event_time"] and acc2["min_event_time"]:
            merged_min = min(acc1["min_event_time"], acc2["min_event_time"])
        elif acc1["min_event_time"]:
            merged_min = acc1["min_event_time"]
        elif acc2["min_event_time"]:
            merged_min = acc2["min_event_time"]

        merged_max = None
        if acc1["max_event_time"] and acc2["max_event_time"]:
            merged_max = max(acc1["max_event_time"], acc2["max_event_time"])
        elif acc1["max_event_time"]:
            merged_max = acc1["max_event_time"]
        elif acc2["max_event_time"]:
            merged_max = acc2["max_event_time"]

        return {
            "count": acc1["count"] + acc2["count"],
            "sum_mag": acc1["sum_mag"] + acc2["sum_mag"],
            "max_mag": max(acc1["max_mag"], acc2["max_mag"]),
            "region": acc1["region"] or acc2["region"],
            "min_event_time": merged_min,
            "max_event_time": merged_max
        }

class GlobalCountOnlyAgg(AggregateFunction):
    """AggregateFunction for global count."""

    def create_accumulator(self):
        return {"count": 0, "min_event_time": None, "max_event_time": None}

    def add(self, value, acc):
        acc["count"] += 1

        event_time = value.get("event_time")
        if event_time:
            if acc["min_event_time"] is None or event_time < acc["min_event_time"]:
                acc["min_event_time"] = event_time
            if acc["max_event_time"] is None or event_time > acc["max_event_time"]:
                acc["max_event_time"] = event_time

        return acc

    def get_result(self, acc):
        return {
            "eq_count": str(acc["count"]),
            "region": "global",
            "min_event_time": acc["min_event_time"] or "unknown",
            "max_event_time": acc["max_event_time"] or "unknown"
        }

    def merge(self, acc1, acc2):
        merged_min = None
        if acc1["min_event_time"] and acc2["min_event_time"]:
            merged_min = min(acc1["min_event_time"], acc2["min_event_time"])
        elif acc1["min_event_time"]:
            merged_min = acc1["min_event_time"]
        elif acc2["min_event_time"]:
            merged_min = acc2["min_event_time"]

        merged_max = None
        if acc1["max_event_time"] and acc2["max_event_time"]:
            merged_max = max(acc1["max_event_time"], acc2["max_event_time"])
        elif acc1["max_event_time"]:
            merged_max = acc1["max_event_time"]
        elif acc2["max_event_time"]:
            merged_max = acc2["max_event_time"]

        return {
            "count": acc1["count"] + acc2["count"],
            "min_event_time": merged_min,
            "max_event_time": merged_max
        }

class MagnitudeBinCountAgg(AggregateFunction):
    """AggregateFunction for magnitude bin counting."""

    def create_accumulator(self):
        return {"count": 0, "mag_bin": None, "min_event_time": None, "max_event_time": None}

    def add(self, value, acc):
        acc["count"] += 1
        if acc["mag_bin"] is None:
            acc["mag_bin"] = value.get("mag_bin", "unknown")

        event_time = value.get("event_time")
        if event_time:
            if acc["min_event_time"] is None or event_time < acc["min_event_time"]:
                acc["min_event_time"] = event_time
            if acc["max_event_time"] is None or event_time > acc["max_event_time"]:
                acc["max_event_time"] = event_time

        return acc

    def get_result(self, acc):
        return {
            "eq_count": str(acc["count"]),
            "mag_bin": acc["mag_bin"] or "unknown",
            "min_event_time": acc["min_event_time"] or "unknown",
            "max_event_time": acc["max_event_time"] or "unknown"
        }

    def merge(self, acc1, acc2):
        merged_min = None
        if acc1["min_event_time"] and acc2["min_event_time"]:
            merged_min = min(acc1["min_event_time"], acc2["min_event_time"])
        elif acc1["min_event_time"]:
            merged_min = acc1["min_event_time"]
        elif acc2["min_event_time"]:
            merged_min = acc2["min_event_time"]

        merged_max = None
        if acc1["max_event_time"] and acc2["max_event_time"]:
            merged_max = max(acc1["max_event_time"], acc2["max_event_time"])
        elif acc1["max_event_time"]:
            merged_max = acc1["max_event_time"]
        elif acc2["max_event_time"]:
            merged_max = acc2["max_event_time"]

        return {
            "count": acc1["count"] + acc2["count"],
            "mag_bin": acc1["mag_bin"] or acc2["mag_bin"],
            "min_event_time": merged_min,
            "max_event_time": merged_max
        }

class DepthPatternAgg(AggregateFunction):
    """AggregateFunction for regional depth patterns."""

    def create_accumulator(self):
        return {"count": 0, "region": None, "depth_bin": None, "min_event_time": None, "max_event_time": None}

    def add(self, value, acc):
        acc["count"] += 1
        if acc["region"] is None:
            acc["region"] = value.get("region", "unknown")
        if acc["depth_bin"] is None:
            acc["depth_bin"] = value.get("depth_bin", "unknown")

        event_time = value.get("event_time")
        if event_time:
            if acc["min_event_time"] is None or event_time < acc["min_event_time"]:
                acc["min_event_time"] = event_time
            if acc["max_event_time"] is None or event_time > acc["max_event_time"]:
                acc["max_event_time"] = event_time

        return acc

    def get_result(self, acc):
        return {
            "eq_count": str(acc["count"]),
            "region": acc["region"] or "unknown",
            "depth_bin": acc["depth_bin"] or "unknown",
            "min_event_time": acc["min_event_time"] or "unknown",
            "max_event_time": acc["max_event_time"] or "unknown"
        }

    def merge(self, acc1, acc2):
        merged_min = None
        if acc1["min_event_time"] and acc2["min_event_time"]:
            merged_min = min(acc1["min_event_time"], acc2["min_event_time"])
        elif acc1["min_event_time"]:
            merged_min = acc1["min_event_time"]
        elif acc2["min_event_time"]:
            merged_min = acc2["min_event_time"]

        merged_max = None
        if acc1["max_event_time"] and acc2["max_event_time"]:
            merged_max = max(acc1["max_event_time"], acc2["max_event_time"])
        elif acc1["max_event_time"]:
            merged_max = acc1["max_event_time"]
        elif acc2["max_event_time"]:
            merged_max = acc2["max_event_time"]

        return {
            "count": acc1["count"] + acc2["count"],
            "region": acc1["region"] or acc2["region"],
            "depth_bin": acc1["depth_bin"] or acc2["depth_bin"],
            "min_event_time": merged_min,
            "max_event_time": merged_max
        }

class SequenceDetectionAgg(AggregateFunction):
    """AggregateFunction for sequence detection."""

    def create_accumulator(self):
        return {
            "count": 0,
            "sum_mag": 0.0,
            "max_mag": None,
            "region": None,
            "lat_grid": None,
            "lon_grid": None,
            "min_event_time": None,
            "max_event_time": None,
        }

    def add(self, value, acc):
        acc["count"] += 1
        mag = float(value["mag"])
        acc["sum_mag"] += mag
        acc["max_mag"] = max(acc["max_mag"], mag) if acc["max_mag"] is not None else mag
        if acc["region"] is None:
            acc["region"] = value.get("region", "unknown")
            acc["lat_grid"] = int(float(value["latitude"]) / 10)
            acc["lon_grid"] = int(float(value["longitude"]) / 10)

        event_time = value.get("event_time")
        if event_time:
            if acc["min_event_time"] is None or event_time < acc["min_event_time"]:
                acc["min_event_time"] = event_time
            if acc["max_event_time"] is None or event_time > acc["max_event_time"]:
                acc["max_event_time"] = event_time

        return acc

    def get_result(self, acc):
        return {
            "eq_count": str(acc["count"]),
            "avg_mag": str(round(acc["sum_mag"] / acc["count"], 2)) if acc["count"] > 0 else "0",
            "max_mag": str(round(acc["max_mag"], 2)) if acc["max_mag"] is not None else "0",
            "region": acc["region"] or "unknown",
            "min_event_time": acc["min_event_time"] or "unknown",
            "max_event_time": acc["max_event_time"] or "unknown",
            "lat_grid": str(acc["lat_grid"]) if acc["lat_grid"] is not None else "0",
            "lon_grid": str(acc["lon_grid"]) if acc["lon_grid"] is not None else "0",
            "sequence_type": SequenceDetectionAgg.classify_sequence(acc)
        }

    def merge(self, acc1, acc2):
        merged_min = None
        if acc1["min_event_time"] and acc2["min_event_time"]:
            merged_min = min(acc1["min_event_time"], acc2["min_event_time"])
        elif acc1["min_event_time"]:
            merged_min = acc1["min_event_time"]
        elif acc2["min_event_time"]:
            merged_min = acc2["min_event_time"]

        merged_max = None
        if acc1["max_event_time"] and acc2["max_event_time"]:
            merged_max = max(acc1["max_event_time"], acc2["max_event_time"])
        elif acc1["max_event_time"]:
            merged_max = acc1["max_event_time"]
        elif acc2["max_event_time"]:
            merged_max = acc2["max_event_time"]

        return {
            "count": acc1["count"] + acc2["count"],
            "sum_mag": acc1["sum_mag"] + acc2["sum_mag"],
            "max_mag": max(acc1["max_mag"], acc2["max_mag"]),
            "region": acc1["region"] or acc2["region"],
            "min_event_time": merged_min,
            "max_event_time": merged_max,
            "lat_grid": acc1["lat_grid"] or acc2["lat_grid"],
            "lon_grid": acc1["lon_grid"] or acc2["lon_grid"]
        }

    @staticmethod
    def classify_sequence(acc):
        """Classify the type of earthquake sequence."""
        if acc["count"] < 3:
            return "isolated_sequence"
        elif acc["count"] >= 10:
            return "swarm"
        elif acc["max_mag"] is not None and acc["max_mag"] > 6.0:
            return "aftershock"
        else:
            return "general_sequence"
