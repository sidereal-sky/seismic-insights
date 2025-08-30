import json
import re
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO)

def parse_event(json_str):
    try:
        event = json.loads(json_str)

        if not event.get("id") or not event.get("mag"):
            return None

        event_time = parse_time_field(event.get("time"))
        if not event_time:
            return None

        event["event_time"] = event_time.isoformat()
        event["year"] = event_time.year
        event["month"] = event_time.month
        event["week"] = event_time.isocalendar()[1]  # ISO week number
        event["date"] = event_time.strftime("%Y-%m-%d")

        event["mag_bin"] = get_magnitude_bin(float(event["mag"]))

        depth = event.get("depth")
        if not depth:
            event["depth_bin"] = "unknown"
        else:
            event["depth_bin"] = get_depth_bin(float(depth))

        event["region"] = get_region_from_place(event.get("place"))

        for key, value in event.items():
            if value is not None and not isinstance(value, str):
                # convert non-string values to string
                event[key] = str(value)

        return event

    except Exception as e:
        logging.error(f"Error parsing event: {e}")
        return None

def parse_time_field(time_value):
    if not time_value:
        return None
    try:
        return datetime.strptime(time_value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except Exception:
        return None

def get_magnitude_bin(magnitude):
    if magnitude is None or magnitude < 0:
        return "unknown"

    lower = int(magnitude)
    upper = lower + 0.9
    return f"{lower}.0-{upper:.1f}"

def get_depth_bin(depth):
    if depth is None or depth < 0:
        return "unknown"
    elif depth < 70:
        return "shallow"
    elif depth < 300:
        return "intermediate"
    else:
        return "deep"

def get_region_from_place(place):
    if not place or place.strip() == "":
        return "Unknown"

    # Look for text after comma (most common pattern)
    comma_match = re.search(r",\s*([^,]+)$", place)
    if comma_match:
        region = comma_match.group(1).strip()
        if region:
            return clean_region_name(region)

    # Look for text after "of"
    of_match = re.search(r".*\bof\s+(.+)$", place, re.IGNORECASE)
    if of_match:
        region = of_match.group(1).strip()
        if region:
            return clean_region_name(region)

    # Fallback to the whole place string
    return clean_region_name(place)

def clean_region_name(region):
    if not region:
        return "Unknown"

    # Remove "region" suffix (case insensitive)
    cleaned = re.sub(r"\s+region$", "", region.strip(), flags=re.IGNORECASE)

    return cleaned if cleaned else "Unknown"

def is_processable_event(event, min_magnitude):
    return event is not None and event.get("mag") is not None and float(event.get("mag")) >= min_magnitude

class DeduplicateById(KeyedProcessFunction):

    def __init__(self):
        self.seen_state = None

    def open(self, runtime_context: "RuntimeContext"):
        state_descriptor = ValueStateDescriptor(
            "seen_events",
            Types.BOOLEAN()
        )

        self.seen_state = runtime_context.get_state(state_descriptor)

    def process_element(self, value: Dict[str, Any], ctx: "KeyedProcessFunction.Context"):
        try:
            seen = self.seen_state.value()
            if seen is None or not seen:
                self.seen_state.update(True)
                yield value
        except Exception as e:
            logging.error(f"Error in deduplication: {e}")
            yield value
