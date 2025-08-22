import json
import re
from datetime import datetime
from typing import Dict, Any, Optional
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

def parse_event(json_str: str) -> Optional[Dict[str, Any]]:
    """
    Parses a JSON string from Kafka into an earthquake event dictionary.
    Enriches the event with derived fields like mag_bin, depth_bin, and region.

    Args:
        json_str: JSON string containing earthquake event data

    Returns:
        Dictionary with parsed and enriched earthquake event data, or None if parsing fails
    """
    try:
        event = json.loads(json_str)

        # Basic validation
        if not event.get("id") or event.get("mag") is None:
            return None

        # Parse time
        event_time = parse_time_field(event.get("time"))
        if not event_time:
            return None

        # Extract derived fields
        event["event_time"] = event_time.isoformat()
        event["year"] = event_time.year
        event["month"] = event_time.month
        event["week"] = event_time.isocalendar()[1]  # ISO week number
        event["date"] = event_time.strftime("%Y-%m-%d")

        # Add magnitude bin
        mag = float(event["mag"])
        event["mag_bin"] = get_magnitude_bin(mag)

        # Add depth bin
        depth = event.get("depth")
        if depth is not None:
            event["depth_bin"] = get_depth_bin(float(depth))
        else:
            event["depth_bin"] = "unknown"

        # Extract region from place
        place = event.get("place", "")
        event["region"] = get_region_from_place(place)

        return event

    except Exception as e:
        # Log error but don't raise to avoid breaking the stream
        import logging
        logging.error(f"Error parsing event: {e}")
        return None

def parse_time_field(time_value) -> Optional[datetime]:
    """
    Parses time field which can be Unix timestamp (ms) or ISO string.
    
    Args:
        time_value: Time value from the event (string or number)
        
    Returns:
        datetime object or None if parsing fails
    """
    if time_value is None:
        return None

    try:
        # Try parsing as Unix timestamp (milliseconds)
        if isinstance(time_value, (int, float)) or str(time_value).isdigit():
            timestamp_ms = float(time_value)
            return datetime.utcfromtimestamp(timestamp_ms / 1000.0)

        # Try parsing as ISO string
        time_str = str(time_value)
        # Handle various ISO formats
        for fmt in [
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ", 
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S"
        ]:
            try:
                return datetime.strptime(time_str, fmt)
            except ValueError:
                continue

        return None

    except Exception:
        return None

def get_magnitude_bin(magnitude: float) -> str:
    """
    Categorizes earthquake magnitude into bins.

    Args:
        magnitude: Earthquake magnitude

    Returns:
        String representation of magnitude bin (e.g., "4.0-4.9")
    """
    if magnitude < 0:
        return "unknown"

    lower = int(magnitude)
    upper = lower + 0.9
    return f"{lower}.0-{upper:.1f}"

def get_depth_bin(depth: float) -> str:
    """
    Categorizes earthquake depth into bins.

    Args:
        depth: Earthquake depth in km

    Returns:
        String representation of depth bin
    """
    if depth < 0:
        return "unknown"
    elif depth < 70:
        return "shallow"
    elif depth < 300:
        return "intermediate"
    else:
        return "deep"

def get_region_from_place(place: str) -> str:
    """
    Extracts region from the place string.
    Looks for text after comma or "of", cleans up "region" suffix.

    Args:
        place: Place string from earthquake data

    Returns:
        Extracted region name or "Unknown" if extraction fails
    """
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

def clean_region_name(region: str) -> str:
    """
    Cleans up region name by removing common suffixes.

    Args:
        region: Raw region name

    Returns:
        Cleaned region name
    """
    if not region:
        return "Unknown"

    # Remove "region" suffix (case insensitive)
    cleaned = re.sub(r"\s+region$", "", region.strip(), flags=re.IGNORECASE)

    return cleaned if cleaned else "Unknown"

class DeduplicateById(KeyedProcessFunction):
    """
    Keyed process function that deduplicates events by ID.
    Only outputs the first occurrence of each event ID.
    """

    def __init__(self):
        self.seen_state = None

    def open(self, runtime_context: 'RuntimeContext'):
        """Initialize the state for tracking seen event IDs."""
        state_descriptor = ValueStateDescriptor(
            "seen_events",
            Types.BOOLEAN()
        )
        self.seen_state = runtime_context.get_state(state_descriptor)

    def process_element(self, value: Dict[str, Any], ctx: 'KeyedProcessFunction.Context'):
        """
        Process each event and output only if not seen before.

        Args:
            value: Event dictionary
            ctx: Processing context
        """
        try:
            # Check if we've seen this event ID before
            seen = self.seen_state.value()

            if seen is None or not seen:
                # Mark as seen and output the event
                self.seen_state.update(True)
                yield value
        except Exception as e:
            import logging
            logging.error(f"Error in deduplication: {e}")
            # In case of error, output the event to avoid data loss
            yield value
