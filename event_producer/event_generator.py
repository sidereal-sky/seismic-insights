import random
from datetime import datetime, timedelta
from typing import Dict, Any

MAGNITUDE_TYPES = ['mb', 'mww', 'mwr', 'ml', 'md']
NETWORKS = ['us', 'ak', 'ci', 'nn', 'uu', 'uw', 'pr', 'hv', 'nc']
EVENT_TYPES = ['earthquake', 'quarry blast', 'explosion', 'mining explosion']
PLACES = [
    "N of Constanta, Romania",
    "S of Bucharest, Romania", 
    "E of Timisoara, Romania",
    "W of Brasov, Romania",
    "NE of Cluj-Napoca, Romania",
    "SW of Oradea, Romania"
]

class SeismicEventGenerator:

    def generate_event(self):
        """
        Generate a synthetic seismic event with random values.
        Returns: Dictionary containing a complete earthquake event
        """
        event_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        latitude = round(random.uniform(-90, 90), 4)
        longitude = round(random.uniform(-180, 180), 4)
        depth = round(random.uniform(0, 400), 3)
        mag = round(random.uniform(1.0, 9.0), 1)
        magType = random.choice(MAGNITUDE_TYPES)
        nst = random.randint(1, 200)
        gap = random.randint(1, 300)
        dmin = round(random.uniform(0.01, 50.0), 3)
        rms = round(random.uniform(0.01, 2.0), 2)
        net = random.choice(NETWORKS)
        event_id = net + event_time + str(latitude) + str(longitude) + str(depth) + str(mag) + magType + str(nst) + str(gap) + str(dmin) + str(rms)
        updated_time = event_time
        place = str(random.randint(1, 100)) + " " + random.choice(PLACES)
        event_type = random.choice(EVENT_TYPES)
        horizontal_error = round(random.uniform(0.1, 10.0), 2)
        depth_error = round(random.uniform(0.5, 10.0), 3)
        mag_error = round(random.uniform(0.001, 0.2), 3)
        mag_nst = random.randint(1, 1000)
        status = 'reviewed'
        location_source = 'us'
        mag_source = 'us'

        # Create complete event with random values
        event = {
            'time': event_time,
            'latitude': latitude,
            'longitude': longitude,
            'depth': depth,
            'mag': mag,
            'magType': magType,
            'nst': nst,
            'gap': gap,
            'dmin': dmin,
            'rms': rms,
            'net': net,
            'id': event_id,
            'updated': updated_time,
            'place': place,
            'type': event_type,
            'horizontalError': horizontal_error,
            'depthError': depth_error,
            'magError': mag_error,
            'magNst': mag_nst,
            'status': status,
            'locationSource': location_source,
            'magSource': mag_source
        }

        return event
