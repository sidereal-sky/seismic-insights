import requests


def download_data(
    starttime: str,
    endtime: str,
    min_magnitude: float = 4.0,
    output_file: str = "usgs_earthquakes.csv",
    user_agent: str = "SeismicInsight/1.0 (test@gmail.com)"
) -> None:
    """
    Downloads earthquake data from the USGS API and saves it as a CSV file.

    Args:
        starttime (str): Start date in YYYY-MM-DD format
        endtime (str): End date in YYYY-MM-DD format
        min_magnitude (float): Minimum magnitude to filter earthquakes
        output_file (str): Path to save the downloaded CSV
        user_agent (str): User-Agent header to pass to USGS (required)
    """

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "csv",
        "starttime": starttime,
        "endtime": endtime,
        "minmagnitude": min_magnitude,
        "orderby": "time"
    }

    headers = {
        "User-Agent": user_agent
    }

    print(f"Downloading earthquake data from {starttime} to {endtime} (M ≥ {min_magnitude})...")
    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200 and response.content:
        with open(output_file, "wb") as f:
            f.write(response.content)
        print(f"File saved to {output_file}")
    else:
        raise Exception(f"Failed to download data: {response.status_code} — {response.text}")
