# SeismicInsights - Batch Pipeline (Spark)

## 1. Overview

This project processes global earthquake data from the USGS API and produces aggregated statistics, that are sotred in csv files.

The pipeline:
1. Downloads raw earthquake events (CSV) from USGS.
2. Parses timestamps and standardizes them into UTC.
3. Filters out duplicates and earthquakes below a minimum magnitude.
4. Derives additional fields:
   - **year, month, week, date** (from event timestamp)
   - **mag_bin** (magnitude ranges: 4.0–4.9, 5.0–5.9, etc.)
   - **depth_bin** (shallow 0–70 km, intermediate 70–300 km, deep >300 km)
   - **region** (extracted from the event `place` field)

Finally, it computes a set of **aggregates** and writes them as CSV files in the `output/` folder.



## 2. Aggregates Produced

### 2.1. Monthly Region Stats
**File:** `output/stats_monthly_region`

Groups earthquakes by year, month, and region.  
For each group it calculates:
- `eq_count` – number of earthquakes
- `avg_mag` – average magnitude
- `max_mag` – strongest earthquake magnitude in that region/month

**Use case:** monitor regional activity trends over time.

---

### 2.2. Weekly Region Stats
**File:** `output/stats_weekly_region`

Similar to monthly, but grouped by **year, week, region**.  
Shows how seismic activity changes week to week.


---

### 2.3. Daily Global Stats
**File:** `output/stats_daily_global`

Aggregated at the **global level** per day.  
Includes:
- `eq_count` – earthquakes per day worldwide
- `avg_mag` – average daily magnitude
- `max_mag` – strongest earthquake on that day

**Use case:** provides a global activity timeline.

---

### 2.4. Monthly Magnitude Distribution
**File:** `output/stats_monthly_magnitude`

Groups by **year, month, region, magnitude bin**.  
Counts how many earthquakes fall into each magnitude category.

**Use case:** show distribution of earthquake sizes over time.

---

### 2.5. Monthly Depth Distribution
**File:** `output/stats_monthly_depth`

Groups by **year, month, region, depth_bin**.  
Counts how many earthquakes fall into shallow, intermediate, or deep categories.

**Use case:** analyze whether a region is experiencing mostly shallow vs. deep quakes, which can indicate different tectonic processes.



## 3. Running the Script

Edit constants for the script:
- `START_DATE`, `END_DATE` – date range in `YYYY-MM-DD`
- `MIN_MAGNITUDE` – filter threshold for minimum magnitude
- `INPUT_FILE` – where raw USGS CSV is saved
- `OUTPUT_FILE` – prefix for result CSV files (default `output/stats`)

Run:
```bash
cd batch_pipeline/
pip install -r requirements.txt
python main.py
