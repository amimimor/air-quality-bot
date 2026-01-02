"""
Israel Air Quality Alert - DigitalOcean Functions
With Region/Station Filtering

Configure WATCH_REGIONS or WATCH_STATIONS to only get alerts for your area.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, List
from zoneinfo import ZoneInfo

import httpx
import redis
import yaml


# ============================================================================
# Configuration Loading
# ============================================================================

def load_aqi_config() -> dict:
    """Load AQI configuration from YAML file."""
    config_path = Path(__file__).parent / "aqi_config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f)
    # Fallback to defaults if config file missing
    return {
        "alert_levels": {"GOOD": 51, "MODERATE": 0, "LOW": -100, "VERY_LOW": -200},
        "benzene_thresholds": {"GOOD": 0.3, "MODERATE": 1.2, "LOW": 1.6, "VERY_LOW": 2.5},
        "breakpoints": {}
    }


# Load config at module level
_AQI_CONFIG = load_aqi_config()

# Israel timezone
ISRAEL_TZ = ZoneInfo("Asia/Jerusalem")


# ============================================================================
# Redis Connection
# ============================================================================

REDIS_URL = os.environ.get("REDIS_URL")


def get_redis():
    """Get Redis connection."""
    if not REDIS_URL:
        return None
    return redis.from_url(REDIS_URL, decode_responses=True, ssl_cert_reqs=None)


# ============================================================================
# Readings Cache (3 minute TTL)
# ============================================================================

READINGS_CACHE_TTL = 600  # 10 minutes (matches cron interval)


def get_cached_reading(station_id: int) -> Optional[dict]:
    """Get cached reading for a station."""
    r = get_redis()
    if not r:
        return None
    data = r.get(f"reading:{station_id}")
    if data:
        return json.loads(data)
    return None


def set_cached_reading(station_id: int, reading: dict):
    """Cache a station reading."""
    r = get_redis()
    if r:
        r.setex(f"reading:{station_id}", READINGS_CACHE_TTL, json.dumps(reading))


def get_subscribers_for_region(region: str) -> list[str]:
    """Get all phone numbers subscribed to a region."""
    r = get_redis()
    if not r:
        return []
    return list(r.smembers(f"region:{region}"))


def get_all_subscribers() -> dict[str, list[str]]:
    """Get all subscribers grouped by region."""
    r = get_redis()
    if not r:
        return {}

    subscribers = {}
    for key in r.scan_iter("region:*"):
        region = key.replace("region:", "")
        phones = list(r.smembers(key))
        if phones:
            subscribers[region] = phones
    return subscribers


def get_station_subscribers() -> dict[int, list[str]]:
    """Get all subscribers grouped by station ID."""
    r = get_redis()
    if not r:
        return {}

    subscribers = {}
    for key in r.scan_iter("station:*"):
        station_id = int(key.replace("station:", ""))
        phones = list(r.smembers(key))
        if phones:
            subscribers[station_id] = phones
    return subscribers


def get_station_subscribers_with_preferences(station_id: int) -> list[dict]:
    """Get subscribers for a station with their alert levels and hours."""
    r = get_redis()
    if not r:
        return []

    phones = r.smembers(f"station:{station_id}")
    subscribers = []
    for phone in phones:
        user_data = get_user_data(phone)
        if user_data:
            subscribers.append({
                "phone": phone,
                "level": user_data.get("level", "MODERATE"),
                "hours": user_data.get("hours", ["morning", "afternoon", "evening", "night"])
            })
    return subscribers


def get_user_data(phone: str) -> Optional[dict]:
    """Get user data including their alert level."""
    r = get_redis()
    if not r:
        return None
    data = r.hget("users", phone)
    return json.loads(data) if data else None


def get_subscribers_with_preferences(region: str) -> list[dict]:
    """Get subscribers for a region with their alert levels and hours."""
    r = get_redis()
    if not r:
        return []

    phones = r.smembers(f"region:{region}")
    subscribers = []
    for phone in phones:
        user_data = get_user_data(phone)
        if user_data:
            subscribers.append({
                "phone": phone,
                "level": user_data.get("level", "MODERATE"),
                "hours": user_data.get("hours", ["morning", "afternoon", "evening", "night"])
            })
    return subscribers


# ============================================================================
# Time Windows
# ============================================================================

TIME_WINDOWS = {
    "morning": {"start": 6, "end": 12},
    "afternoon": {"start": 12, "end": 18},
    "evening": {"start": 18, "end": 22},
    "night": {"start": 22, "end": 6},
}


def get_current_time_window() -> str:
    """Get the current time window based on Israel time."""
    now = datetime.now(ISRAEL_TZ)
    hour = now.hour

    if 6 <= hour < 12:
        return "morning"
    elif 12 <= hour < 18:
        return "afternoon"
    elif 18 <= hour < 22:
        return "evening"
    else:
        return "night"


def is_within_user_hours(user_hours: list[str]) -> bool:
    """Check if current time is within user's preferred hours."""
    current_window = get_current_time_window()
    return current_window in user_hours


# ============================================================================
# Air Quality API Configuration
# ============================================================================

AIR_API_URL = "https://air-api.sviva.gov.il/v1/envista"
AIR_SITE_URL = "https://air.sviva.gov.il/"

# Caches
_api_token_cache = {"token": None, "expires": 0}
_stations_cache = {"stations": [], "expires": 0}


def get_api_token() -> str:
    """Get a fresh API token from the air quality website."""
    import re
    import time

    # Check cache (tokens seem to last a few minutes)
    if _api_token_cache["token"] and time.time() < _api_token_cache["expires"]:
        return _api_token_cache["token"]

    try:
        response = httpx.get(AIR_SITE_URL, timeout=10.0)
        if response.status_code == 200:
            # Look for the Authorization header token (the one that works for data endpoints)
            match = re.search(r'"Authorization":\s*[\'"]ApiToken ([a-f0-9-]+)[\'"]', response.text)
            if match:
                token = match.group(1)
                _api_token_cache["token"] = token
                _api_token_cache["expires"] = time.time() + 300  # Cache for 5 minutes
                return token
    except Exception as e:
        print(f"Error fetching API token: {e}")

    # Fallback token (may not work but try anyway)
    return "dcbbd3f2-8491-4ede-b798-ce2375d4d506"


# Region ID to region code mapping
REGION_ID_MAP = {
    0: "other",      # Mobile/other
    1: "haifa",      # Haifa Bay
    2: "haifa",      # Haifa
    3: "north",      # Jezreel Valley
    4: "sharon",     # Sharon-Carmel
    5: "center",     # Ariel
    6: "center",     # Inner Lowlands (Shoham, Modiin)
    7: "tel_aviv",   # Gush Dan
    8: "jerusalem",  # Jerusalem
    9: "south",      # Dead Sea
    10: "coastal",   # Southern Coastal Plain
    11: "south",     # Negev
    12: "south",     # Eilat
    13: "north",     # North Galilee
    14: "north",     # Upper Galilee
    15: "north",     # Golan
}

REGION_HE_MAP = {
    "tel_aviv": "תל אביב",
    "center": "מרכז",
    "jerusalem": "ירושלים",
    "haifa": "חיפה",
    "south": "דרום",
    "coastal": "מישור החוף",
    "sharon": "שרון",
    "north": "צפון",
    "other": "אחר",
}

REGION_NAMES = {
    "tel_aviv": "Tel Aviv",
    "center": "Center",
    "jerusalem": "Jerusalem",
    "haifa": "Haifa",
    "south": "South",
    "coastal": "Coastal Plain",
    "sharon": "Sharon",
    "north": "North",
    "other": "Other",
}


def get_all_stations() -> list[dict]:
    """Fetch all stations from API, with caching."""
    import time

    # Check cache (refresh every 6 hours)
    if _stations_cache["stations"] and time.time() < _stations_cache["expires"]:
        return _stations_cache["stations"]

    api_token = get_api_token()
    stations = []

    try:
        response = httpx.get(
            f"{AIR_API_URL}/stations",
            headers={"Authorization": f"ApiToken {api_token}"},
            timeout=30.0,
        )

        if response.status_code == 200:
            raw_stations = response.json()
            for s in raw_stations:
                if not s.get("active", False):
                    continue  # Skip inactive stations

                region_id = s.get("regionId", 0)
                region = REGION_ID_MAP.get(region_id, "other")
                region_he = REGION_HE_MAP.get(region, "אחר")
                raw_city = s.get("city")
                station_name = s["name"]

                # Build display name: "Station, City" if city available
                # Handle None, empty string, or literal "None" string from API
                if raw_city and raw_city != "None" and raw_city != station_name:
                    city = raw_city
                    display_name = f"{station_name}, {city}"
                else:
                    city = ""
                    display_name = station_name

                stations.append({
                    "id": s["stationId"],
                    "name": station_name,
                    "city": city,
                    "display_name": display_name,
                    "nameEn": s["name"],  # Hebrew name as fallback
                    "region": region,
                    "regionHe": region_he,
                })

            _stations_cache["stations"] = stations
            _stations_cache["expires"] = time.time() + 21600  # Cache for 6 hours
            print(f"Loaded {len(stations)} stations from API")

    except Exception as e:
        print(f"Error fetching stations: {e}")

    # If API failed and cache is empty, return empty list
    return stations if stations else _stations_cache.get("stations", [])


# Convenience function to get ALL_STATIONS (for backwards compatibility)
def get_stations_list() -> list[dict]:
    """Get the list of all available stations."""
    return get_all_stations()


# ============================================================================
# Configuration
# ============================================================================

def get_watched_stations() -> list[dict]:
    """
    Get stations to monitor based on configuration.
    
    Set via environment variables:
    - WATCH_REGIONS: Comma-separated region codes (e.g., "tel_aviv,center,jerusalem")
    - WATCH_STATIONS: Comma-separated station IDs (e.g., "1,2,3")
    
    If neither is set, monitors ALL stations.
    """
    watch_regions = os.environ.get("WATCH_REGIONS", "").lower().strip()
    watch_stations = os.environ.get("WATCH_STATIONS", "").strip()
    
    # If specific station IDs provided
    if watch_stations:
        station_ids = [int(s.strip()) for s in watch_stations.split(",") if s.strip()]
        return [s for s in ALL_STATIONS if s["id"] in station_ids]
    
    # If regions provided
    if watch_regions:
        regions = [r.strip() for r in watch_regions.split(",") if r.strip()]
        return [s for s in ALL_STATIONS if s["region"] in regions]
    
    # Default: all stations
    return ALL_STATIONS


# Israeli AQI thresholds (100=best, negative=worst)
# Alert when AQI drops BELOW these values
# Loaded from aqi_config.yaml
ALERT_LEVELS = _AQI_CONFIG.get("alert_levels", {
    "GOOD": 51, "MODERATE": 0, "LOW": -100, "VERY_LOW": -200
})

# Benzene thresholds in ppb (API returns ppb)
# WHO: No safe threshold - benzene is a known carcinogen
# Loaded from aqi_config.yaml
BENZENE_THRESHOLDS = _AQI_CONFIG.get("benzene_thresholds", {
    "GOOD": 1.0, "MODERATE": 1.55, "LOW": 2.10, "VERY_LOW": 2.64
})


# ============================================================================
# Air Quality Functions
# ============================================================================

def transform_pollutant_alias(name: str, alias: str) -> str:
    """
    Transform pollutant alias for cleaner display.
    e.g., "חלקיקים נשימים בגודל 2.5 מיקרון" -> "חלקיקים נשימים PM2.5"
    """
    # Map of pollutant names to cleaner Hebrew aliases
    ALIAS_MAP = {
        "PM2.5": "חלקיקים נשימים PM2.5",
        "PM10": "חלקיקים נשימים PM10",
        "O3": "אוזון O3",
        "NO2": "חנקן דו-חמצני NO2",
        "SO2": "גופרית דו-חמצנית SO2",
        "CO": "פחמן חד-חמצני CO",
        "NOX": "תחמוצות חנקן NOx",
        "BENZENE": "בנזן",
    }
    return ALIAS_MAP.get(name.upper(), alias)


def should_alert_benzene(benzene_ppb: float, threshold: str) -> bool:
    """Check if Benzene level should trigger an alert based on user's threshold."""
    threshold_value = BENZENE_THRESHOLDS.get(threshold, BENZENE_THRESHOLDS["MODERATE"])
    return benzene_ppb >= threshold_value


def get_benzene_level(benzene_ppb: float) -> str:
    """Get the alert level for a Benzene reading."""
    if benzene_ppb >= BENZENE_THRESHOLDS["VERY_LOW"]:
        return "VERY_LOW"
    elif benzene_ppb >= BENZENE_THRESHOLDS["LOW"]:
        return "LOW"
    elif benzene_ppb >= BENZENE_THRESHOLDS["MODERATE"]:
        return "MODERATE"
    elif benzene_ppb >= BENZENE_THRESHOLDS["GOOD"]:
        return "GOOD"
    return None  # No alert needed


def get_alert_level(aqi: float) -> str:
    """Get alert level based on Israeli AQI (100=best, negative=worst)."""
    if aqi > 50:  # sub-index 0-49 = Good
        return "GOOD"
    elif aqi >= 0:  # sub-index 50-100 = Moderate
        return "MODERATE"
    elif aqi >= -100:  # sub-index 101-200 = Unhealthy for sensitive
        return "LOW"
    else:  # sub-index > 200 = Unhealthy/Hazardous
        return "VERY_LOW"


def should_alert(aqi: float, threshold: str) -> bool:
    threshold_value = ALERT_LEVELS.get(threshold, ALERT_LEVELS["LOW"])
    return aqi < threshold_value


def calculate_sub_index(value: float, breakpoints: list) -> float:
    """
    Calculate sub-index using Israeli piecewise linear interpolation.
    breakpoints: list of (conc_low, conc_high, idx_low, idx_high)
    """
    for conc_lo, conc_hi, idx_lo, idx_hi in breakpoints:
        if conc_lo <= value <= conc_hi:
            return ((idx_hi - idx_lo) / (conc_hi - conc_lo)) * (value - conc_lo) + idx_lo
    # Above highest breakpoint
    return breakpoints[-1][3]


# Default breakpoints (fallback if config not loaded)
_DEFAULT_BREAKPOINTS = {
    "PM2.5": [[0, 18.5, 0, 49], [18.5, 37.5, 50, 100], [37.5, 84.5, 101, 200], [84.5, 130.5, 201, 300], [130.5, 165.5, 301, 400], [165.5, 200, 401, 500]],
    "PM10": [[0, 65, 0, 49], [65, 130, 50, 100], [130, 216, 101, 200], [216, 301, 201, 300], [301, 356, 301, 400], [356, 430, 401, 500]],
    "O3": [[0, 35, 0, 49], [35, 71, 50, 100], [71, 98, 101, 200], [98, 118, 201, 300], [118, 156, 301, 400], [156, 188, 401, 500]],
    "NO2": [[0, 53, 0, 49], [53, 106, 50, 100], [106, 161, 101, 200], [161, 214, 201, 300], [214, 261, 301, 400], [261, 316, 401, 500]],
    "SO2": [[0, 67, 0, 49], [67, 134, 50, 100], [134, 164, 101, 200], [164, 192, 201, 300], [192, 254, 301, 400], [254, 303, 401, 500]],
    "CO": [[0, 26, 0, 49], [26, 52, 50, 100], [52, 79, 101, 200], [79, 105, 201, 300], [105, 131, 301, 400], [131, 156, 401, 500]],
    "NOX": [[0, 250, 0, 49], [250, 500, 50, 100], [500, 751, 101, 200], [751, 1001, 201, 300], [1001, 1201, 301, 400], [1201, 1400, 401, 500]],
}

# Load breakpoints from config (converted from lists to tuples for calculate_sub_index)
BREAKPOINTS = {}
for pollutant, ranges in _AQI_CONFIG.get("breakpoints", _DEFAULT_BREAKPOINTS).items():
    BREAKPOINTS[pollutant.upper()] = [tuple(r) for r in ranges]


def calculate_aqi(pollutants: dict) -> int:
    """
    Calculate Air Quality Index using official Israeli formula.
    Israeli AQI: 100 = best, 0 = worst (inverted scale)
    Formula: AQI = 100 - max(sub_indices)

    Breakpoints loaded from aqi_config.yaml
    """
    sub_indices = []

    for pollutant, breakpoints in BREAKPOINTS.items():
        value = pollutants.get(pollutant)
        if value is not None and value >= 0:
            sub_idx = calculate_sub_index(value, breakpoints)
            sub_indices.append(sub_idx)

    if not sub_indices:
        return 50  # Default if no data

    # Israeli AQI = 100 - worst sub-index (can go negative)
    worst_sub_index = max(sub_indices)
    aqi = 100 - worst_sub_index
    return int(round(aqi))


def _fetch_single_station(station: dict, api_token: str) -> Optional[dict]:
    """Fetch a single station's reading. Used by concurrent fetcher."""
    station_id = station["id"]
    try:
        response = httpx.get(
            f"{AIR_API_URL}/stations/{station_id}/data/latest",
            headers={"Authorization": f"ApiToken {api_token}"},
            timeout=10.0,
        )

        if response.status_code != 200:
            return None

        data = response.json()
        data_list = data.get("data", [])
        if not data_list:
            return None

        channels = data_list[0].get("channels", [])
        timestamp = data_list[0].get("datetime", datetime.now(ISRAEL_TZ).isoformat())

        # Collect all pollutants with their metadata
        pollutants = {}
        pollutant_meta = {}
        for channel in channels:
            name = channel.get("name", "")
            value = channel.get("value")
            if value is not None and channel.get("valid", False):
                pollutants[name.upper()] = float(value)
                pollutant_meta[name.upper()] = {
                    "alias": channel.get("alias", name),
                    "units": channel.get("units", ""),
                }

        if not pollutants:
            return None

        aqi = calculate_aqi(pollutants)
        benzene_ppb = pollutants.get("BENZENE", 0)

        reading = {
            "station": station,
            "aqi": aqi,
            "level": get_alert_level(aqi),
            "pollutants": pollutants,
            "pollutant_meta": pollutant_meta,
            "pm25": pollutants.get("PM2.5", 0),
            "pm10": pollutants.get("PM10", 0),
            "o3": pollutants.get("O3", 0),
            "no2": pollutants.get("NO2", 0),
            "so2": pollutants.get("SO2", 0),
            "co": pollutants.get("CO", 0),
            "benzene_ppb": benzene_ppb,
            "benzene_level": get_benzene_level(benzene_ppb) if benzene_ppb else None,
            "timestamp": timestamp,
            "fetched_at": datetime.now(ISRAEL_TZ).isoformat(),
        }

        # Cache the reading
        cache_data = {k: v for k, v in reading.items() if k != "station"}
        set_cached_reading(station_id, cache_data)

        return reading

    except Exception as e:
        return None


def fetch_readings(stations: list[dict], use_cache: bool = True) -> list[dict]:
    """Fetch air quality readings from API with optional Redis caching.

    Uses concurrent requests for uncached stations (20 parallel workers).

    Args:
        stations: List of station dicts with 'id' key
        use_cache: If True, check Redis cache first (3 min TTL)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    readings = []
    stations_to_fetch = []
    api_token = get_api_token()

    # Check cache first for all stations
    cache_hits = 0
    for station in stations:
        station_id = station["id"]
        if use_cache:
            cached = get_cached_reading(station_id)
            if cached and "aqi" in cached:
                # Ensure 'level' exists (old cache entries might be missing it)
                if "level" not in cached:
                    cached["level"] = get_alert_level(cached["aqi"])
                if "timestamp" not in cached:
                    cached["timestamp"] = datetime.now(ISRAEL_TZ).isoformat()
                cached["station"] = station
                readings.append(cached)
                cache_hits += 1
                continue
        stations_to_fetch.append(station)

    print(f"Cache: {cache_hits} hits, {len(stations_to_fetch)} to fetch")

    # Fetch uncached stations concurrently with shared client
    if stations_to_fetch:
        print(f"API token: {api_token[:8]}...")
        fetch_success = 0
        fetch_fail = 0
        with httpx.Client(timeout=10.0) as client:
            def fetch_with_client(station):
                return _fetch_single_station_with_client(station, api_token, client)

            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = {executor.submit(fetch_with_client, s): s for s in stations_to_fetch}
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        readings.append(result)
                        fetch_success += 1
                    else:
                        fetch_fail += 1
        print(f"Fetch: {fetch_success} success, {fetch_fail} fail")

    return readings


def _fetch_single_station_with_client(station: dict, api_token: str, client: httpx.Client) -> Optional[dict]:
    """Fetch a single station using shared client."""
    station_id = station["id"]
    try:
        response = client.get(
            f"{AIR_API_URL}/stations/{station_id}/data/latest",
            headers={"Authorization": f"ApiToken {api_token}"},
        )

        if response.status_code != 200:
            return None

        data = response.json()
        data_list = data.get("data", [])
        if not data_list:
            return None

        channels = data_list[0].get("channels", [])
        timestamp = data_list[0].get("datetime", datetime.now(ISRAEL_TZ).isoformat())

        pollutants = {}
        pollutant_meta = {}
        for channel in channels:
            name = channel.get("name", "")
            value = channel.get("value")
            if value is not None and channel.get("valid", False):
                pollutants[name.upper()] = float(value)
                pollutant_meta[name.upper()] = {
                    "alias": channel.get("alias", name),
                    "units": channel.get("units", ""),
                }

        if not pollutants:
            return None

        aqi = calculate_aqi(pollutants)
        benzene_ppb = pollutants.get("BENZENE", 0)

        reading = {
            "station": station,
            "aqi": aqi,
            "level": get_alert_level(aqi),
            "pollutants": pollutants,
            "pollutant_meta": pollutant_meta,
            "pm25": pollutants.get("PM2.5", 0),
            "pm10": pollutants.get("PM10", 0),
            "o3": pollutants.get("O3", 0),
            "no2": pollutants.get("NO2", 0),
            "so2": pollutants.get("SO2", 0),
            "co": pollutants.get("CO", 0),
            "benzene_ppb": benzene_ppb,
            "benzene_level": get_benzene_level(benzene_ppb) if benzene_ppb else None,
            "timestamp": timestamp,
            "fetched_at": datetime.now(ISRAEL_TZ).isoformat(),
        }

        cache_data = {k: v for k, v in reading.items() if k != "station"}
        set_cached_reading(station_id, cache_data)

        return reading

    except Exception:
        return None


def get_last_alert_time(station_id: int, phone: str) -> Optional[str]:
    """Get the last time we sent an alert for this station to this user."""
    r = get_redis()
    if not r:
        return None
    return r.hget(f"last_alert:{phone}", str(station_id))


def set_last_alert_time(station_id: int, phone: str, timestamp: str):
    """Record when we sent an alert for this station to this user."""
    r = get_redis()
    if r:
        r.hset(f"last_alert:{phone}", str(station_id), timestamp)
        # Expire after 24 hours
        r.expire(f"last_alert:{phone}", 86400)


def should_send_alert(station_id: int, phone: str, current_level: str) -> bool:
    """
    Determine if we should send an alert.
    Avoids spamming by checking:
    - At least 2 hours since last alert for same station
    - Or if level got significantly worse
    """
    import time

    r = get_redis()
    if not r:
        return True  # If no Redis, always send

    last_time = get_last_alert_time(station_id, phone)
    if not last_time:
        return True  # Never alerted before

    # Parse last alert time
    try:
        last_ts = datetime.fromisoformat(last_time.replace("Z", "+00:00"))
        hours_since = (datetime.now(ISRAEL_TZ) - last_ts).total_seconds() / 3600
        if hours_since >= 2:
            return True  # At least 2 hours since last alert
    except:
        return True

    return False


def format_alert_message(reading: dict, language: str = "en") -> str:
    """Format alert message with region info and all available pollutants."""
    level_emoji = {"GOOD": "🟢", "MODERATE": "🟡", "LOW": "🟠", "VERY_LOW": "🔴"}
    level_text_en = {"GOOD": "Good", "MODERATE": "Moderate", "LOW": "Unhealthy", "VERY_LOW": "Dangerous"}
    level_text_he = {"GOOD": "טוב", "MODERATE": "בינוני", "LOW": "לא בריא", "VERY_LOW": "מסוכן"}

    recommendations_en = {
        "GOOD": "✅ Safe for outdoor activities.",
        "MODERATE": "⚠️ Sensitive individuals should limit prolonged outdoor exertion.",
        "LOW": "🚨 Sensitive groups should stay indoors. Others limit outdoor activity.",
        "VERY_LOW": "🚨 DANGER: Everyone should avoid outdoor activity!",
    }

    recommendations_he = {
        "GOOD": "✅ בטוח לפעילות בחוץ.",
        "MODERATE": "⚠️ לבעלי רגישות מומלץ להגביל פעילות מאומצת בחוץ.",
        "LOW": "🚨 בעלי רגישות מומלצים להישאר בתוך מבנים ובתים.",
        "VERY_LOW": "🚨 סכנה: מומלץ להימנע מפעילויות בחוץ!",
    }

    station = reading["station"]
    level = reading["level"]
    emoji = level_emoji.get(level, "⚪")

    # Build pollutants string based on what's available
    pollutants = reading.get("pollutants", {})
    pollutant_meta = reading.get("pollutant_meta", {})

    # Check benzene level and use worst-case color
    benzene_ppb = reading.get("benzene_ppb", 0)
    benzene_level = reading.get("benzene_level")
    benzene_emoji_map = {"GOOD": "🟡", "MODERATE": "🟠", "LOW": "🔴", "VERY_LOW": "🟣"}
    benzene_emoji = benzene_emoji_map.get(benzene_level) if benzene_level else None

    # Use worst case color between AQI and Benzene
    severity_order = {"🟢": 0, "🟡": 1, "🟠": 2, "🔴": 3, "🟣": 4}
    if benzene_emoji and severity_order.get(benzene_emoji, 0) > severity_order.get(emoji, 0):
        emoji = benzene_emoji

    # Determine overall quality level (worst of AQI or Benzene)
    # Map benzene levels to quality terminology (not pollution level terminology)
    benzene_to_quality_he = {"GOOD": "בינוני", "MODERATE": "לא בריא", "LOW": "לא בריא", "VERY_LOW": "מסוכן"}
    aqi_severity = {"GOOD": 0, "MODERATE": 1, "LOW": 2, "VERY_LOW": 3}
    benzene_severity = {"GOOD": 1, "MODERATE": 2, "LOW": 3, "VERY_LOW": 4}

    overall_level = level
    overall_level_he = level_text_he[level]
    recommendation_level = level
    if benzene_level and benzene_severity.get(benzene_level, 0) > aqi_severity.get(level, 0):
        overall_level_he = benzene_to_quality_he[benzene_level]
        overall_level = benzene_level  # Keep for comparison
        # Use benzene-appropriate recommendations when benzene is elevated
        # Map benzene levels to recommendation levels
        benzene_to_recommendation = {"GOOD": "MODERATE", "MODERATE": "LOW", "LOW": "LOW", "VERY_LOW": "VERY_LOW"}
        recommendation_level = benzene_to_recommendation.get(benzene_level, recommendation_level)

    if language == "he":
        pollutant_lines = []

        # Show ALL available pollutants with transformed Hebrew aliases
        for name, value in pollutants.items():
            if value is not None:
                meta = pollutant_meta.get(name, {})
                original_alias = meta.get("alias", name)
                alias = transform_pollutant_alias(name, original_alias)
                units = meta.get("units", "")
                pollutant_lines.append(f"• {alias}: {value:.1f} {units}")

        pollutants_str = "\n".join(pollutant_lines) if pollutant_lines else "אין נתונים זמינים"

        # Show benzene line if elevated
        benzene_line = ""
        benzene_level_names = {"GOOD": "מוגבר", "MODERATE": "גבוה", "LOW": "גבוה מאוד", "VERY_LOW": "מסוכן"}
        if benzene_level:
            benzene_line = f"\n⚗️ *בנזן:* {benzene_level_names.get(benzene_level, benzene_level)}"

        return f"""
{emoji} *התראת איכות אוויר*

📍 *תחנה:* {station.get('display_name', station['name'])}
🗺️ *אזור:* {station.get('regionHe', 'לא ידוע')}
📊 *איכות:* {overall_level_he}
🌬️ *מדד AQI:* {reading['aqi']} ({level_text_he[level]}){benzene_line}
🕐 *זמן:* {reading['timestamp'][:16]}

*מזהמים:*
{pollutants_str}

💡 *המלצה:*
{recommendations_he[recommendation_level]}

🔗 https://air.sviva.gov.il

💬 /help לעזרה
""".strip()

    # English version
    pollutant_lines = []
    if pollutants.get("PM2.5"):
        pollutant_lines.append(f"• PM2.5: {pollutants['PM2.5']:.1f} µg/m³")
    if pollutants.get("PM10"):
        pollutant_lines.append(f"• PM10: {pollutants['PM10']:.1f} µg/m³")
    if pollutants.get("O3"):
        pollutant_lines.append(f"• Ozone (O3): {pollutants['O3']:.1f} ppb")
    if pollutants.get("NO2"):
        pollutant_lines.append(f"• Nitrogen Dioxide (NO2): {pollutants['NO2']:.1f} ppb")
    if pollutants.get("SO2"):
        pollutant_lines.append(f"• Sulfur Dioxide (SO2): {pollutants['SO2']:.1f} ppb")
    if pollutants.get("CO"):
        pollutant_lines.append(f"• Carbon Monoxide (CO): {pollutants['CO']:.1f} ppm")
    benzene_val = pollutants.get("BENZENE") or pollutants.get("Benzene")
    if benzene_val:
        pollutant_lines.append(f"• Benzene: {benzene_val:.1f} µg/m³")

    pollutants_str = "\n".join(pollutant_lines) if pollutant_lines else "No data available"

    return f"""
{emoji} *Air Quality Alert*

📍 *Station:* {station.get('display_name', station['name'])}
🗺️ *Region:* {REGION_NAMES.get(station['region'], station['region'])}
📊 *AQI:* {reading['aqi']} ({level_text_en[level]})
🕐 *Time:* {reading['timestamp'][:16]}

*Pollutants:*
{pollutants_str}

💡 *Recommendation:*
{recommendations_en[level]}

🔗 https://air.sviva.gov.il
""".strip()


def format_improved_message(reading: dict, current_level: str, language: str = "en") -> str:
    """Format 'improved' message when air quality gets better."""
    station = reading["station"]
    aqi = reading["aqi"]
    benzene_ppb = reading.get("benzene_ppb", 0)
    benzene_level = reading.get("benzene_level")

    # Level-specific messaging
    level_info_he = {
        "GOOD": {
            "emoji": "✅",
            "title": "הכל בסדר - איכות האוויר השתפרה",
            "quality": "טוב",
            "message": "💚 איכות האוויר חזרה לרמה תקינה.\nניתן לחזור לפעילות רגילה בחוץ."
        },
        "MODERATE": {
            "emoji": "🟡",
            "title": "איכות האוויר השתפרה",
            "quality": "בינוני",
            "message": "⚠️ איכות האוויר השתפרה אך עדיין בינונית.\nבעלי רגישות מומלצים להמשיך לעקוב."
        },
        "LOW": {
            "emoji": "🟠",
            "title": "איכות האוויר השתפרה מעט",
            "quality": "לא בריא",
            "message": "⚠️ איכות האוויר השתפרה אך עדיין לא בריאה.\nמומלץ להמשיך להגביל פעילות בחוץ."
        },
    }

    level_info_en = {
        "GOOD": {
            "emoji": "✅",
            "title": "All Clear - Air Quality Improved",
            "quality": "Good",
            "message": "💚 Air quality has returned to normal levels.\nSafe to resume outdoor activities."
        },
        "MODERATE": {
            "emoji": "🟡",
            "title": "Air Quality Improved",
            "quality": "Moderate",
            "message": "⚠️ Air quality improved but still moderate.\nSensitive individuals should continue monitoring."
        },
        "LOW": {
            "emoji": "🟠",
            "title": "Air Quality Slightly Improved",
            "quality": "Unhealthy",
            "message": "⚠️ Air quality improved but still unhealthy.\nContinue to limit outdoor activities."
        },
    }

    # Build benzene line if present
    benzene_level_names = {"GOOD": "מוגבר", "MODERATE": "גבוה", "LOW": "גבוה מאוד", "VERY_LOW": "מסוכן"}
    benzene_line_he = ""
    benzene_line_en = ""
    if benzene_ppb and benzene_level:
        benzene_line_he = f"\n⚗️ *בנזן:* {benzene_ppb:.2f} ppb ({benzene_level_names.get(benzene_level, benzene_level)})"
        benzene_line_en = f"\n⚗️ *Benzene:* {benzene_ppb:.2f} ppb"

    if language == "he":
        info = level_info_he.get(current_level, level_info_he["MODERATE"])
        return f"""
{info['emoji']} *{info['title']}*

📍 *תחנה:* {station.get('display_name', station['name'])}
🗺️ *אזור:* {station.get('regionHe', 'לא ידוע')}
📊 *איכות:* {info['quality']}
🌬️ *מדד AQI:* {aqi}{benzene_line_he}
🕐 *זמן:* {reading['timestamp'][:16]}

{info['message']}

🔗 https://air.sviva.gov.il

💬 /help לעזרה
""".strip()

    info = level_info_en.get(current_level, level_info_en["MODERATE"])
    return f"""
{info['emoji']} *{info['title']}*

📍 *Station:* {station.get('display_name', station['name'])}
🗺️ *Region:* {REGION_NAMES.get(station['region'], station['region'])}
📊 *Quality:* {info['quality']}
🌬️ *AQI:* {aqi}{benzene_line_en}
🕐 *Time:* {reading['timestamp'][:16]}

{info['message']}

🔗 https://air.sviva.gov.il
""".strip()


def format_benzene_alert_message(reading: dict, language: str = "en") -> str:
    """Format special alert message for high Benzene levels."""
    benzene_ppb = reading.get("benzene_ppb", 0)
    benzene_level = reading.get("benzene_level", "MODERATE")
    station = reading["station"]

    level_emoji = {"GOOD": "🟡", "MODERATE": "🟠", "LOW": "🔴", "VERY_LOW": "🟣"}
    level_text_he = {"GOOD": "מוגבר", "MODERATE": "גבוה", "LOW": "גבוה מאוד", "VERY_LOW": "מסוכן"}

    # Convert ppb to µg/m³ for display (1 ppb benzene ≈ 3.19 µg/m³)
    benzene_ugm3 = benzene_ppb * 3.19

    recommendations_he = {
        "GOOD": "⚠️ רמת בנזן מוגברת. מומלץ לאוורר את הבית.",
        "MODERATE": "⚠️ רמת בנזן גבוהה. בעלי רגישות מומלצים להימנע משהייה ממושכת בחוץ.",
        "LOW": "🚨 רמת בנזן גבוהה מאוד! מומלץ להישאר בתוך מבנים.",
        "VERY_LOW": "🚨 סכנה: רמת בנזן מסוכנת! הימנעו משהייה בחוץ!",
    }

    emoji = level_emoji.get(benzene_level, "🟠")
    rtl = "\u200f"

    if language == "he":
        return f"""
{emoji} *התראת בנזן*

📍 *תחנה:* {station.get('display_name', station['name'])}
🗺️ *אזור:* {station.get('regionHe', 'לא ידוע')}
⚗️ *בנזן:* {benzene_ppb:.2f} ppb ({benzene_ugm3:.1f} µg/m³)
📊 *רמה:* {level_text_he[benzene_level]}
🕐 *זמן:* {reading['timestamp'][:16]}

💡 *המלצה:*
{recommendations_he[benzene_level]}

ℹ️ הגבול האירופי: 5 µg/m³ (ממוצע שנתי)

🔗 https://air.sviva.gov.il

💬 /help לעזרה
""".strip()

    # English version
    level_text_en = {"GOOD": "Elevated", "MODERATE": "High", "LOW": "Very High", "VERY_LOW": "Hazardous"}
    recommendations_en = {
        "GOOD": "⚠️ Elevated benzene levels. Consider ventilating your home.",
        "MODERATE": "⚠️ High benzene levels. Sensitive individuals should limit prolonged outdoor exposure.",
        "LOW": "🚨 Very high benzene levels! Stay indoors if possible.",
        "VERY_LOW": "🚨 DANGER: Hazardous benzene levels! Avoid outdoor exposure!",
    }

    return f"""
{emoji} *Benzene Alert*

📍 *Station:* {station.get('display_name', station['name'])}
🗺️ *Region:* {REGION_NAMES.get(station['region'], station['region'])}
⚗️ *Benzene:* {benzene_ppb:.2f} ppb ({benzene_ugm3:.1f} µg/m³)
   Level: {level_text_en[benzene_level]}
🕐 *Time:* {reading['timestamp'][:16]}

💡 *Recommendation:*
{recommendations_en[benzene_level]}

ℹ️ EU limit: 5 µg/m³ (annual average)

🔗 https://air.sviva.gov.il
""".strip()


# ============================================================================
# Telegram
# ============================================================================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")


def get_telegram_user(chat_id: str) -> Optional[dict]:
    """Get Telegram user from Redis."""
    r = get_redis()
    if not r:
        return None
    try:
        data = r.get(f"telegram:user:{chat_id}")
        return json.loads(data) if data else None
    except:
        return None


def deactivate_telegram_user(chat_id: str) -> bool:
    """Deactivate a Telegram user (e.g., when they block the bot)."""
    r = get_redis()
    if not r:
        return False
    try:
        user = get_telegram_user(chat_id)
        if user:
            user["active"] = False
            r.set(f"telegram:user:{chat_id}", json.dumps(user))
            print(f"Deactivated user {chat_id} (blocked bot)")
            return True
        return False
    except:
        return False


def get_telegram_subscribers_by_region(region: str) -> list[dict]:
    """Get Telegram subscribers for a region with their preferences."""
    r = get_redis()
    if not r:
        return []

    subscribers = []
    chat_ids = r.smembers("telegram:users")
    for chat_id in chat_ids:
        user = get_telegram_user(chat_id)
        if user and user.get("active") and region in user.get("regions", []):
            subscribers.append({
                "chat_id": chat_id,
                "level": user.get("level", "MODERATE"),
                "hours": user.get("hours", ["morning", "afternoon", "evening", "night"]),
            })
    return subscribers


def get_telegram_subscribers_by_station(station_id: int) -> list[dict]:
    """Get Telegram subscribers for a specific station with their preferences."""
    r = get_redis()
    if not r:
        return []

    subscribers = []
    chat_ids = r.smembers("telegram:users")
    for chat_id in chat_ids:
        user = get_telegram_user(chat_id)
        if user and user.get("active") and station_id in user.get("stations", []):
            subscribers.append({
                "chat_id": chat_id,
                "level": user.get("level", "MODERATE"),
                "hours": user.get("hours", ["morning", "afternoon", "evening", "night"]),
            })
    return subscribers


def get_all_telegram_users_cached() -> list[dict]:
    """Get all Telegram users with pipelining for efficiency."""
    r = get_redis()
    if not r:
        return []

    chat_ids = list(r.smembers("telegram:users"))
    if not chat_ids:
        return []

    # Use pipeline to fetch all users in one round trip
    pipe = r.pipeline()
    for chat_id in chat_ids:
        pipe.get(f"telegram:user:{chat_id}")
    results = pipe.execute()

    users = []
    for data in results:
        if data:
            try:
                user = json.loads(data)
                if user.get("active"):
                    users.append(user)
            except:
                pass
    return users


def get_all_telegram_regions() -> list[str]:
    """Get all regions that have Telegram subscribers."""
    users = get_all_telegram_users_cached()
    regions = set()
    for user in users:
        for region in user.get("regions", []):
            regions.add(region)
    return list(regions)


def get_all_telegram_stations() -> list[int]:
    """Get all station IDs that have Telegram subscribers."""
    users = get_all_telegram_users_cached()
    stations = set()
    for user in users:
        for station_id in user.get("stations", []):
            stations.add(station_id)
    return list(stations)


def deactivate_telegram_user(chat_id: str):
    """Deactivate a Telegram user (e.g., when they block the bot)."""
    r = get_redis()
    if not r:
        return
    data = r.get(f"telegram:user:{chat_id}")
    if data:
        user = json.loads(data)
        user["active"] = False
        r.set(f"telegram:user:{chat_id}", json.dumps(user))
        print(f"Deactivated user {chat_id} (blocked bot)")


def send_telegram_message(chat_id: str, message: str) -> dict:
    """Send a message via Telegram Bot API."""
    if not TELEGRAM_BOT_TOKEN:
        return {"status": "skipped", "reason": "No Telegram token"}
    try:
        response = httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
            },
            timeout=10.0,
        )
        # Handle blocked users (403 Forbidden)
        if response.status_code == 403:
            deactivate_telegram_user(chat_id)
            return {"status": "blocked", "code": 403}
        return {
            "status": "sent" if response.status_code == 200 else "failed",
            "code": response.status_code,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def send_telegram_alerts(message: str, chat_ids: list[str]) -> dict:
    """Send alerts to multiple Telegram recipients."""
    results = []
    for chat_id in chat_ids:
        result = send_telegram_message(chat_id, message)
        result["chat_id"] = chat_id
        # Auto-deactivate users who blocked the bot (403 Forbidden)
        if result.get("code") == 403:
            deactivate_telegram_user(chat_id)
        results.append(result)
    return {"results": results}


def get_telegram_last_alert_time(station_id: int, chat_id: str) -> Optional[str]:
    """Get the last time we sent an alert for this station to this Telegram user."""
    r = get_redis()
    if not r:
        return None
    return r.hget(f"telegram:last_alert:{chat_id}", str(station_id))


def set_telegram_last_alert_time(station_id: int, chat_id: str, timestamp: str):
    """Record when we sent an alert for this station to this Telegram user."""
    r = get_redis()
    if r:
        r.hset(f"telegram:last_alert:{chat_id}", str(station_id), timestamp)
        r.expire(f"telegram:last_alert:{chat_id}", 86400)


def get_telegram_last_alert_info(station_id: int, chat_id: str) -> tuple:
    """Get the last alert info for this station/user. Returns (timestamp, overall_level)."""
    r = get_redis()
    if not r:
        return None, None
    data = r.hget(f"telegram:last_alert:{chat_id}", str(station_id))
    if not data:
        return None, None
    # Format: "timestamp|level" e.g., "2025-12-17T20:30:00+02:00|VERY_LOW"
    if "|" in data:
        parts = data.split("|")
        return parts[0], parts[1] if len(parts) > 1 else None
    return data, None  # Old format without level


def set_telegram_last_alert_info(station_id: int, chat_id: str, timestamp: str, overall_level: str):
    """Record when we sent an alert and what level it was."""
    r = get_redis()
    if r:
        r.hset(f"telegram:last_alert:{chat_id}", str(station_id), f"{timestamp}|{overall_level}")
        r.expire(f"telegram:last_alert:{chat_id}", 86400)


def get_telegram_all_clear_sent(station_id: int, chat_id: str) -> bool:
    """Check if we already sent an 'all clear' for this station after the last alert."""
    r = get_redis()
    if not r:
        return False
    return r.hget(f"telegram:all_clear:{chat_id}", str(station_id)) is not None


def set_telegram_all_clear_sent(station_id: int, chat_id: str, timestamp: str):
    """Record that we sent an 'all clear' notification."""
    r = get_redis()
    if r:
        r.hset(f"telegram:all_clear:{chat_id}", str(station_id), timestamp)
        r.expire(f"telegram:all_clear:{chat_id}", 86400)


def clear_telegram_all_clear(station_id: int, chat_id: str):
    """Clear the 'all clear' flag when a new alert is sent."""
    r = get_redis()
    if r:
        r.hdel(f"telegram:all_clear:{chat_id}", str(station_id))


def should_send_telegram_alert(station_id: int, chat_id: str, current_overall_level: str) -> bool:
    """
    Check if we should send a Telegram alert.
    - Always send if no previous alert
    - Send if 2 hours have passed (cooldown expired)
    - Send if overall level got WORSE (even within cooldown)
    """
    last_time, last_level = get_telegram_last_alert_info(station_id, chat_id)
    if not last_time:
        return True

    # Level severity: higher = worse
    # These are the English level names we store in Redis
    level_severity = {
        "GOOD": 0,
        "MODERATE": 1,
        "LOW": 2,
        "VERY_LOW": 3,
    }
    current_severity = level_severity.get(current_overall_level, 0)
    last_severity = level_severity.get(last_level, 0)

    # Alert if level got worse
    if current_severity > last_severity:
        return True

    # Otherwise, check 2-hour cooldown
    try:
        last_ts = datetime.fromisoformat(last_time.replace("Z", "+00:00"))
        hours_since = (datetime.now(ISRAEL_TZ) - last_ts).total_seconds() / 3600
        return hours_since >= 2
    except:
        return True


# ============================================================================
# Twilio WhatsApp
# ============================================================================

def send_twilio_whatsapp(message: str, recipients: list[str]) -> dict:
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN")
    from_number = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
    
    if not account_sid or not auth_token:
        return {"error": "Twilio credentials not configured"}
    
    results = []
    
    for recipient in recipients:
        try:
            to_number = f"whatsapp:{recipient}" if not recipient.startswith("whatsapp:") else recipient
            
            response = httpx.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
                auth=(account_sid, auth_token),
                data={"From": from_number, "To": to_number, "Body": message},
                timeout=30.0,
            )
            
            results.append({
                "recipient": recipient,
                "status": "sent" if response.status_code in [200, 201] else "failed",
            })
        except Exception as e:
            results.append({"recipient": recipient, "status": "error", "error": str(e)})
    
    return {"results": results}


# ============================================================================
# Main Entry Point
# ============================================================================

def main(args: dict) -> dict:
    """DigitalOcean Functions entry point."""

    # Admin actions (invoke with doctl serverless functions invoke)
    admin_action = args.get("admin_action")
    if admin_action == "get_env":
        return {"statusCode": 200, "body": {
            "REDIS_URL": os.environ.get("REDIS_URL", ""),
            "TELEGRAM_BOT_TOKEN": os.environ.get("TELEGRAM_BOT_TOKEN", ""),
            "TWILIO_ACCOUNT_SID": os.environ.get("TWILIO_ACCOUNT_SID", ""),
            "TWILIO_AUTH_TOKEN": os.environ.get("TWILIO_AUTH_TOKEN", ""),
        }}
    if admin_action == "deactivate_user":
        chat_id = args.get("chat_id")
        if chat_id:
            success = deactivate_telegram_user(str(chat_id))
            return {"statusCode": 200, "body": {"action": "deactivate_user", "chat_id": chat_id, "success": success}}
        return {"statusCode": 400, "body": {"error": "chat_id required"}}
    if admin_action == "debug_api":
        import re
        results = {}
        # Test fetching the site
        try:
            site_resp = httpx.get(AIR_SITE_URL, timeout=10.0)
            results["site_status"] = site_resp.status_code
            results["site_length"] = len(site_resp.text)
            match = re.search(r'"Authorization":\s*[\'"]ApiToken ([a-f0-9-]+)[\'"]', site_resp.text)
            results["token_found"] = match.group(1) if match else None
            # Also try old regex
            match2 = re.search(r"ApiToken ([a-f0-9-]+)", site_resp.text)
            results["token_old_regex"] = match2.group(1) if match2 else None
        except Exception as e:
            results["site_error"] = str(e)
        # Test API with token
        if results.get("token_found"):
            try:
                api_resp = httpx.get(
                    f"{AIR_API_URL}/stations/3/data/latest",
                    headers={"Authorization": f"ApiToken {results['token_found']}"},
                    timeout=10.0
                )
                results["api_status"] = api_resp.status_code
                results["api_body"] = api_resp.text[:200] if api_resp.status_code != 200 else "OK"
            except Exception as e:
                results["api_error"] = str(e)
        return {"statusCode": 200, "body": results}

    # Config
    language = args.get("language") or os.environ.get("LANGUAGE", "he")
    current_time_window = get_current_time_window()

    # Batch processing: split stations across multiple cron invocations
    # Auto-detect batch from current minute if not provided (DO triggers don't pass body)
    if "batch" in args and "total_batches" in args:
        batch = int(args.get("batch", 0))
        total_batches = int(args.get("total_batches", 1))
    else:
        # Determine batch from current minute:
        # batch-0 runs at :00, :10, :20, :30, :40, :50 (minute % 10 < 2)
        # batch-1 runs at :02, :12, :22, :32, :42, :52 (minute % 10 >= 2)
        current_minute = datetime.now(ISRAEL_TZ).minute % 10
        total_batches = 2
        batch = 0 if current_minute < 2 else 1

    # Get all WhatsApp subscribers from Redis (grouped by region AND station)
    subscribers_by_region = get_all_subscribers()
    subscribers_by_station = get_station_subscribers()

    active_regions = list(subscribers_by_region.keys())
    active_station_ids = list(subscribers_by_station.keys())

    # Get all Telegram subscribers
    telegram_regions = get_all_telegram_regions()
    telegram_stations = get_all_telegram_stations()

    # Combine active regions and stations from both platforms
    all_active_regions = list(set(active_regions + telegram_regions))
    all_active_station_ids = list(set(active_station_ids + telegram_stations))

    if not all_active_regions and not all_active_station_ids:
        return {
            "statusCode": 200,
            "body": {
                "timestamp": datetime.now(ISRAEL_TZ).isoformat(),
                "current_time_window": current_time_window,
                "batch": batch,
                "total_batches": total_batches,
                "message": "No subscribers registered",
                "stations_checked": 0,
                "alerts_sent": [],
            },
        }

    # Fetch all stations from API (cached)
    all_stations = get_all_stations()

    # Build list of ALL stations to check (directly subscribed + regional)
    stations_to_check_all = []
    seen_station_ids = set()

    # First: Add specific stations that users directly subscribed to
    for s in all_stations:
        if s["id"] in all_active_station_ids:
            stations_to_check_all.append(s)
            seen_station_ids.add(s["id"])

    # Second: Add all stations from active regions
    for s in all_stations:
        if s["region"] in all_active_regions and s["id"] not in seen_station_ids:
            stations_to_check_all.append(s)
            seen_station_ids.add(s["id"])

    # Sort by station ID for consistent batch assignment across runs
    stations_to_check_all.sort(key=lambda s: s["id"])

    # Filter to only this batch's stations
    stations_to_check = [
        s for i, s in enumerate(stations_to_check_all)
        if i % total_batches == batch
    ]

    # Fetch readings
    readings = fetch_readings(stations_to_check)

    # Send alerts to subscribers based on their individual thresholds, hours, and anti-spam
    alerts_sent = []
    total_whatsapp_notifications = 0
    total_telegram_notifications = 0
    skipped_due_to_hours = 0
    skipped_due_to_recent_alert = 0

    for reading in readings:
        station_id = reading["station"]["id"]
        region = reading["station"]["region"]
        aqi = reading["aqi"]
        level = reading["level"]
        timestamp = reading["timestamp"]
        benzene_ppb = reading.get("benzene_ppb", 0)
        benzene_level = reading.get("benzene_level")

        # Calculate overall quality level (worst of AQI or Benzene)
        # Severity mapping for comparison
        aqi_severity = {"GOOD": 0, "MODERATE": 1, "LOW": 2, "VERY_LOW": 3}
        benzene_severity = {"GOOD": 1, "MODERATE": 2, "LOW": 3, "VERY_LOW": 4}

        overall_level = level  # Start with AQI level
        if benzene_level and benzene_severity.get(benzene_level, 0) > aqi_severity.get(level, 0):
            overall_level = benzene_level

        whatsapp_recipients = []
        telegram_recipients = []

        # ===== WhatsApp Subscribers =====
        region_subscribers = get_subscribers_with_preferences(region)
        for s in region_subscribers:
            # Alert if AQI OR Benzene triggers for user's threshold
            should_alert_aqi = should_alert(aqi, s["level"])
            should_alert_benz = benzene_level and should_alert_benzene(benzene_ppb, s["level"])
            if should_alert_aqi or should_alert_benz:
                if not is_within_user_hours(s["hours"]):
                    skipped_due_to_hours += 1
                elif not should_send_alert(station_id, s["phone"], overall_level):
                    skipped_due_to_recent_alert += 1
                else:
                    whatsapp_recipients.append(s["phone"])

        station_subscribers = get_station_subscribers_with_preferences(station_id)
        for s in station_subscribers:
            if s["phone"] not in whatsapp_recipients:
                should_alert_aqi = should_alert(aqi, s["level"])
                should_alert_benz = benzene_level and should_alert_benzene(benzene_ppb, s["level"])
                if should_alert_aqi or should_alert_benz:
                    if not is_within_user_hours(s["hours"]):
                        skipped_due_to_hours += 1
                    elif not should_send_alert(station_id, s["phone"], overall_level):
                        skipped_due_to_recent_alert += 1
                    else:
                        whatsapp_recipients.append(s["phone"])

        # ===== Telegram Subscribers =====
        telegram_region_subs = get_telegram_subscribers_by_region(region)
        for s in telegram_region_subs:
            should_alert_aqi = should_alert(aqi, s["level"])
            should_alert_benz = benzene_level and should_alert_benzene(benzene_ppb, s["level"])
            if should_alert_aqi or should_alert_benz:
                if not is_within_user_hours(s["hours"]):
                    skipped_due_to_hours += 1
                elif not should_send_telegram_alert(station_id, s["chat_id"], overall_level):
                    skipped_due_to_recent_alert += 1
                else:
                    telegram_recipients.append(s["chat_id"])

        telegram_station_subs = get_telegram_subscribers_by_station(station_id)
        for s in telegram_station_subs:
            if s["chat_id"] not in telegram_recipients:
                should_alert_aqi = should_alert(aqi, s["level"])
                should_alert_benz = benzene_level and should_alert_benzene(benzene_ppb, s["level"])
                if should_alert_aqi or should_alert_benz:
                    if not is_within_user_hours(s["hours"]):
                        skipped_due_to_hours += 1
                    elif not should_send_telegram_alert(station_id, s["chat_id"], overall_level):
                        skipped_due_to_recent_alert += 1
                    else:
                        telegram_recipients.append(s["chat_id"])

        # Send alerts
        message = format_alert_message(reading, language)
        whatsapp_result = None
        telegram_result = None

        whatsapp_enabled = os.environ.get("WHATSAPP_ENABLED", "false").lower() == "true"
        if whatsapp_recipients and whatsapp_enabled:
            whatsapp_result = send_twilio_whatsapp(message, whatsapp_recipients)
            total_whatsapp_notifications += len(whatsapp_recipients)
            for phone in whatsapp_recipients:
                set_last_alert_time(station_id, phone, timestamp)

        if telegram_recipients:
            telegram_result = send_telegram_alerts(message, telegram_recipients)
            total_telegram_notifications += len(telegram_recipients)
            # Record alert with overall level for "worse level" detection
            # Also clear "all clear" flag so we can send it when quality improves
            for chat_id in telegram_recipients:
                set_telegram_last_alert_info(station_id, chat_id, timestamp, overall_level)
                clear_telegram_all_clear(station_id, chat_id)

        if whatsapp_recipients or telegram_recipients:
            alerts_sent.append({
                "station": reading["station"].get("nameEn", reading["station"]["name"]),
                "region": region,
                "aqi": aqi,
                "level": level,
                "overall_level": overall_level,
                "benzene_ppb": benzene_ppb,
                "benzene_level": benzene_level,
                "pollutants": reading.get("pollutants", {}),
                "whatsapp_recipients": len(whatsapp_recipients),
                "telegram_recipients": len(telegram_recipients),
                "whatsapp_result": whatsapp_result,
                "telegram_result": telegram_result,
            })

        # ===== "Improved" Notifications =====
        # Send if quality improved from previous alert level
        # Level severity for comparison
        level_severity_map = {"GOOD": 0, "MODERATE": 1, "LOW": 2, "VERY_LOW": 3}
        current_severity = level_severity_map.get(overall_level, 0)

        improved_recipients = []
        all_subs = telegram_region_subs + telegram_station_subs
        seen_chat_ids = set()

        for s in all_subs:
            chat_id = s["chat_id"]
            if chat_id in seen_chat_ids:
                continue
            seen_chat_ids.add(chat_id)

            # Check if user was previously alerted for this station
            last_time, last_level = get_telegram_last_alert_info(station_id, chat_id)
            if not last_time or not last_level:
                continue

            last_severity = level_severity_map.get(last_level, 0)

            # Only notify if quality IMPROVED (current is better than last)
            if current_severity >= last_severity:
                continue

            # Check if we already sent "improved" notification after this alert
            if get_telegram_all_clear_sent(station_id, chat_id):
                continue

            # Check user's hour preferences
            if not is_within_user_hours(s["hours"]):
                continue

            improved_recipients.append(chat_id)

        if improved_recipients:
            improved_message = format_improved_message(reading, overall_level, language)
            improved_result = send_telegram_alerts(improved_message, improved_recipients)
            total_telegram_notifications += len(improved_recipients)

            # Update last alert info to track further improvements
            for chat_id in improved_recipients:
                # Only set all_clear if we reached GOOD (true "all clear")
                # For intermediate improvements (VERY_LOW→LOW, LOW→MODERATE),
                # don't set the flag so we can notify on further improvements
                if overall_level == "GOOD":
                    set_telegram_all_clear_sent(station_id, chat_id, timestamp)
                # Update the LEVEL to track further improvements, but keep the
                # ORIGINAL timestamp so the cooldown timer isn't reset.
                # This prevents repeating alerts after improvement notifications.
                original_time, _ = get_telegram_last_alert_info(station_id, chat_id)
                if original_time:
                    set_telegram_last_alert_info(station_id, chat_id, original_time, overall_level)
                else:
                    set_telegram_last_alert_info(station_id, chat_id, timestamp, overall_level)

            alerts_sent.append({
                "station": reading["station"].get("nameEn", reading["station"]["name"]),
                "region": region,
                "type": "improved",
                "from_level": last_level,
                "to_level": overall_level,
                "aqi": aqi,
                "telegram_recipients": len(improved_recipients),
                "telegram_result": improved_result,
            })

    total_region_subs = sum(len(s) for s in subscribers_by_region.values())
    total_station_subs = sum(len(s) for s in subscribers_by_station.values())
    total_telegram_subs = len(get_redis().smembers("telegram:users")) if get_redis() else 0

    return {
        "statusCode": 200,
        "body": {
            "timestamp": datetime.now(ISRAEL_TZ).isoformat(),
            "current_time_window": current_time_window,
            "batch": batch,
            "total_batches": total_batches,
            "stations_in_batch": len(stations_to_check),
            "total_stations_to_check": len(stations_to_check_all),
            "active_regions": all_active_regions,
            "active_stations": all_active_station_ids,
            "whatsapp_subscribers": total_region_subs + total_station_subs,
            "telegram_subscribers": total_telegram_subs,
            "stations_checked": len(readings),
            "stations_available": len(all_stations),
            "alerts_triggered": len(alerts_sent),
            "whatsapp_notifications": total_whatsapp_notifications,
            "telegram_notifications": total_telegram_notifications,
            "skipped_due_to_hours": skipped_due_to_hours,
            "skipped_due_to_recent_alert": skipped_due_to_recent_alert,
            "alerts_sent": alerts_sent,
        },
    }


if __name__ == "__main__":
    result = main({})
    print(json.dumps(result, indent=2))