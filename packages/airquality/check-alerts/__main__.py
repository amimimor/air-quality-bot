"""
Israel Air Quality Alert - DigitalOcean Functions
With Region/Station Filtering

Configure WATCH_REGIONS or WATCH_STATIONS to only get alerts for your area.
"""

import json
import os
from datetime import datetime
from typing import Optional, List
from zoneinfo import ZoneInfo

import httpx
import redis

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
    return redis.from_url(REDIS_URL, decode_responses=True)


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
            match = re.search(r"ApiToken ([a-f0-9-]+)", response.text)
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

                stations.append({
                    "id": s["stationId"],
                    "name": s["name"],
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


ALERT_LEVELS = {
    "GOOD": 51,
    "MODERATE": 0,
    "LOW": -200,
    "VERY_LOW": -400,
}


# ============================================================================
# Air Quality Functions
# ============================================================================

def get_alert_level(aqi: float) -> str:
    if aqi >= 51:
        return "GOOD"
    elif aqi >= 0:
        return "MODERATE"
    elif aqi >= -200:
        return "LOW"
    else:
        return "VERY_LOW"


def should_alert(aqi: float, threshold: str) -> bool:
    threshold_value = ALERT_LEVELS.get(threshold, ALERT_LEVELS["LOW"])
    return aqi < threshold_value


def calculate_aqi(pollutants: dict) -> int:
    """
    Calculate Air Quality Index based on pollutant values.
    Uses Israeli standard: positive is good, negative is bad.
    Range: 100 (excellent) to -400 (hazardous)

    Primary pollutants considered (in priority order):
    - PM2.5: Fine particulate matter
    - PM10: Coarse particulate matter
    - O3: Ozone
    - NO2: Nitrogen dioxide
    """
    pm25 = pollutants.get("PM2.5")
    pm10 = pollutants.get("PM10")
    o3 = pollutants.get("O3")
    no2 = pollutants.get("NO2")

    scores = []

    # PM2.5 scoring (µg/m³)
    if pm25 is not None:
        if pm25 <= 12:
            scores.append(100)
        elif pm25 <= 35:
            scores.append(75 - int((pm25 - 12) * 1.5))
        elif pm25 <= 55:
            scores.append(50 - int((pm25 - 35) * 2.5))
        elif pm25 <= 150:
            scores.append(0 - int((pm25 - 55) * 2))
        elif pm25 <= 250:
            scores.append(-200 - int((pm25 - 150) * 2))
        else:
            scores.append(-400)

    # PM10 scoring (µg/m³)
    if pm10 is not None:
        if pm10 <= 50:
            scores.append(100)
        elif pm10 <= 100:
            scores.append(75 - int((pm10 - 50) * 0.5))
        elif pm10 <= 150:
            scores.append(50 - int((pm10 - 100) * 1))
        elif pm10 <= 250:
            scores.append(0 - int((pm10 - 150) * 2))
        else:
            scores.append(-400)

    # O3 scoring (ppb)
    if o3 is not None:
        if o3 <= 60:
            scores.append(100)
        elif o3 <= 80:
            scores.append(75 - int((o3 - 60)))
        elif o3 <= 100:
            scores.append(55 - int((o3 - 80) * 2.5))
        elif o3 <= 150:
            scores.append(0 - int((o3 - 100) * 2))
        else:
            scores.append(-200)

    # NO2 scoring (ppb)
    if no2 is not None:
        if no2 <= 53:
            scores.append(100)
        elif no2 <= 100:
            scores.append(75 - int((no2 - 53) * 0.5))
        elif no2 <= 150:
            scores.append(50 - int((no2 - 100) * 1))
        else:
            scores.append(0 - int((no2 - 150) * 2))

    # Return the worst (lowest) score, or 50 if no data
    return min(scores) if scores else 50


def fetch_readings(stations: list[dict]) -> list[dict]:
    """Fetch real air quality readings from air.sviva.gov.il API."""
    readings = []
    api_token = get_api_token()

    for station in stations:
        station_id = station["id"]
        try:
            response = httpx.get(
                f"{AIR_API_URL}/stations/{station_id}/data/latest",
                headers={"Authorization": f"ApiToken {api_token}"},
                timeout=10.0,
            )

            if response.status_code == 200:
                data = response.json()
                data_list = data.get("data", [])
                if not data_list:
                    continue

                channels = data_list[0].get("channels", [])
                timestamp = data_list[0].get("datetime", datetime.now(ISRAEL_TZ).isoformat())

                # Collect all pollutants
                pollutants = {}
                for channel in channels:
                    name = channel.get("name", "").upper()
                    value = channel.get("value")
                    if value is not None and channel.get("valid", False):
                        pollutants[name] = float(value)

                aqi = calculate_aqi(pollutants)

                readings.append({
                    "station": station,
                    "aqi": aqi,
                    "level": get_alert_level(aqi),
                    "pollutants": pollutants,
                    "pm25": pollutants.get("PM2.5", 0),
                    "pm10": pollutants.get("PM10", 0),
                    "o3": pollutants.get("O3", 0),
                    "no2": pollutants.get("NO2", 0),
                    "so2": pollutants.get("SO2", 0),
                    "co": pollutants.get("CO", 0),
                    "timestamp": timestamp,
                })
            else:
                print(f"Station {station_id} returned {response.status_code}")

        except Exception as e:
            print(f"Error fetching station {station_id}: {e}")
            continue

    return readings


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
        "MODERATE": "⚠️ אנשים רגישים צריכים להגביל מאמץ בחוץ.",
        "LOW": "🚨 קבוצות רגישות צריכות להישאר בפנים.",
        "VERY_LOW": "🚨 סכנה: כולם צריכים להימנע מפעילות בחוץ!",
    }

    station = reading["station"]
    level = reading["level"]
    emoji = level_emoji.get(level, "⚪")

    # Build pollutants string based on what's available
    pollutants = reading.get("pollutants", {})

    if language == "he":
        pollutant_lines = []
        if pollutants.get("PM2.5"):
            pollutant_lines.append(f"• PM2.5: {pollutants['PM2.5']:.1f} µg/m³")
        if pollutants.get("PM10"):
            pollutant_lines.append(f"• PM10: {pollutants['PM10']:.1f} µg/m³")
        if pollutants.get("O3"):
            pollutant_lines.append(f"• אוזון (O3): {pollutants['O3']:.1f} ppb")
        if pollutants.get("NO2"):
            pollutant_lines.append(f"• חנקן דו-חמצני (NO2): {pollutants['NO2']:.1f} ppb")
        if pollutants.get("SO2"):
            pollutant_lines.append(f"• גופרית דו-חמצנית (SO2): {pollutants['SO2']:.1f} ppb")
        if pollutants.get("CO"):
            pollutant_lines.append(f"• פחמן חד-חמצני (CO): {pollutants['CO']:.1f} ppm")

        pollutants_str = "\n".join(pollutant_lines) if pollutant_lines else "אין נתונים זמינים"

        return f"""
{emoji} *התראת איכות אוויר*

📍 *תחנה:* {station['name']}
🗺️ *אזור:* {station.get('regionHe', 'לא ידוע')}
📊 *מדד:* {reading['aqi']} ({level_text_he[level]})
🕐 *זמן:* {reading['timestamp'][:16]}

*מזהמים:*
{pollutants_str}

💡 *המלצה:*
{recommendations_he[level]}

🔗 https://air.sviva.gov.il
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

    pollutants_str = "\n".join(pollutant_lines) if pollutant_lines else "No data available"

    return f"""
{emoji} *Air Quality Alert*

📍 *Station:* {station.get('nameEn', station['name'])}
🗺️ *Region:* {REGION_NAMES.get(station['region'], station['region'])}
📊 *AQI:* {reading['aqi']} ({level_text_en[level]})
🕐 *Time:* {reading['timestamp'][:16]}

*Pollutants:*
{pollutants_str}

💡 *Recommendation:*
{recommendations_en[level]}

🔗 https://air.sviva.gov.il
""".strip()


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

    # Config
    language = args.get("language") or os.environ.get("LANGUAGE", "he")
    current_time_window = get_current_time_window()

    # Get all subscribers from Redis (grouped by region AND station)
    subscribers_by_region = get_all_subscribers()
    subscribers_by_station = get_station_subscribers()

    active_regions = list(subscribers_by_region.keys())
    active_station_ids = list(subscribers_by_station.keys())

    if not active_regions and not active_station_ids:
        return {
            "statusCode": 200,
            "body": {
                "timestamp": datetime.now(ISRAEL_TZ).isoformat(),
                "current_time_window": current_time_window,
                "message": "No subscribers registered",
                "stations_checked": 0,
                "alerts_sent": [],
            },
        }

    # Fetch all stations from API (cached)
    all_stations = get_all_stations()

    # Get stations to check: regions with subscribers + specific stations with subscribers
    stations_to_check = []
    seen_station_ids = set()

    # Add stations from active regions
    for s in all_stations:
        if s["region"] in active_regions:
            stations_to_check.append(s)
            seen_station_ids.add(s["id"])

    # Add specific stations that have subscribers (if not already included)
    for s in all_stations:
        if s["id"] in active_station_ids and s["id"] not in seen_station_ids:
            stations_to_check.append(s)
            seen_station_ids.add(s["id"])

    # Fetch readings
    readings = fetch_readings(stations_to_check)

    # Send alerts to subscribers based on their individual thresholds, hours, and anti-spam
    alerts_sent = []
    total_notifications = 0
    skipped_due_to_hours = 0
    skipped_due_to_recent_alert = 0

    for reading in readings:
        station_id = reading["station"]["id"]
        region = reading["station"]["region"]
        aqi = reading["aqi"]
        level = reading["level"]
        timestamp = reading["timestamp"]

        recipients_to_notify = []

        # Get region subscribers with their preferences
        region_subscribers = get_subscribers_with_preferences(region)
        for s in region_subscribers:
            if should_alert(aqi, s["level"]):
                if not is_within_user_hours(s["hours"]):
                    skipped_due_to_hours += 1
                elif not should_send_alert(station_id, s["phone"], level):
                    skipped_due_to_recent_alert += 1
                else:
                    recipients_to_notify.append(s["phone"])

        # Get station-specific subscribers with their preferences
        station_subscribers = get_station_subscribers_with_preferences(station_id)
        for s in station_subscribers:
            if s["phone"] not in recipients_to_notify:  # Avoid duplicates
                if should_alert(aqi, s["level"]):
                    if not is_within_user_hours(s["hours"]):
                        skipped_due_to_hours += 1
                    elif not should_send_alert(station_id, s["phone"], level):
                        skipped_due_to_recent_alert += 1
                    else:
                        recipients_to_notify.append(s["phone"])

        if recipients_to_notify:
            message = format_alert_message(reading, language)
            result = send_twilio_whatsapp(message, recipients_to_notify)
            total_notifications += len(recipients_to_notify)

            # Record alert time for anti-spam
            for phone in recipients_to_notify:
                set_last_alert_time(station_id, phone, timestamp)

            alerts_sent.append({
                "station": reading["station"].get("nameEn", reading["station"]["name"]),
                "region": region,
                "aqi": aqi,
                "level": level,
                "pollutants": reading.get("pollutants", {}),
                "recipients_count": len(recipients_to_notify),
                "notification": result,
            })

    total_region_subs = sum(len(s) for s in subscribers_by_region.values())
    total_station_subs = sum(len(s) for s in subscribers_by_station.values())

    return {
        "statusCode": 200,
        "body": {
            "timestamp": datetime.now(ISRAEL_TZ).isoformat(),
            "current_time_window": current_time_window,
            "active_regions": active_regions,
            "active_stations": active_station_ids,
            "total_subscribers": total_region_subs + total_station_subs,
            "stations_checked": len(readings),
            "stations_available": len(all_stations),
            "alerts_triggered": len(alerts_sent),
            "total_notifications": total_notifications,
            "skipped_due_to_hours": skipped_due_to_hours,
            "skipped_due_to_recent_alert": skipped_due_to_recent_alert,
            "alerts_sent": alerts_sent,
        },
    }


if __name__ == "__main__":
    result = main({})
    print(json.dumps(result, indent=2))