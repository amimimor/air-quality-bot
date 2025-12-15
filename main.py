"""
Israel Air Quality Alert - DigitalOcean Functions
With Region/Station Filtering

Configure WATCH_REGIONS or WATCH_STATIONS to only get alerts for your area.
"""

import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import redis
from dotenv import load_dotenv

load_dotenv()

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


def get_user_data(phone: str) -> dict | None:
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
# All Available Stations
# ============================================================================

ALL_STATIONS = [
    # Tel Aviv Area
    {"id": 1, "name": "אחד העם", "nameEn": "Ahad Ha'am", "region": "tel_aviv", "regionHe": "תל אביב"},
    {"id": 13, "name": "תל אביב מרכז", "nameEn": "Tel Aviv Center", "region": "tel_aviv", "regionHe": "תל אביב"},
    
    # Center
    {"id": 2, "name": "רמת גן", "nameEn": "Ramat Gan", "region": "center", "regionHe": "מרכז"},
    {"id": 10, "name": "רחובות", "nameEn": "Rehovot", "region": "center", "regionHe": "מרכז"},
    {"id": 11, "name": "מודיעין", "nameEn": "Modi'in", "region": "center", "regionHe": "מרכז"},
    {"id": 12, "name": "פתח תקווה", "nameEn": "Petah Tikva", "region": "center", "regionHe": "מרכז"},
    {"id": 14, "name": "ראשון לציון", "nameEn": "Rishon LeZion", "region": "center", "regionHe": "מרכז"},
    
    # Jerusalem
    {"id": 3, "name": "ירושלים", "nameEn": "Jerusalem", "region": "jerusalem", "regionHe": "ירושלים"},
    
    # Haifa Area
    {"id": 4, "name": "חיפה - נווה שאנן", "nameEn": "Haifa - Neve Sha'anan", "region": "haifa", "regionHe": "חיפה"},
    {"id": 15, "name": "חיפה - קריית חיים", "nameEn": "Haifa - Kiryat Haim", "region": "haifa", "regionHe": "חיפה"},
    {"id": 16, "name": "קריות", "nameEn": "Krayot", "region": "haifa", "regionHe": "חיפה"},
    
    # South
    {"id": 5, "name": "באר שבע", "nameEn": "Beer Sheva", "region": "south", "regionHe": "דרום"},
    {"id": 6, "name": "אילת", "nameEn": "Eilat", "region": "south", "regionHe": "דרום"},
    {"id": 7, "name": "אשדוד", "nameEn": "Ashdod", "region": "south", "regionHe": "דרום"},
    {"id": 8, "name": "אשקלון", "nameEn": "Ashkelon", "region": "south", "regionHe": "דרום"},
    
    # Sharon (Coastal Plain)
    {"id": 9, "name": "נתניה", "nameEn": "Netanya", "region": "sharon", "regionHe": "שרון"},
    {"id": 17, "name": "הרצליה", "nameEn": "Herzliya", "region": "sharon", "regionHe": "שרון"},
    {"id": 18, "name": "רעננה", "nameEn": "Ra'anana", "region": "sharon", "regionHe": "שרון"},
    
    # North
    {"id": 19, "name": "נצרת", "nameEn": "Nazareth", "region": "north", "regionHe": "צפון"},
    {"id": 20, "name": "עפולה", "nameEn": "Afula", "region": "north", "regionHe": "צפון"},
    {"id": 21, "name": "טבריה", "nameEn": "Tiberias", "region": "north", "regionHe": "צפון"},
]

# Region name mappings for easy configuration
REGION_NAMES = {
    "tel_aviv": "Tel Aviv",
    "center": "Center",
    "jerusalem": "Jerusalem", 
    "haifa": "Haifa",
    "south": "South",
    "sharon": "Sharon",
    "north": "North",
}


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


def fetch_readings(stations: list[dict]) -> list[dict]:
    """Fetch air quality readings for specified stations."""
    import random
    
    readings = []
    for station in stations:
        # Simulate AQI - replace with actual API call
        aqi = random.choice([75, 65, 45, 30, -50, -150, -250])
        
        readings.append({
            "station": station,
            "aqi": aqi,
            "level": get_alert_level(aqi),
            "pm25": round(random.uniform(5, 35), 1),
            "pm10": round(random.uniform(10, 50), 1),
            "timestamp": datetime.now().isoformat(),
        })
    
    return readings


def format_alert_message(reading: dict, language: str = "en") -> str:
    """Format alert message with region info."""
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
    
    if language == "he":
        return f"""
{emoji} *התראת איכות אוויר*

📍 *תחנה:* {station['name']}
🗺️ *אזור:* {station['regionHe']}
📊 *מדד:* {reading['aqi']} ({level_text_he[level]})
🕐 *זמן:* {reading['timestamp'][:16]}

*מזהמים:*
• PM2.5: {reading['pm25']} µg/m³
• PM10: {reading['pm10']} µg/m³

💡 *המלצה:*
{recommendations_he[level]}

🔗 https://air.sviva.gov.il
""".strip()
    
    return f"""
{emoji} *Air Quality Alert*

📍 *Station:* {station['nameEn']}
🗺️ *Region:* {REGION_NAMES.get(station['region'], station['region'])}
📊 *AQI:* {reading['aqi']} ({level_text_en[level]})
🕐 *Time:* {reading['timestamp'][:16]}

*Pollutants:*
• PM2.5: {reading['pm25']} µg/m³
• PM10: {reading['pm10']} µg/m³

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

    # Get all subscribers from Redis (grouped by region)
    subscribers_by_region = get_all_subscribers()
    active_regions = list(subscribers_by_region.keys())

    if not active_regions:
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

    # Get stations for regions with subscribers
    all_stations = [s for s in ALL_STATIONS if s["region"] in active_regions]

    # Fetch readings
    readings = fetch_readings(all_stations)

    # Send alerts to subscribers based on their individual thresholds and hours
    alerts_sent = []
    total_notifications = 0
    skipped_due_to_hours = 0

    for reading in readings:
        region = reading["station"]["region"]
        aqi = reading["aqi"]

        # Get subscribers with their preferences (level and hours)
        subscribers = get_subscribers_with_preferences(region)

        # Filter subscribers who should be alerted based on threshold AND hours
        recipients_to_notify = []
        for s in subscribers:
            if should_alert(aqi, s["level"]):
                if is_within_user_hours(s["hours"]):
                    recipients_to_notify.append(s["phone"])
                else:
                    skipped_due_to_hours += 1

        if recipients_to_notify:
            message = format_alert_message(reading, language)
            result = send_twilio_whatsapp(message, recipients_to_notify)
            total_notifications += len(recipients_to_notify)
            alerts_sent.append({
                "station": reading["station"]["nameEn"],
                "region": region,
                "aqi": aqi,
                "level": reading["level"],
                "recipients_count": len(recipients_to_notify),
                "notification": result,
            })

    return {
        "statusCode": 200,
        "body": {
            "timestamp": datetime.now(ISRAEL_TZ).isoformat(),
            "current_time_window": current_time_window,
            "active_regions": active_regions,
            "total_subscribers": sum(len(s) for s in subscribers_by_region.values()),
            "stations_checked": len(readings),
            "alerts_triggered": len(alerts_sent),
            "total_notifications": total_notifications,
            "skipped_due_to_hours": skipped_due_to_hours,
            "alerts_sent": alerts_sent,
        },
    }


if __name__ == "__main__":
    result = main({})
    print(json.dumps(result, indent=2))