"""
Twilio WhatsApp Webhook Handler - Hebrew Questionnaire
Handles user registration and region/city preferences with drill-down.
"""

import os
import json
import redis
import httpx
import re
from typing import Optional, List
from urllib.parse import parse_qs

# ============================================================================
# Redis Connection
# ============================================================================

REDIS_URL = os.environ.get("REDIS_URL")

def get_redis():
    """Get Redis connection."""
    return redis.from_url(REDIS_URL, decode_responses=True)


# ============================================================================
# Air Quality API - Dynamic Station Fetching
# ============================================================================

AIR_API_URL = "https://air-api.sviva.gov.il/v1/envista"
AIR_SITE_URL = "https://air.sviva.gov.il/"

_api_token_cache = {"token": None, "expires": 0}
_stations_cache = {"stations": [], "by_region": {}, "expires": 0}


def get_api_token() -> str:
    """Get a fresh API token from the air quality website."""
    import time
    if _api_token_cache["token"] and time.time() < _api_token_cache["expires"]:
        return _api_token_cache["token"]
    try:
        response = httpx.get(AIR_SITE_URL, timeout=10.0)
        if response.status_code == 200:
            match = re.search(r"ApiToken ([a-f0-9-]+)", response.text)
            if match:
                token = match.group(1)
                _api_token_cache["token"] = token
                _api_token_cache["expires"] = time.time() + 300
                return token
    except:
        pass
    return ""


# Region ID to region code mapping
REGION_ID_MAP = {
    0: "other", 1: "haifa", 2: "haifa", 3: "north", 4: "sharon",
    5: "center", 6: "center", 7: "tel_aviv", 8: "jerusalem",
    9: "south", 10: "coastal", 11: "south", 12: "south",
    13: "north", 14: "north", 15: "north",
}


def get_stations_by_region() -> dict:
    """Fetch all stations from API grouped by region, with caching."""
    import time
    if _stations_cache["by_region"] and time.time() < _stations_cache["expires"]:
        return _stations_cache["by_region"]

    api_token = get_api_token()
    if not api_token:
        return _stations_cache.get("by_region", {})

    try:
        response = httpx.get(
            f"{AIR_API_URL}/stations",
            headers={"Authorization": f"ApiToken {api_token}"},
            timeout=30.0,
        )
        if response.status_code == 200:
            raw_stations = response.json()
            by_region = {}
            all_stations = []
            for s in raw_stations:
                if not s.get("active", False):
                    continue
                region_id = s.get("regionId", 0)
                region = REGION_ID_MAP.get(region_id, "other")
                city = s.get("city") or s["name"]
                station_name = s["name"]
                # Use city as display name, add station name if different
                if city and city != station_name:
                    display_name = f"{city} ({station_name})"
                else:
                    display_name = station_name
                station = {
                    "id": s["stationId"],
                    "name": station_name,
                    "city": city,
                    "display_name": display_name,
                    "region": region,
                }
                all_stations.append(station)
                if region not in by_region:
                    by_region[region] = []
                by_region[region].append(station)

            # Sort stations in each region by city name
            for region in by_region:
                by_region[region].sort(key=lambda x: x["city"])

            _stations_cache["stations"] = all_stations
            _stations_cache["by_region"] = by_region
            _stations_cache["expires"] = time.time() + 3600  # Cache 1 hour
    except:
        pass

    return _stations_cache.get("by_region", {})


# ============================================================================
# Region Data
# ============================================================================

REGIONS = {
    "1": {"id": "tel_aviv", "name": "תל אביב"},
    "2": {"id": "center", "name": "מרכז"},
    "3": {"id": "jerusalem", "name": "ירושלים"},
    "4": {"id": "haifa", "name": "חיפה"},
    "5": {"id": "south", "name": "דרום"},
    "6": {"id": "coastal", "name": "מישור החוף"},
    "7": {"id": "sharon", "name": "שרון"},
    "8": {"id": "north", "name": "צפון"},
}

REGION_NAMES_HE = {
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

# ============================================================================
# Alert Levels
# ============================================================================

ALERT_LEVELS = {
    "1": {"id": "GOOD", "name": "טוב", "desc": "התראה רק כשיורד מטוב"},
    "2": {"id": "MODERATE", "name": "בינוני", "desc": "התראה כשיורד מבינוני"},
    "3": {"id": "LOW", "name": "לא בריא", "desc": "התראה כשלא בריא"},
    "4": {"id": "VERY_LOW", "name": "מסוכן", "desc": "התראה רק במצב מסוכן"},
}

# ============================================================================
# Time Windows
# ============================================================================

TIME_WINDOWS = {
    "1": {"id": "morning", "name": "בוקר", "start": 6, "end": 12},
    "2": {"id": "afternoon", "name": "צהריים", "start": 12, "end": 18},
    "3": {"id": "evening", "name": "ערב", "start": 18, "end": 22},
    "4": {"id": "night", "name": "לילה", "start": 22, "end": 6},
}


# ============================================================================
# Hebrew Messages
# ============================================================================

WELCOME_MESSAGE = """שלום! 👋
ברוכים הבאים לבוט התראות איכות האוויר.

באילו אזורים תרצו לקבל התראות?

1️⃣ תל אביב
2️⃣ מרכז
3️⃣ ירושלים
4️⃣ חיפה
5️⃣ דרום
6️⃣ מישור החוף
7️⃣ שרון
8️⃣ צפון
9️⃣ 🏙️ בחירת עיר ספציפית

שלחו מספרים (לדוגמה: 1,3) או "הכל" לכל האזורים
או 9 לבחירת ערים ספציפיות."""

REGION_DRILLDOWN_MESSAGE = """🏙️ מאיזה אזור תרצו לבחור עיר?

1️⃣ תל אביב
2️⃣ מרכז
3️⃣ ירושלים
4️⃣ חיפה
5️⃣ דרום
6️⃣ מישור החוף
7️⃣ שרון
8️⃣ צפון

שלחו מספר אזור (1-8)"""

LEVEL_MESSAGE = """🎚️ באיזה מצב לשלוח התראה?

1️⃣ טוב - התראה כשיורד מאיכות טובה
2️⃣ בינוני - התראה כשיורד מבינוני (מומלץ)
3️⃣ לא בריא - התראה רק כשלא בריא
4️⃣ מסוכן - התראה רק במצב מסוכן

שלחו מספר בודד (1-4)"""

TIME_MESSAGE = """🕐 מתי לשלוח התראות?

1️⃣ בוקר (06:00-12:00)
2️⃣ צהריים (12:00-18:00)
3️⃣ ערב (18:00-22:00)
4️⃣ לילה (22:00-06:00)

שלחו מספרים (לדוגמה: 1,2,3) או "תמיד" לכל השעות"""

EXISTING_USER_MESSAGE = """👋 שלום! יש לך כבר הגדרות פעילות:

{status}

📌 פקודות:
• "שנה" - לשינוי ההגדרות
• "אזורים" - לשינוי האזורים/ערים
• "רמה" - לשינוי סף ההתראה
• "שעות" - לשינוי שעות ההתראה
• "עצור" - להפסקת ההתראות"""

STOPPED_MESSAGE = """👋 הוסרתם מרשימת ההתראות.

לחזרה, שלחו הודעה כלשהי."""

HELP_MESSAGE = """📌 פקודות זמינות:

• "אזורים" - לבחירת אזורים חדשים
• "רמה" - לשינוי סף ההתראה
• "שעות" - לשינוי שעות ההתראה
• "סטטוס" - לצפייה בהגדרות
• "עצור" - להפסקת ההתראות
• "עזרה" - הצגת הודעה זו"""

INVALID_INPUT_MESSAGE = """❌ לא הבנתי את הבחירה.

שלחו מספרים מ-1 עד 8 מופרדים בפסיק.
לדוגמה: 1,3,5

או שלחו "הכל" לכל האזורים."""

INVALID_LEVEL_MESSAGE = """❌ לא הבנתי את הבחירה.

שלחו מספר בודד מ-1 עד 4."""

INVALID_HOURS_MESSAGE = """❌ לא הבנתי את הבחירה.

שלחו מספרים מ-1 עד 4 מופרדים בפסיק.
לדוגמה: 1,2,3

או שלחו "תמיד" לכל השעות."""


# ============================================================================
# User State Management
# ============================================================================

def get_user(phone: str) -> Optional[dict]:
    """Get user data from Redis."""
    r = get_redis()
    data = r.hget("users", phone)
    return json.loads(data) if data else None


def save_user(phone: str, regions: Optional[List[str]] = None, stations: Optional[List[int]] = None,
              level: str = "MODERATE", hours: Optional[List[str]] = None):
    """Save user with their regions/stations, alert level, and hours to Redis."""
    if hours is None:
        hours = ["morning", "afternoon", "evening", "night"]
    if regions is None:
        regions = []
    if stations is None:
        stations = []

    r = get_redis()
    user_data = {
        "phone": phone,
        "regions": regions,
        "stations": stations,
        "level": level,
        "hours": hours
    }
    r.hset("users", phone, json.dumps(user_data))

    # Clear old indexes
    for key in r.scan_iter("region:*"):
        r.srem(key, phone)
    for key in r.scan_iter("station:*"):
        r.srem(key, phone)

    # Add to region indexes
    for region_id in regions:
        r.sadd(f"region:{region_id}", phone)

    # Add to station indexes
    for station_id in stations:
        r.sadd(f"station:{station_id}", phone)


def update_user_level(phone: str, level: str):
    """Update user's alert level."""
    user = get_user(phone)
    if user:
        save_user(phone, user.get("regions", []), user.get("stations", []),
                  level, user.get("hours"))


def update_user_hours(phone: str, hours: List[str]):
    """Update user's alert hours."""
    user = get_user(phone)
    if user:
        save_user(phone, user.get("regions", []), user.get("stations", []),
                  user.get("level", "MODERATE"), hours)


def delete_user(phone: str):
    """Remove user from Redis."""
    r = get_redis()
    user = get_user(phone)
    if user:
        for region_id in user.get("regions", []):
            r.srem(f"region:{region_id}", phone)
        for station_id in user.get("stations", []):
            r.srem(f"station:{station_id}", phone)
    r.hdel("users", phone)


def get_user_state(phone: str) -> Optional[str]:
    """Get user's conversation state."""
    r = get_redis()
    return r.hget("user_states", phone)


def set_user_state(phone: str, state: str, data: Optional[dict] = None):
    """Set user's conversation state with optional data."""
    r = get_redis()
    if state:
        r.hset("user_states", phone, state)
        if data:
            r.hset("user_state_data", phone, json.dumps(data))
    else:
        r.hdel("user_states", phone)
        r.hdel("user_state_data", phone)


def get_user_state_data(phone: str) -> Optional[dict]:
    """Get user's conversation state data."""
    r = get_redis()
    data = r.hget("user_state_data", phone)
    return json.loads(data) if data else None


# ============================================================================
# Message Processing Helpers
# ============================================================================

def parse_region_input(text: str) -> Optional[List[str]]:
    """Parse user input for region selection."""
    text = text.strip()
    if text in ["הכל", "כולם", "all"]:
        return [r["id"] for r in REGIONS.values()]
    try:
        numbers = [n.strip() for n in text.replace(" ", ",").split(",") if n.strip()]
        regions = []
        for num in numbers:
            if num in REGIONS:
                regions.append(REGIONS[num]["id"])
            else:
                return None
        return regions if regions else None
    except:
        return None


def get_region_names(region_ids: List[str]) -> str:
    """Get Hebrew names for region IDs."""
    names = [REGION_NAMES_HE.get(r, r) for r in region_ids]
    return ", ".join(names) if names else "אין"


def get_station_names(station_ids: List[int]) -> str:
    """Get Hebrew names for station IDs from cached data."""
    stations = _stations_cache.get("stations", [])
    names = []
    for s in stations:
        if s["id"] in station_ids:
            # Use city name for display
            names.append(s.get("city") or s["name"])
    return ", ".join(names) if names else "אין"


def get_location_display(user: dict) -> str:
    """Get display string for user's locations."""
    regions = user.get("regions", [])
    stations = user.get("stations", [])

    if stations:
        return f"🏙️ ערים: {get_station_names(stations)}"
    elif regions:
        return f"🗺️ אזורים: {get_region_names(regions)}"
    else:
        return "🗺️ אזורים: אין"


def parse_level_input(text: str) -> Optional[str]:
    """Parse user input for level selection."""
    text = text.strip()
    if text in ALERT_LEVELS:
        return ALERT_LEVELS[text]["id"]
    return None


def get_level_name(level_id: str) -> str:
    """Get Hebrew name for level ID."""
    for level in ALERT_LEVELS.values():
        if level["id"] == level_id:
            return level["name"]
    return "בינוני"


def parse_hours_input(text: str) -> Optional[List[str]]:
    """Parse user input for hours selection."""
    text = text.strip()
    if text in ["תמיד", "כל השעות", "always", "הכל"]:
        return [t["id"] for t in TIME_WINDOWS.values()]
    try:
        numbers = [n.strip() for n in text.replace(" ", ",").split(",") if n.strip()]
        hours = []
        for num in numbers:
            if num in TIME_WINDOWS:
                hours.append(TIME_WINDOWS[num]["id"])
            else:
                return None
        return hours if hours else None
    except:
        return None


def get_hours_names(hour_ids: List[str]) -> str:
    """Get Hebrew names for hour IDs."""
    if set(hour_ids) == {"morning", "afternoon", "evening", "night"}:
        return "תמיד"
    names = []
    for t in TIME_WINDOWS.values():
        if t["id"] in hour_ids:
            names.append(t["name"])
    return ", ".join(names) if names else "אין"


def format_user_status(user: dict) -> str:
    """Format user status with location display."""
    location = get_location_display(user)
    level = get_level_name(user.get("level", "MODERATE"))
    hours = get_hours_names(user.get("hours", ["morning", "afternoon", "evening", "night"]))
    return f"{location}\n🎚️ סף התראה: {level}\n🕐 שעות: {hours}"


def build_cities_message(region_code: str) -> str:
    """Build a message showing cities in a specific region."""
    by_region = get_stations_by_region()
    stations = by_region.get(region_code, [])
    region_name = REGION_NAMES_HE.get(region_code, region_code)

    if not stations:
        return f"❌ לא נמצאו תחנות באזור {region_name}"

    lines = [f"🏙️ ערים באזור *{region_name}*:", ""]
    for i, station in enumerate(stations, 1):
        lines.append(f"{i}. {station['display_name']}")

    lines.append("")
    lines.append("שלחו מספרי ערים (לדוגמה: 1,3,5)")
    lines.append("או \"חזור\" לחזרה לבחירת אזור")

    return "\n".join(lines)


def parse_city_selection(text: str, region_code: str) -> Optional[List[int]]:
    """Parse city selection within a region."""
    by_region = get_stations_by_region()
    stations = by_region.get(region_code, [])

    if not stations:
        return None

    try:
        numbers = [n.strip() for n in text.replace(" ", ",").split(",") if n.strip()]
        selected = []
        for num in numbers:
            idx = int(num) - 1
            if 0 <= idx < len(stations):
                selected.append(stations[idx]["id"])
            else:
                return None
        return selected if selected else None
    except:
        return None


# ============================================================================
# Main Message Processing
# ============================================================================

def process_message(phone: str, message: str) -> str:
    """Process incoming message and return response."""
    message = message.strip()
    user = get_user(phone)
    state = get_user_state(phone)
    msg_lower = message.lower()

    # Command handling
    if msg_lower in ["עצור", "stop", "הפסק"]:
        delete_user(phone)
        set_user_state(phone, None)
        return STOPPED_MESSAGE

    if msg_lower in ["עזרה", "help", "?"]:
        return HELP_MESSAGE

    if msg_lower in ["סטטוס", "status", "מצב"]:
        if user:
            return f"""📊 הגדרות נוכחיות:

{format_user_status(user)}

📌 פקודות:
• "שנה" - לשינוי ההגדרות
• "אזורים" - לשינוי האזורים/ערים
• "רמה" - לשינוי סף ההתראה
• "שעות" - לשינוי שעות ההתראה
• "עצור" - להפסקת ההתראות"""
        else:
            set_user_state(phone, "selecting_regions")
            return WELCOME_MESSAGE

    if msg_lower in ["אזורים", "regions", "שנה", "ערים", "עיר"]:
        set_user_state(phone, "selecting_regions")
        return WELCOME_MESSAGE

    if msg_lower in ["רמה", "level", "סף"]:
        if user:
            set_user_state(phone, "selecting_level")
            return LEVEL_MESSAGE
        else:
            set_user_state(phone, "selecting_regions")
            return WELCOME_MESSAGE

    if msg_lower in ["שעות", "hours", "זמן"]:
        if user:
            set_user_state(phone, "selecting_hours")
            return TIME_MESSAGE
        else:
            set_user_state(phone, "selecting_regions")
            return WELCOME_MESSAGE

    # State: selecting hours (existing user)
    if state == "selecting_hours":
        hours = parse_hours_input(message)
        if hours:
            update_user_hours(phone, hours)
            set_user_state(phone, None)
            user = get_user(phone)
            return f"✅ שעות ההתראה עודכנו!\n\n{format_user_status(user)}"
        else:
            return INVALID_HOURS_MESSAGE

    # State: selecting level (existing user)
    if state == "selecting_level":
        level = parse_level_input(message)
        if level:
            update_user_level(phone, level)
            set_user_state(phone, None)
            user = get_user(phone)
            return f"✅ סף ההתראה עודכן!\n\n{format_user_status(user)}"
        else:
            return INVALID_LEVEL_MESSAGE

    # State: selecting region for city drill-down
    if state == "selecting_region_drilldown":
        if message.strip() in REGIONS:
            region_code = REGIONS[message.strip()]["id"]
            set_user_state(phone, "selecting_cities", {"region": region_code})
            return build_cities_message(region_code)
        else:
            return "❌ שלחו מספר אזור מ-1 עד 8"

    # State: selecting cities within a region
    if state == "selecting_cities":
        state_data = get_user_state_data(phone)
        region_code = state_data.get("region") if state_data else None

        if msg_lower in ["חזור", "back"]:
            set_user_state(phone, "selecting_region_drilldown")
            return REGION_DRILLDOWN_MESSAGE

        if region_code:
            stations = parse_city_selection(message, region_code)
            if stations:
                if user:
                    save_user(phone, [], stations, user.get("level", "MODERATE"), user.get("hours"))
                    set_user_state(phone, None)
                    user = get_user(phone)
                    return f"✅ הערים עודכנו!\n\n{format_user_status(user)}"
                else:
                    r = get_redis()
                    r.hset("pending_stations", phone, json.dumps(stations))
                    r.hdel("pending_regions", phone)
                    set_user_state(phone, "selecting_level_new")
                    return LEVEL_MESSAGE
            else:
                return f"❌ לא הבנתי. {build_cities_message(region_code)}"

        set_user_state(phone, "selecting_region_drilldown")
        return REGION_DRILLDOWN_MESSAGE

    # State: selecting regions
    if state == "selecting_regions":
        # Option 9: drill-down to cities
        if message.strip() == "9":
            set_user_state(phone, "selecting_region_drilldown")
            return REGION_DRILLDOWN_MESSAGE

        regions = parse_region_input(message)
        if regions:
            if user:
                save_user(phone, regions, [], user.get("level", "MODERATE"), user.get("hours"))
                set_user_state(phone, None)
                user = get_user(phone)
                return f"✅ האזורים עודכנו!\n\n{format_user_status(user)}"
            else:
                r = get_redis()
                r.hset("pending_regions", phone, json.dumps(regions))
                r.hdel("pending_stations", phone)
                set_user_state(phone, "selecting_level_new")
                return LEVEL_MESSAGE
        else:
            return INVALID_INPUT_MESSAGE

    # State: new user selecting level
    if state == "selecting_level_new":
        level = parse_level_input(message)
        if level:
            r = get_redis()
            r.hset("pending_level", phone, level)
            set_user_state(phone, "selecting_hours_new")
            return TIME_MESSAGE
        else:
            return INVALID_LEVEL_MESSAGE

    # State: new user selecting hours
    if state == "selecting_hours_new":
        hours = parse_hours_input(message)
        if hours:
            r = get_redis()
            regions_json = r.hget("pending_regions", phone)
            stations_json = r.hget("pending_stations", phone)
            regions = json.loads(regions_json) if regions_json else []
            stations = json.loads(stations_json) if stations_json else []
            level = r.hget("pending_level", phone) or "MODERATE"
            r.hdel("pending_regions", phone)
            r.hdel("pending_stations", phone)
            r.hdel("pending_level", phone)
            save_user(phone, regions, stations, level, hours)
            set_user_state(phone, None)
            user = get_user(phone)
            return f"""✅ נרשמתם בהצלחה!

{format_user_status(user)}

📌 פקודות:
• "אזורים" - לשינוי האזורים/ערים
• "רמה" - לשינוי סף ההתראה
• "שעות" - לשינוי שעות ההתראה
• "סטטוס" - לצפייה בהגדרות
• "עצור" - להפסקת ההתראות"""
        else:
            return INVALID_HOURS_MESSAGE

    # Existing user with no state - show current settings
    if user and not state:
        return EXISTING_USER_MESSAGE.format(status=format_user_status(user))

    # New user - start registration flow
    if not user:
        set_user_state(phone, "selecting_regions")
        return WELCOME_MESSAGE

    # Default
    return HELP_MESSAGE


# ============================================================================
# Twilio Response Formatting
# ============================================================================

def twiml_response(message: str) -> str:
    """Format response as TwiML."""
    message = message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Message>{message}</Message>
</Response>"""


# ============================================================================
# DigitalOcean Functions Entry Point
# ============================================================================

def normalize_phone(phone: str) -> str:
    """Normalize phone number to consistent format (+972...)."""
    phone = phone.replace("whatsapp:", "")
    phone = phone.strip()
    if phone and not phone.startswith("+"):
        phone = "+" + phone
    return phone


def main(args: dict) -> dict:
    """Handle incoming Twilio webhook."""
    body = args.get("Body", "")
    from_number = args.get("From", "").replace("whatsapp:", "")

    if "__ow_body" in args:
        import base64
        try:
            decoded = base64.b64decode(args["__ow_body"]).decode("utf-8")
            parsed = parse_qs(decoded)
            body = parsed.get("Body", [""])[0]
            from_number = parsed.get("From", [""])[0].replace("whatsapp:", "")
        except:
            pass

    from_number = normalize_phone(from_number)

    if not from_number:
        return {"statusCode": 400, "body": "Missing From number"}

    response_text = process_message(from_number, body)

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "text/xml"},
        "body": twiml_response(response_text)
    }
