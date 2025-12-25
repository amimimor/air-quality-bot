"""
Telegram Webhook Handler for Air Quality Alert Bot

Handles incoming Telegram messages and manages user registration
through a conversational Hebrew interface.
"""

import json
import os
import ssl
from typing import Optional, List
import httpx
import redis

# ============================================================================
# Configuration
# ============================================================================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
REDIS_URL = os.environ.get("REDIS_URL", "")
AIR_API_URL = "https://air-api.sviva.gov.il/v1/envista"
AIR_WEB_URL = "https://air.sviva.gov.il"

# Redis connection with SSL
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

redis_client = redis.from_url(
    REDIS_URL,
    decode_responses=True,
    ssl_cert_reqs=None,
) if REDIS_URL else None

# ============================================================================
# Readings Cache (3 minute TTL)
# ============================================================================

READINGS_CACHE_TTL = 180  # 3 minutes


def get_cached_reading(station_id: int) -> Optional[dict]:
    """Get cached reading for a station."""
    if not redis_client:
        return None
    data = redis_client.get(f"reading:{station_id}")
    if data:
        return json.loads(data)
    return None


def set_cached_reading(station_id: int, reading: dict):
    """Cache a station reading."""
    if redis_client:
        redis_client.setex(f"reading:{station_id}", READINGS_CACHE_TTL, json.dumps(reading))


# ============================================================================
# API Token Management
# ============================================================================

_api_token_cache = {"token": None, "expires": 0}


def get_api_token() -> Optional[str]:
    """Get API token from air.sviva.gov.il, with caching."""
    import time
    if _api_token_cache["token"] and time.time() < _api_token_cache["expires"]:
        return _api_token_cache["token"]

    try:
        response = httpx.get(AIR_WEB_URL, timeout=10.0)
        if response.status_code == 200:
            import re
            match = re.search(r'ApiToken\s+([a-f0-9-]+)', response.text)
            if match:
                _api_token_cache["token"] = match.group(1)
                _api_token_cache["expires"] = time.time() + 3600
                return _api_token_cache["token"]
    except:
        pass
    return _api_token_cache.get("token")


# ============================================================================
# Station Data
# ============================================================================

_stations_cache = {"stations": [], "by_region": {}, "expires": 0}

REGION_ID_MAP = {
    2: "haifa", 3: "galilee", 4: "carmel",
    5: "west_bank", 6: "center", 7: "tel_aviv",
    8: "jerusalem", 9: "dead_sea", 10: "south",
    11: "negev", 12: "eilat", 13: "jezreel",
    14: "north", 15: "north",
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
                raw_city = s.get("city")
                station_name = s["name"]
                # Handle None, empty string, or literal "None" string from API
                if raw_city and raw_city != "None" and raw_city != station_name:
                    city = raw_city
                    display_name = f"{city} ({station_name})"
                else:
                    city = station_name
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

            for region in by_region:
                by_region[region].sort(key=lambda x: x["city"])

            _stations_cache["stations"] = all_stations
            _stations_cache["by_region"] = by_region
            _stations_cache["expires"] = time.time() + 3600
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
    "5": {"id": "north", "name": "צפון"},
    "6": {"id": "south", "name": "דרום"},
    "7": {"id": "negev", "name": "נגב"},
    "8": {"id": "eilat", "name": "אילת"},
}

REGION_NAMES_HE = {
    "tel_aviv": "תל אביב",
    "center": "מרכז",
    "jerusalem": "ירושלים",
    "haifa": "חיפה",
    "north": "צפון",
    "south": "דרום",
    "negev": "נגב",
    "eilat": "אילת",
    "carmel": "כרמל",
    "galilee": "גליל",
    "jezreel": "עמק יזרעאל",
    "dead_sea": "ים המלח",
    "west_bank": "יהודה ושומרון",
}

ALERT_LEVELS = {
    "1": {"id": "GOOD", "name": "טוב (רגיש מאוד)"},
    "2": {"id": "MODERATE", "name": "בינוני (מומלץ)"},
    "3": {"id": "LOW", "name": "נמוך"},
    "4": {"id": "VERY_LOW", "name": "נמוך מאוד"},
}

TIME_WINDOWS = {
    "1": {"id": "morning", "name": "בוקר (6:00-12:00)"},
    "2": {"id": "afternoon", "name": "צהריים (12:00-18:00)"},
    "3": {"id": "evening", "name": "ערב (18:00-22:00)"},
    "4": {"id": "night", "name": "לילה (22:00-6:00)"},
}


# ============================================================================
# Messages
# ============================================================================

WELCOME_MESSAGE = """🌬️ שלום! אני בוט התראות איכות אוויר.

אשלח לך התראות כשאיכות האוויר יורדת באזור שלך.

📍 *שלב 1: בחירת אזור*

בחרו אזור (שלחו מספר):
1. תל אביב
2. מרכז
3. ירושלים
4. חיפה
5. צפון
6. דרום
7. נגב
8. אילת

או שלחו *9* לבחירת עיר ספציפית"""

REGION_DRILLDOWN_MESSAGE = """🗺️ *בחירת אזור לצפייה בערים*

בחרו אזור (שלחו מספר):
1. תל אביב
2. מרכז
3. ירושלים
4. חיפה וקריות
5. צפון וגליל
6. שפלה ודרום
7. נגב
8. אילת וערבה

או "חזור" לבחירה רגילה"""

LEVEL_MESSAGE = """🎚️ *שלב 2: סף התראה*

באיזו רמת איכות אוויר לשלוח התראה?

1. טוב - התראה גם על ירידה קלה (לרגישים)
2. בינוני - התראה על איכות בינונית ומטה (מומלץ)
3. נמוך - התראה רק על איכות נמוכה
4. נמוך מאוד - התראה רק על איכות גרועה מאוד"""

HOURS_MESSAGE = """🕐 *שלב 3: שעות התראה*

באילו שעות לשלוח התראות?

1. בוקר (6:00-12:00)
2. צהריים (12:00-18:00)
3. ערב (18:00-22:00)
4. לילה (22:00-6:00)

שלחו מספרים מופרדים בפסיק (לדוגמה: 1,2,3)
או "תמיד" לכל השעות"""

# Centralized commands list - single source of truth
COMMANDS_TEXT = """📌 *פקודות:*
• /now - מצב איכות האוויר כרגע
• /status - הצגת ההגדרות
• /thresholds - סף התראה לפי מזהם
• /change - שינוי כל ההגדרות
• /regions - שינוי אזורים/ערים
• /level - שינוי סף התראה
• /hours - שינוי שעות
• /stop - הפסקת התראות
• /help - עזרה"""

THRESHOLDS_MESSAGE = """📊 <b>מדדי התראה</b>

<b>🌬️ מדד AQI (ישראלי)</b>
<code>100 = מצוין, 0 = גרוע</code>
• טוב: &gt;50
• בינוני: 0-50
• לא בריא: 0 עד -100
• מסוכן: &lt;-100

<b>⚗️ בנזן (ppb)</b>
<code>אין סף בטוח - מסרטן</code>
• מוגבר: ≥0.3 (~1 µg/m³)
• גבוה: ≥1.2 (תקן ישראלי)
• גבוה מאוד: ≥1.6 (גבול EU)
• מסוכן: ≥2.5

<b>⚠️ המלצות לפי רמה</b>
• מוגבר → הגבילו פעילות בחוץ
• גבוה+ → הישארו בפנים

🔗 https://air.sviva.gov.il"""

COMPLETE_MESSAGE = f"""✅ *ההרשמה הושלמה!*

{{status}}

תקבלו התראות כשאיכות האוויר תרד מתחת לסף שהגדרתם.

{COMMANDS_TEXT}"""

EXISTING_USER_MESSAGE = f"""👋 שלום! יש לך כבר הגדרות פעילות:

{{status}}

{COMMANDS_TEXT}"""

STOPPED_MESSAGE = """⏹️ ההתראות הופסקו.

שלחו /start כדי להתחיל מחדש."""

HELP_MESSAGE = f"""🌬️ *בוט התראות איכות אוויר*

{COMMANDS_TEXT}

המידע מבוסס על נתוני משרד הגנת הסביבה."""


# ============================================================================
# User Management
# ============================================================================

def get_user(chat_id: str) -> Optional[dict]:
    """Get user from Redis."""
    if not redis_client:
        return None
    try:
        data = redis_client.get(f"telegram:user:{chat_id}")
        return json.loads(data) if data else None
    except:
        return None


def save_user(
    chat_id: str,
    regions: Optional[List[str]] = None,
    stations: Optional[List[int]] = None,
    level: str = "MODERATE",
    hours: Optional[List[str]] = None,
    active: bool = True,
) -> bool:
    """Save user to Redis."""
    if not redis_client:
        return False
    try:
        user = {
            "chat_id": chat_id,
            "regions": regions or [],
            "stations": stations or [],
            "level": level,
            "hours": hours or ["morning", "afternoon", "evening", "night"],
            "active": active,
            "platform": "telegram",
        }
        redis_client.set(f"telegram:user:{chat_id}", json.dumps(user))
        redis_client.sadd("telegram:users", chat_id)
        return True
    except:
        return False


def update_user(chat_id: str, **kwargs) -> bool:
    """Update user fields in Redis."""
    user = get_user(chat_id)
    if not user:
        return False
    user.update(kwargs)
    try:
        redis_client.set(f"telegram:user:{chat_id}", json.dumps(user))
        return True
    except:
        return False


def delete_user(chat_id: str) -> bool:
    """Delete user from Redis."""
    if not redis_client:
        return False
    try:
        redis_client.delete(f"telegram:user:{chat_id}")
        redis_client.srem("telegram:users", chat_id)
        return True
    except:
        return False


def get_user_state(chat_id: str) -> Optional[dict]:
    """Get user's conversation state."""
    if not redis_client:
        return None
    try:
        data = redis_client.get(f"telegram:state:{chat_id}")
        return json.loads(data) if data else None
    except:
        return None


def set_user_state(chat_id: str, state: str, data: Optional[dict] = None):
    """Set user's conversation state with optional data."""
    if not redis_client:
        return
    try:
        state_data = {"state": state, "data": data or {}}
        redis_client.set(f"telegram:state:{chat_id}", json.dumps(state_data), ex=3600)
    except:
        pass


def clear_user_state(chat_id: str):
    """Clear user's conversation state."""
    if not redis_client:
        return
    try:
        redis_client.delete(f"telegram:state:{chat_id}")
    except:
        pass


# ============================================================================
# Display Helpers
# ============================================================================

def get_region_names(region_ids: List[str]) -> str:
    """Get Hebrew names for region IDs."""
    names = [REGION_NAMES_HE.get(r, r) for r in region_ids]
    return ", ".join(names) if names else "אין"


def get_station_names(station_ids: List[int]) -> str:
    """Get display names for station IDs from cached data."""
    stations = _stations_cache.get("stations", [])
    names = []
    for s in stations:
        if s["id"] in station_ids:
            # Use display_name which includes "Station, City" format
            names.append(s.get("display_name") or s.get("city") or s["name"])
    return ", ".join(names) if names else "אין"


def get_location_display(user: dict) -> str:
    """Get display string for user's locations."""
    regions = user.get("regions", [])
    stations = user.get("stations", [])

    if stations:
        return f"📍 תחנות: {get_station_names(stations)}"
    elif regions:
        return f"🗺️ אזורים: {get_region_names(regions)}"
    else:
        return "🗺️ אזורים: אין"


def get_level_name(level_id: str) -> str:
    """Get Hebrew name for level ID."""
    for level in ALERT_LEVELS.values():
        if level["id"] == level_id:
            return level["name"]
    return "בינוני"


def get_hours_names(hour_ids: List[str]) -> str:
    """Get Hebrew names for hour IDs."""
    if set(hour_ids) == {"morning", "afternoon", "evening", "night"}:
        return "כל השעות"
    names = []
    for tw in TIME_WINDOWS.values():
        if tw["id"] in hour_ids:
            names.append(tw["name"].split(" ")[0])
    return ", ".join(names) if names else "כל השעות"


def get_user_status(user: dict) -> str:
    """Get formatted status string for user."""
    location = get_location_display(user)
    level = get_level_name(user.get("level", "MODERATE"))
    hours = get_hours_names(user.get("hours", ["morning", "afternoon", "evening", "night"]))
    return f"{location}\n🎚️ סף התראה: {level}\n🕐 שעות: {hours}"


def get_user_status_html(user: dict) -> str:
    """Get styled HTML status for user."""
    # Location
    stations = user.get("stations", [])
    regions = user.get("regions", [])
    if stations:
        location = get_station_names(stations)
        location_line = f"📍 <b>תחנות:</b> <code>{location}</code>"
    elif regions:
        location = get_region_names(regions)
        location_line = f"🗺️ <b>אזורים:</b> <code>{location}</code>"
    else:
        location_line = "🗺️ <b>אזורים:</b> <i>לא הוגדרו</i>"

    # Level with color indicator
    level_id = user.get("level", "MODERATE")
    level_name = get_level_name(level_id)
    level_emoji = {"GOOD": "🟢", "MODERATE": "🟡", "LOW": "🟠", "VERY_LOW": "🔴"}.get(level_id, "⚪")

    # Hours
    hours = get_hours_names(user.get("hours", ["morning", "afternoon", "evening", "night"]))

    # Active status
    active = user.get("active", False)
    status_line = "🟢 <b>פעיל</b>" if active else "⏸ <b>מושהה</b>"

    return f"""📊 <b>הגדרות ההתראות שלך</b>

{location_line}

🎚️ <b>סף התראה:</b> {level_emoji} {level_name}

🕐 <b>שעות:</b> <code>{hours}</code>

{status_line}

━━━━━━━━━━━━━━━
<i>שנה הגדרות:</i> /change
<i>מדדי התראה:</i> /thresholds"""


def build_cities_message(region_code: str) -> str:
    """Build a message showing cities in a specific region."""
    by_region = get_stations_by_region()
    stations = by_region.get(region_code, [])
    region_name = REGION_NAMES_HE.get(region_code, region_code)

    if not stations:
        return f"❌ לא נמצאו תחנות באזור {region_name}"

    lines = [f"🏙️ *ערים באזור {region_name}:*", ""]
    for i, station in enumerate(stations, 1):
        lines.append(f"{i}. {station['display_name']}")

    lines.append("")
    lines.append("שלחו מספרי ערים (לדוגמה: 1,3,5)")
    lines.append('או "חזור" לחזרה לבחירת אזור')

    return "\n".join(lines)


# ============================================================================
# Input Parsing
# ============================================================================

def parse_region_input(text: str) -> Optional[List[str]]:
    """Parse user input for region selection."""
    text = text.strip()
    if text == "9":
        return None  # Trigger city drill-down
    if text in REGIONS:
        return [REGIONS[text]["id"]]
    return None


def parse_drilldown_region(text: str) -> Optional[str]:
    """Parse region selection for drill-down."""
    region_map = {
        "1": "tel_aviv",
        "2": "center",
        "3": "jerusalem",
        "4": "haifa",
        "5": "north",
        "6": "south",
        "7": "negev",
        "8": "eilat",
    }
    return region_map.get(text.strip())


def parse_city_selection(text: str, region_code: str) -> Optional[List[int]]:
    """Parse city selection within a region."""
    by_region = get_stations_by_region()
    stations = by_region.get(region_code, [])

    if not stations:
        return None

    try:
        numbers = [n.strip() for n in text.replace(" ", ",").split(",") if n.strip()]
        selected = []
        for n in numbers:
            idx = int(n) - 1
            if 0 <= idx < len(stations):
                selected.append(stations[idx]["id"])
        return selected if selected else None
    except:
        return None


def parse_level_input(text: str) -> Optional[str]:
    """Parse user input for level selection."""
    text = text.strip()
    if text in ALERT_LEVELS:
        return ALERT_LEVELS[text]["id"]
    return None


def parse_hours_input(text: str) -> Optional[List[str]]:
    """Parse user input for hours selection."""
    text = text.strip()
    if text in ["תמיד", "כל השעות", "always", "הכל"]:
        return [t["id"] for t in TIME_WINDOWS.values()]
    try:
        numbers = [n.strip() for n in text.replace(" ", ",").split(",") if n.strip()]
        hours = []
        for n in numbers:
            if n in TIME_WINDOWS:
                hours.append(TIME_WINDOWS[n]["id"])
        return hours if hours else None
    except:
        return None


# ============================================================================
# Telegram API
# ============================================================================

def send_message(chat_id: str, text: str, parse_mode: str = "Markdown") -> bool:
    """Send a message via Telegram Bot API."""
    if not TELEGRAM_BOT_TOKEN:
        return False
    try:
        response = httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode,
            },
            timeout=10.0,
        )
        return response.status_code == 200
    except:
        return False


# ============================================================================
# Message Handler
# ============================================================================

def handle_message(chat_id: str, text: str) -> str:
    """Process incoming message and return response."""
    text = text.strip()
    text_lower = text.lower()

    # Handle commands
    if text_lower.startswith("/"):
        return handle_command(chat_id, text_lower)

    # Get user and state
    user = get_user(chat_id)
    state_data = get_user_state(chat_id)
    state = state_data.get("state") if state_data else None
    data = state_data.get("data", {}) if state_data else {}

    # If user exists and no active state, show existing setup
    if user and not state:
        if text_lower in ["שנה", "change", "שינוי"]:
            set_user_state(chat_id, "selecting_region")
            return WELCOME_MESSAGE
        elif text_lower in ["אזורים", "regions", "ערים"]:
            set_user_state(chat_id, "selecting_region")
            return WELCOME_MESSAGE
        elif text_lower in ["רמה", "level", "סף"]:
            set_user_state(chat_id, "selecting_level")
            return LEVEL_MESSAGE
        elif text_lower in ["שעות", "hours"]:
            set_user_state(chat_id, "selecting_hours")
            return HOURS_MESSAGE
        elif text_lower in ["עצור", "stop", "הפסק"]:
            update_user(chat_id, active=False)
            clear_user_state(chat_id)
            return STOPPED_MESSAGE
        else:
            status = get_user_status(user)
            return EXISTING_USER_MESSAGE.format(status=status)

    # Handle state-based flow
    if state == "selecting_region":
        if text == "9":
            set_user_state(chat_id, "selecting_region_drilldown")
            return REGION_DRILLDOWN_MESSAGE

        regions = parse_region_input(text)
        if regions:
            save_user(chat_id, regions=regions)
            set_user_state(chat_id, "selecting_level")
            return LEVEL_MESSAGE
        return "❌ בחירה לא תקינה. שלחו מספר בין 1-9."

    elif state == "selecting_region_drilldown":
        if text_lower in ["חזור", "back"]:
            set_user_state(chat_id, "selecting_region")
            return WELCOME_MESSAGE

        region_code = parse_drilldown_region(text)
        if region_code:
            set_user_state(chat_id, "selecting_cities", {"region": region_code})
            return build_cities_message(region_code)
        return "❌ בחירה לא תקינה. שלחו מספר בין 1-8."

    elif state == "selecting_cities":
        if text_lower in ["חזור", "back"]:
            set_user_state(chat_id, "selecting_region_drilldown")
            return REGION_DRILLDOWN_MESSAGE

        region_code = data.get("region", "center")
        stations = parse_city_selection(text, region_code)
        if stations:
            save_user(chat_id, stations=stations)
            set_user_state(chat_id, "selecting_level")
            return LEVEL_MESSAGE
        return "❌ בחירה לא תקינה. שלחו מספרי ערים מופרדים בפסיק."

    elif state == "selecting_level":
        level = parse_level_input(text)
        if level:
            update_user(chat_id, level=level)
            set_user_state(chat_id, "selecting_hours")
            return HOURS_MESSAGE
        return "❌ בחירה לא תקינה. שלחו מספר בין 1-4."

    elif state == "selecting_hours":
        hours = parse_hours_input(text)
        if hours:
            update_user(chat_id, hours=hours, active=True)
            clear_user_state(chat_id)
            user = get_user(chat_id)
            status = get_user_status(user)
            return COMPLETE_MESSAGE.format(status=status)
        return "❌ בחירה לא תקינה. שלחו מספרים מופרדים בפסיק (1-4) או 'תמיד'."

    # No state - start registration
    set_user_state(chat_id, "selecting_region")
    return WELCOME_MESSAGE


def calculate_sub_index(value: float, breakpoints: list) -> float:
    """Calculate sub-index using Israeli piecewise linear interpolation."""
    for conc_lo, conc_hi, idx_lo, idx_hi in breakpoints:
        if conc_lo <= value <= conc_hi:
            return ((idx_hi - idx_lo) / (conc_hi - conc_lo)) * (value - conc_lo) + idx_lo
    return breakpoints[-1][3]


def calculate_aqi(pollutants: dict) -> int:
    """
    Calculate Air Quality Index using official Israeli formula.
    Israeli AQI: 100 = best, 0 = worst (inverted scale)
    """
    BREAKPOINTS = {
        "PM2.5": [(0, 18.5, 0, 49), (18.6, 37, 50, 100), (37.5, 84, 101, 200), (84.5, 130, 201, 300), (130.5, 165, 301, 400), (165.5, 200, 401, 500)],
        "PM10": [(0, 65, 0, 49), (66, 129, 50, 100), (130, 215, 101, 200), (216, 300, 201, 300), (301, 355, 301, 400), (356, 430, 401, 500)],
        "O3": [(0, 35, 0, 49), (36, 70, 50, 100), (71, 97, 101, 200), (98, 117, 201, 300), (118, 155, 301, 400), (156, 188, 401, 500)],
        "NO2": [(0, 53, 0, 49), (54, 105, 50, 100), (106, 160, 101, 200), (161, 213, 201, 300), (214, 260, 301, 400), (261, 316, 401, 500)],
        "SO2": [(0, 67, 0, 49), (68, 133, 50, 100), (134, 163, 101, 200), (164, 191, 201, 300), (192, 253, 301, 400), (254, 303, 401, 500)],
        "CO": [(0, 26, 0, 49), (27, 51, 50, 100), (52, 78, 101, 200), (79, 104, 201, 300), (105, 130, 301, 400), (131, 156, 401, 500)],
        "NOX": [(0, 250, 0, 49), (251, 499, 50, 100), (500, 750, 101, 200), (751, 1000, 201, 300), (1001, 1200, 301, 400), (1201, 1400, 401, 500)],
    }

    sub_indices = []
    for pollutant, breakpoints in BREAKPOINTS.items():
        value = pollutants.get(pollutant)
        if value is not None and value >= 0:
            sub_idx = calculate_sub_index(value, breakpoints)
            sub_indices.append(sub_idx)

    if not sub_indices:
        return 50

    # Israeli AQI = 100 - worst sub-index (can go negative)
    worst_sub_index = max(sub_indices)
    aqi = 100 - worst_sub_index
    return int(round(aqi))


def get_aqi_level(aqi: int) -> tuple:
    """Get AQI level name and emoji. Israeli scale: 100=best, negative=worst."""
    if aqi > 50:  # sub-index 0-49 = Good
        return "טוב", "🟢"
    elif aqi >= 0:  # sub-index 50-100 = Moderate
        return "בינוני", "🟡"
    elif aqi >= -100:  # sub-index 101-200 = Unhealthy for sensitive
        return "לא בריא לרגישים", "🟠"
    elif aqi >= -200:  # sub-index 201-300 = Unhealthy
        return "לא בריא", "🔴"
    else:  # sub-index > 300 = Hazardous
        return "מסוכן", "🟣"


def transform_pollutant_alias(name: str, alias: str) -> str:
    """
    Transform pollutant alias for cleaner display.
    e.g., "חלקיקים נשימים בגודל 2.5 מיקרון" -> "חלקיקים נשימים PM2.5"
    """
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


# Benzene thresholds in ppb (aligned with check-alerts)
# Conversion: 1 ppb Benzene = 3.19 µg/m³
BENZENE_THRESHOLDS = {
    "GOOD": 0.3,      # ~1 µg/m³
    "MODERATE": 1.2,  # ~3.8 µg/m³ (Israeli standard)
    "LOW": 1.6,       # ~5 µg/m³ (EU limit)
    "VERY_LOW": 2.5,  # ~8 µg/m³
}


def get_benzene_level(benzene_ppb: float) -> tuple:
    """Get benzene level name and emoji."""
    if benzene_ppb >= BENZENE_THRESHOLDS["VERY_LOW"]:
        return "מסוכן", "🟣"
    elif benzene_ppb >= BENZENE_THRESHOLDS["LOW"]:
        return "גבוה מאוד", "🔴"
    elif benzene_ppb >= BENZENE_THRESHOLDS["MODERATE"]:
        return "גבוה", "🟠"
    elif benzene_ppb >= BENZENE_THRESHOLDS["GOOD"]:
        return "מוגבר", "🟡"
    return None, None


def get_current_readings(user: dict) -> str:
    """Fetch and format current air quality readings for user's locations."""
    stations = user.get("stations", [])
    regions = user.get("regions", [])

    if not stations and not regions:
        return "❌ לא הגדרת אזורים או ערים. שלחו /change לשינוי ההגדרות."

    api_token = get_api_token()
    if not api_token:
        return "❌ שגיאה בגישה ל-API. נסו שוב מאוחר יותר."

    # Pre-load stations cache for name lookups
    get_stations_by_region()

    # Get stations to check - prioritize user's specific stations
    station_ids = []
    if stations:
        # User has specific stations selected
        station_ids = [int(s) for s in stations]  # Ensure integers
    elif regions:
        # User has regions - get representative stations
        by_region = get_stations_by_region()
        for region in regions:
            region_stations = by_region.get(region, [])
            # Take first 3 stations per region
            for s in region_stations[:3]:
                if s["id"] not in station_ids:
                    station_ids.append(s["id"])

    if not station_ids:
        return "❌ לא נמצאו תחנות לבדיקה."

    lines = ["📊 *מצב איכות האוויר כרגע:*", ""]

    for station_id in station_ids[:5]:  # Limit to 5 stations
        try:
            # Check cache first
            cached = get_cached_reading(station_id)
            if cached:
                pollutants = cached.get("pollutants", {})
                pollutant_meta = cached.get("pollutant_meta", {})
                aqi = cached.get("aqi", 50)
            else:
                # Fetch from API
                response = httpx.get(
                    f"{AIR_API_URL}/stations/{station_id}/data/latest",
                    headers={"Authorization": f"ApiToken {api_token}"},
                    timeout=10.0,
                )
                if response.status_code != 200:
                    continue

                data = response.json().get("data", [])
                if not data:
                    continue

                channels = data[0].get("channels", [])
                pollutants = {}
                pollutant_meta = {}
                for c in channels:
                    if c.get("valid"):
                        name = c["name"].upper()
                        pollutants[name] = c["value"]
                        pollutant_meta[name] = {
                            "alias": c.get("alias", c["name"]),
                            "units": c.get("units", ""),
                        }

                if not pollutants:
                    continue

                aqi = calculate_aqi(pollutants)

                # Cache the reading
                set_cached_reading(station_id, {
                    "pollutants": pollutants,
                    "pollutant_meta": pollutant_meta,
                    "aqi": aqi,
                })
            level_name, emoji = get_aqi_level(aqi)

            # Get station name with city
            station_name = str(station_id)
            all_stations = _stations_cache.get("stations", [])
            for s in all_stations:
                if s["id"] == station_id:
                    # Use display_name which includes "Station, City" format
                    station_name = s.get("display_name") or s.get("city") or s["name"]
                    break

            # Check for elevated benzene (not included in AQI)
            benzene_ppb = pollutants.get("BENZENE", 0)
            benzene_level_name, benzene_emoji = None, None
            if benzene_ppb:
                benzene_level_name, benzene_emoji = get_benzene_level(benzene_ppb)

            # Use worst case color between AQI and Benzene
            # Severity order: 🟢 < 🟡 < 🟠 < 🔴 < 🟣
            severity_order = {"🟢": 0, "🟡": 1, "🟠": 2, "🔴": 3, "🟣": 4}
            overall_emoji = emoji
            if benzene_emoji and severity_order.get(benzene_emoji, 0) > severity_order.get(emoji, 0):
                overall_emoji = benzene_emoji

            # Determine overall quality level (worst of AQI or Benzene)
            # Map benzene levels to quality terminology
            benzene_to_quality = {"מוגבר": "בינוני", "גבוה": "לא בריא", "גבוה מאוד": "לא בריא", "מסוכן": "מסוכן"}
            aqi_severity = {"טוב": 0, "בינוני": 1, "לא בריא לרגישים": 2, "לא בריא": 3, "מסוכן": 4}
            benzene_severity = {"מוגבר": 1, "גבוה": 2, "גבוה מאוד": 3, "מסוכן": 4}

            overall_level = level_name
            if benzene_level_name and benzene_severity.get(benzene_level_name, 0) > aqi_severity.get(level_name, 0):
                overall_level = benzene_to_quality.get(benzene_level_name, benzene_level_name)

            lines.append(f"{overall_emoji} *{station_name}*")
            lines.append(f"📊 איכות: {overall_level}")
            lines.append(f"🌬️ מדד AQI: {aqi} ({level_name})")

            # Show benzene level if elevated
            if benzene_level_name:
                lines.append(f"⚗️ בנזן: {benzene_level_name}")

            # Show pollutants with transformed Hebrew aliases
            for name, value in pollutants.items():
                if value is not None:
                    meta = pollutant_meta.get(name, {})
                    original_alias = meta.get("alias", name)
                    alias = transform_pollutant_alias(name, original_alias)
                    units = meta.get("units", "")
                    lines.append(f"• {alias}: {value:.1f} {units}")
            lines.append("")

        except Exception as e:
            continue

    if len(lines) <= 2:
        return "❌ לא הצלחתי לקבל נתונים. נסו שוב מאוחר יותר."

    lines.append("🔗 https://air.sviva.gov.il")
    return "\n".join(lines)


def handle_command(chat_id: str, command: str) -> str:
    """Handle Telegram commands."""
    user = get_user(chat_id)

    if command == "/start":
        if user and user.get("active"):
            status = get_user_status(user)
            return EXISTING_USER_MESSAGE.format(status=status)
        set_user_state(chat_id, "selecting_region")
        return WELCOME_MESSAGE

    elif command == "/stop":
        if user:
            update_user(chat_id, active=False)
        clear_user_state(chat_id)
        return STOPPED_MESSAGE

    elif command == "/status":
        if user:
            # Test HTML styling for specific user
            if chat_id == "7984476273":
                return ("HTML", get_user_status_html(user))
            status = get_user_status(user)
            active_status = "✅ פעיל" if user.get("active") else "⏹️ מושהה"
            return f"📊 *הסטטוס שלך:*\n\n{status}\n\nסטטוס: {active_status}"
        return "❌ אינך רשום עדיין. שלחו /start להרשמה."

    elif command == "/change":
        set_user_state(chat_id, "selecting_region")
        return WELCOME_MESSAGE

    elif command == "/regions":
        set_user_state(chat_id, "selecting_region")
        return WELCOME_MESSAGE

    elif command == "/level":
        if not user:
            return "❌ אינך רשום עדיין. שלחו /start להרשמה."
        set_user_state(chat_id, "selecting_level")
        return LEVEL_MESSAGE

    elif command == "/hours":
        if not user:
            return "❌ אינך רשום עדיין. שלחו /start להרשמה."
        set_user_state(chat_id, "selecting_hours")
        return HOURS_MESSAGE

    elif command == "/help":
        return HELP_MESSAGE

    elif command == "/thresholds":
        return ("HTML", THRESHOLDS_MESSAGE)

    elif command == "/now":
        if not user:
            return "❌ אינך רשום עדיין. שלחו /start להרשמה."
        return get_current_readings(user)

    return "❌ פקודה לא מוכרת. שלחו /help לרשימת הפקודות."


# ============================================================================
# Main Handler
# ============================================================================

def main(args: dict) -> dict:
    """Main entry point for Telegram webhook."""
    try:
        # Handle Telegram webhook verification
        if args.get("__ow_method") == "get":
            return {
                "statusCode": 200,
                "body": "OK",
            }

        # Parse incoming update
        body = args
        if "__ow_body" in args:
            import base64
            try:
                body = json.loads(base64.b64decode(args["__ow_body"]).decode())
            except:
                body = json.loads(args["__ow_body"])

        # Extract message
        message = body.get("message", {})
        if not message:
            # Could be an edited message or other update type
            message = body.get("edited_message", {})

        chat = message.get("chat", {})
        chat_id = str(chat.get("id", ""))
        text = message.get("text", "")

        if not chat_id or not text:
            return {"statusCode": 200, "body": "OK"}

        # Pre-load stations cache
        get_stations_by_region()

        # Process message
        response = handle_message(chat_id, text)

        # Send response (handle HTML tuple format)
        if isinstance(response, tuple) and response[0] == "HTML":
            send_message(chat_id, response[1], parse_mode="HTML")
        else:
            send_message(chat_id, response)

        return {"statusCode": 200, "body": "OK"}

    except Exception as e:
        print(f"Error: {e}")
        return {"statusCode": 200, "body": "OK"}
