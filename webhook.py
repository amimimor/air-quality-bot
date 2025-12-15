"""
Twilio WhatsApp Webhook Handler - Hebrew Questionnaire
Handles user registration and region preferences.
"""

import os
import json
import redis
from urllib.parse import parse_qs

from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# Redis Connection
# ============================================================================

REDIS_URL = os.environ.get("REDIS_URL")

def get_redis():
    """Get Redis connection."""
    return redis.from_url(REDIS_URL, decode_responses=True)


# ============================================================================
# Region Data
# ============================================================================

REGIONS = {
    "1": {"id": "tel_aviv", "name": "תל אביב"},
    "2": {"id": "center", "name": "מרכז"},
    "3": {"id": "jerusalem", "name": "ירושלים"},
    "4": {"id": "haifa", "name": "חיפה"},
    "5": {"id": "south", "name": "דרום"},
    "6": {"id": "sharon", "name": "שרון"},
    "7": {"id": "north", "name": "צפון"},
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
6️⃣ שרון
7️⃣ צפון

שלחו את המספרים המתאימים (לדוגמה: 1,3 לתל אביב וירושלים)
או שלחו "הכל" לקבלת התראות מכל האזורים."""

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

REGISTERED_MESSAGE = """✅ נרשמתם בהצלחה!

🗺️ אזורים: {regions}
🎚️ סף התראה: {level}
🕐 שעות: {hours}

📌 פקודות:
• "אזורים" - לשינוי האזורים
• "רמה" - לשינוי סף ההתראה
• "שעות" - לשינוי שעות ההתראה
• "סטטוס" - לצפייה בהגדרות
• "עצור" - להפסקת ההתראות"""

UPDATED_REGIONS_MESSAGE = """✅ האזורים עודכנו!

🗺️ אזורים: {regions}
🎚️ סף התראה: {level}
🕐 שעות: {hours}"""

UPDATED_LEVEL_MESSAGE = """✅ סף ההתראה עודכן!

🗺️ אזורים: {regions}
🎚️ סף התראה: {level}
🕐 שעות: {hours}"""

UPDATED_HOURS_MESSAGE = """✅ שעות ההתראה עודכנו!

🗺️ אזורים: {regions}
🎚️ סף התראה: {level}
🕐 שעות: {hours}"""

STATUS_MESSAGE = """📊 הגדרות נוכחיות:

🗺️ אזורים: {regions}
🎚️ סף התראה: {level}
🕐 שעות: {hours}

📌 פקודות:
• "אזורים" - לשינוי האזורים
• "רמה" - לשינוי סף ההתראה
• "שעות" - לשינוי שעות ההתראה
• "עצור" - להפסקת ההתראות"""

STOPPED_MESSAGE = """👋 הוסרתם מרשימת ההתראות.

לחזרה, שלחו הודעה כלשהי."""

INVALID_INPUT_MESSAGE = """❌ לא הבנתי את הבחירה.

שלחו מספרים מ-1 עד 7 מופרדים בפסיק.
לדוגמה: 1,3,5

או שלחו "הכל" לכל האזורים."""

HELP_MESSAGE = """📌 פקודות זמינות:

• "אזורים" - לבחירת אזורים חדשים
• "רמה" - לשינוי סף ההתראה
• "שעות" - לשינוי שעות ההתראה
• "סטטוס" - לצפייה בהגדרות
• "עצור" - להפסקת ההתראות
• "עזרה" - הצגת הודעה זו"""

INVALID_LEVEL_MESSAGE = """❌ לא הבנתי את הבחירה.

שלחו מספר בודד מ-1 עד 4."""

INVALID_HOURS_MESSAGE = """❌ לא הבנתי את הבחירה.

שלחו מספרים מ-1 עד 4 מופרדים בפסיק.
לדוגמה: 1,2,3

או שלחו "תמיד" לכל השעות."""


# ============================================================================
# User State Management
# ============================================================================

def get_user(phone: str) -> dict | None:
    """Get user data from Redis."""
    r = get_redis()
    data = r.hget("users", phone)
    return json.loads(data) if data else None


def save_user(phone: str, regions: list[str], level: str = "MODERATE", hours: list[str] = None):
    """Save user with their regions, alert level, and hours to Redis."""
    if hours is None:
        hours = ["morning", "afternoon", "evening", "night"]  # Default: all hours

    r = get_redis()
    user_data = {"phone": phone, "regions": regions, "level": level, "hours": hours}
    r.hset("users", phone, json.dumps(user_data))

    # Also maintain region -> users index for efficient lookups
    for region_id in REGIONS.values():
        region_key = f"region:{region_id['id']}"
        if region_id["id"] in regions:
            r.sadd(region_key, phone)
        else:
            r.srem(region_key, phone)


def update_user_level(phone: str, level: str):
    """Update user's alert level."""
    user = get_user(phone)
    if user:
        save_user(phone, user["regions"], level, user.get("hours"))


def update_user_hours(phone: str, hours: list[str]):
    """Update user's alert hours."""
    user = get_user(phone)
    if user:
        save_user(phone, user["regions"], user.get("level", "MODERATE"), hours)


def delete_user(phone: str):
    """Remove user from Redis."""
    r = get_redis()
    user = get_user(phone)
    if user:
        # Remove from all region sets
        for region_id in REGIONS.values():
            r.srem(f"region:{region_id['id']}", phone)
    r.hdel("users", phone)


def get_user_state(phone: str) -> str | None:
    """Get user's conversation state."""
    r = get_redis()
    return r.hget("user_states", phone)


def set_user_state(phone: str, state: str):
    """Set user's conversation state."""
    r = get_redis()
    if state:
        r.hset("user_states", phone, state)
    else:
        r.hdel("user_states", phone)


# ============================================================================
# Message Processing
# ============================================================================

def parse_region_input(text: str) -> list[str] | None:
    """Parse user input for region selection."""
    text = text.strip()

    # Handle "all" in Hebrew
    if text in ["הכל", "כולם", "all"]:
        return [r["id"] for r in REGIONS.values()]

    # Parse comma-separated numbers
    try:
        numbers = [n.strip() for n in text.replace(" ", ",").split(",") if n.strip()]
        regions = []
        for num in numbers:
            if num in REGIONS:
                regions.append(REGIONS[num]["id"])
            else:
                return None  # Invalid number
        return regions if regions else None
    except:
        return None


def get_region_names(region_ids: list[str]) -> str:
    """Get Hebrew names for region IDs."""
    names = []
    for r in REGIONS.values():
        if r["id"] in region_ids:
            names.append(r["name"])
    return ", ".join(names) if names else "אין"


def parse_level_input(text: str) -> str | None:
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


def parse_hours_input(text: str) -> list[str] | None:
    """Parse user input for hours selection."""
    text = text.strip()

    # Handle "always" in Hebrew
    if text in ["תמיד", "כל השעות", "always", "הכל"]:
        return [t["id"] for t in TIME_WINDOWS.values()]

    # Parse comma-separated numbers
    try:
        numbers = [n.strip() for n in text.replace(" ", ",").split(",") if n.strip()]
        hours = []
        for num in numbers:
            if num in TIME_WINDOWS:
                hours.append(TIME_WINDOWS[num]["id"])
            else:
                return None  # Invalid number
        return hours if hours else None
    except:
        return None


def get_hours_names(hour_ids: list[str]) -> str:
    """Get Hebrew names for hour IDs."""
    if set(hour_ids) == {"morning", "afternoon", "evening", "night"}:
        return "תמיד"

    names = []
    for t in TIME_WINDOWS.values():
        if t["id"] in hour_ids:
            names.append(t["name"])
    return ", ".join(names) if names else "אין"


def process_message(phone: str, message: str) -> str:
    """Process incoming message and return response."""
    message = message.strip()
    user = get_user(phone)
    state = get_user_state(phone)

    # Command handling (case insensitive)
    msg_lower = message.lower()

    if msg_lower in ["עצור", "stop", "הפסק"]:
        delete_user(phone)
        set_user_state(phone, None)
        return STOPPED_MESSAGE

    if msg_lower in ["עזרה", "help", "?"]:
        return HELP_MESSAGE

    if msg_lower in ["סטטוס", "status", "מצב"]:
        if user:
            return STATUS_MESSAGE.format(
                regions=get_region_names(user["regions"]),
                level=get_level_name(user.get("level", "MODERATE")),
                hours=get_hours_names(user.get("hours", ["morning", "afternoon", "evening", "night"]))
            )
        else:
            return WELCOME_MESSAGE

    if msg_lower in ["אזורים", "regions", "שנה"]:
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
            user = get_user(phone)  # Refresh
            return UPDATED_HOURS_MESSAGE.format(
                regions=get_region_names(user["regions"]),
                level=get_level_name(user.get("level", "MODERATE")),
                hours=get_hours_names(hours)
            )
        else:
            return INVALID_HOURS_MESSAGE

    # State: selecting level (existing user)
    if state == "selecting_level":
        level = parse_level_input(message)
        if level:
            update_user_level(phone, level)
            set_user_state(phone, None)
            user = get_user(phone)  # Refresh
            return UPDATED_LEVEL_MESSAGE.format(
                regions=get_region_names(user["regions"]),
                level=get_level_name(level),
                hours=get_hours_names(user.get("hours", ["morning", "afternoon", "evening", "night"]))
            )
        else:
            return INVALID_LEVEL_MESSAGE

    # State: selecting regions
    if state == "selecting_regions":
        regions = parse_region_input(message)
        if regions:
            if user:
                # Existing user changing regions
                save_user(phone, regions, user.get("level", "MODERATE"), user.get("hours"))
                set_user_state(phone, None)
                return UPDATED_REGIONS_MESSAGE.format(
                    regions=get_region_names(regions),
                    level=get_level_name(user.get("level", "MODERATE")),
                    hours=get_hours_names(user.get("hours", ["morning", "afternoon", "evening", "night"]))
                )
            else:
                # New user - save regions temporarily and ask for level
                r = get_redis()
                r.hset("pending_regions", phone, json.dumps(regions))
                set_user_state(phone, "selecting_level_new")
                return LEVEL_MESSAGE
        else:
            return INVALID_INPUT_MESSAGE

    # State: new user selecting level (after regions)
    if state == "selecting_level_new":
        level = parse_level_input(message)
        if level:
            r = get_redis()
            r.hset("pending_level", phone, level)
            set_user_state(phone, "selecting_hours_new")
            return TIME_MESSAGE
        else:
            return INVALID_LEVEL_MESSAGE

    # State: new user selecting hours (after level)
    if state == "selecting_hours_new":
        hours = parse_hours_input(message)
        if hours:
            r = get_redis()
            regions_json = r.hget("pending_regions", phone)
            regions = json.loads(regions_json) if regions_json else []
            level = r.hget("pending_level", phone) or "MODERATE"
            r.hdel("pending_regions", phone)
            r.hdel("pending_level", phone)
            save_user(phone, regions, level, hours)
            set_user_state(phone, None)
            return REGISTERED_MESSAGE.format(
                regions=get_region_names(regions),
                level=get_level_name(level),
                hours=get_hours_names(hours)
            )
        else:
            return INVALID_HOURS_MESSAGE

    # New user - start flow
    if not user:
        set_user_state(phone, "selecting_regions")
        return WELCOME_MESSAGE

    # Default: show help
    return HELP_MESSAGE


# ============================================================================
# Twilio Response Formatting
# ============================================================================

def twiml_response(message: str) -> str:
    """Format response as TwiML."""
    # Escape XML special characters
    message = message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Message>{message}</Message>
</Response>"""


# ============================================================================
# DigitalOcean Functions Entry Point
# ============================================================================

def main(args: dict) -> dict:
    """Handle incoming Twilio webhook."""

    # Parse Twilio webhook payload
    # Twilio sends form-encoded data in __ow_body (base64) or as query params
    body = args.get("Body", "")
    from_number = args.get("From", "").replace("whatsapp:", "")

    # Handle base64 encoded body from DO Functions
    if "__ow_body" in args:
        import base64
        try:
            decoded = base64.b64decode(args["__ow_body"]).decode("utf-8")
            parsed = parse_qs(decoded)
            body = parsed.get("Body", [""])[0]
            from_number = parsed.get("From", [""])[0].replace("whatsapp:", "")
        except:
            pass

    if not from_number:
        return {
            "statusCode": 400,
            "body": "Missing From number"
        }

    # Process message and get response
    response_text = process_message(from_number, body)

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "text/xml"},
        "body": twiml_response(response_text)
    }


# ============================================================================
# Local Testing
# ============================================================================

if __name__ == "__main__":
    # Test locally
    test_phone = "+972501234567"

    print("Testing conversation flow...\n")

    # Simulate conversation: new user signup with level and hours selection
    messages = [
        "שלום",      # Start -> Welcome (select regions)
        "1,2",       # Select regions -> Ask for level
        "2",         # Select level (MODERATE) -> Ask for hours
        "1,2,3",     # Select hours (morning, afternoon, evening) -> Registered
        "סטטוס",     # Check status
        "רמה",       # Change level
        "3",         # Select new level (LOW)
        "שעות",      # Change hours
        "תמיד",      # Select all hours
        "אזורים",    # Change regions
        "3,4,5",     # Select new regions
        "עצור"       # Unsubscribe
    ]

    for msg in messages:
        print(f"User: {msg}")
        response = process_message(test_phone, msg)
        print(f"Bot: {response}\n")
        print("-" * 40)
