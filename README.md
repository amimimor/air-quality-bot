# Israel Air Quality Alert Bot

WhatsApp bot that sends air quality alerts to subscribers based on their preferences.

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────┐
│  Twilio         │────▶│  webhook.py      │────▶│  Redis      │
│  (WhatsApp)     │◀────│  (DO Function)   │◀────│  (Valkey)   │
└─────────────────┘     └──────────────────┘     └─────────────┘
                                                        │
┌─────────────────┐     ┌──────────────────┐           │
│  Air Quality    │────▶│  main.py         │───────────┘
│  API            │     │  (DO Function)   │
└─────────────────┘     │  [Scheduled]     │
                        └──────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `main.py` | Alert checker - fetches air quality, sends alerts to subscribers |
| `webhook.py` | Twilio webhook - handles user registration & preferences |
| `project.yml` | DigitalOcean Functions deployment config |
| `requirements.txt` | Python dependencies |
| `.env` | Environment variables (not committed) |

## User Preferences

Users can configure:

| Setting | Options | Storage |
|---------|---------|---------|
| **Regions** | tel_aviv, center, jerusalem, haifa, south, sharon, north | `region:{id}` sets + `users` hash |
| **Alert Level** | GOOD, MODERATE, LOW, VERY_LOW | `users` hash |
| **Hours** | morning (06-12), afternoon (12-18), evening (18-22), night (22-06) | `users` hash |

## Redis Data Structure

```
# User data (hash)
users: {
  "+972501234567": {
    "phone": "+972501234567",
    "regions": ["tel_aviv", "center"],
    "level": "MODERATE",
    "hours": ["morning", "afternoon", "evening"]
  }
}

# Region index (sets) - for efficient lookups
region:tel_aviv: ["+972501234567", "+972509876543"]
region:center: ["+972501234567"]

# Conversation state (hash)
user_states: {
  "+972501234567": "selecting_regions"
}

# Temporary storage during registration
pending_regions: {"+972501234567": "[\"tel_aviv\", \"center\"]"}
pending_level: {"+972501234567": "MODERATE"}
```

## Alert Levels

| Level | Hebrew | AQI Threshold | Description |
|-------|--------|---------------|-------------|
| GOOD | טוב | < 51 | Alert when drops from good |
| MODERATE | בינוני | < 0 | Alert when drops from moderate |
| LOW | לא בריא | < -200 | Alert only when unhealthy |
| VERY_LOW | מסוכן | < -400 | Alert only in dangerous conditions |

## Bot Commands (Hebrew)

| Command | Action |
|---------|--------|
| `אזורים` | Change monitored regions |
| `רמה` | Change alert threshold |
| `שעות` | Change alert hours |
| `סטטוס` | View current settings |
| `עצור` | Unsubscribe |
| `עזרה` | Show help |

## Time Windows

| Window | Hebrew | Hours |
|--------|--------|-------|
| morning | בוקר | 06:00-12:00 |
| afternoon | צהריים | 12:00-18:00 |
| evening | ערב | 18:00-22:00 |
| night | לילה | 22:00-06:00 |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TWILIO_ACCOUNT_SID` | Yes | Twilio account SID |
| `TWILIO_AUTH_TOKEN` | Yes | Twilio auth token |
| `REDIS_URL` | Yes | Redis/Valkey connection URL |
| `TWILIO_WHATSAPP_FROM` | No | WhatsApp sender number |
| `LANGUAGE` | No | Default: `he` |

## Deployment

### Prerequisites

- DigitalOcean account with `doctl` configured
- Twilio account with WhatsApp sandbox enabled

### Deploy to DigitalOcean Functions

```bash
# 1. Deploy functions
doctl serverless deploy . \
  --env TWILIO_ACCOUNT_SID=your_sid \
  --env TWILIO_AUTH_TOKEN=your_token \
  --env REDIS_URL="rediss://..."

# 2. Get webhook URL
doctl serverless functions get airquality/webhook --url

# 3. Configure Twilio webhook
# In Twilio Console → Messaging → WhatsApp Sandbox
# Set "When a message comes in" to your webhook URL
```

### Schedule Alert Checks

Use DigitalOcean Functions triggers or external cron to call:
```
doctl serverless functions invoke airquality/check-alerts
```

## Local Development

```bash
# Install dependencies
uv pip install -r requirements.txt

# Set environment variables
cp .env.example .env
# Edit .env with your credentials

# Test webhook conversation
uv run python webhook.py

# Test alert checker
uv run python main.py
```

## Conversation Flow

```
User: שלום
Bot: שלום! 👋ברוכים הבאים לבוט התראות איכות האוויר.
     באילו אזורים תרצו לקבל התראות?
     1️⃣ תל אביב  2️⃣ מרכז  3️⃣ ירושלים  4️⃣ חיפה
     5️⃣ דרום  6️⃣ שרון  7️⃣ צפון

User: 1,2
Bot: 🎚️ באיזה מצב לשלוח התראה?
     1️⃣ טוב  2️⃣ בינוני (מומלץ)  3️⃣ לא בריא  4️⃣ מסוכן

User: 2
Bot: 🕐 מתי לשלוח התראות?
     1️⃣ בוקר (06:00-12:00)  2️⃣ צהריים (12:00-18:00)
     3️⃣ ערב (18:00-22:00)  4️⃣ לילה (22:00-06:00)

User: 1,2,3
Bot: ✅ נרשמתם בהצלחה!
     🗺️ אזורים: תל אביב, מרכז
     🎚️ סף התראה: בינוני
     🕐 שעות: בוקר, צהריים, ערב
```
