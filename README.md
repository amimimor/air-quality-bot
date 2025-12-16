# Israel Air Quality Alert Bot

Multi-platform bot (WhatsApp + Telegram) that sends air quality alerts to subscribers based on their preferences.

## Features

- **Multi-platform**: WhatsApp (via Twilio) and Telegram support
- **Real-time data**: Fetches from Israel Ministry of Environmental Protection API
- **Customizable alerts**: Region/city selection, alert thresholds, time windows
- **Hebrew interface**: Full Hebrew conversational flow
- **Pollutant monitoring**: PM2.5, PM10, O3, NO2, Benzene
- **Anti-spam**: 2-hour cooldown between alerts for same station

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────┐
│  Twilio         │────▶│  webhook         │────▶│  Redis      │
│  (WhatsApp)     │◀────│  (DO Function)   │◀────│  (Valkey)   │
└─────────────────┘     └──────────────────┘     └─────────────┘
                                                        │
┌─────────────────┐     ┌──────────────────┐           │
│  Telegram       │────▶│  telegram-webhook│───────────┤
│  Bot API        │◀────│  (DO Function)   │           │
└─────────────────┘     └──────────────────┘           │
                                                        │
┌─────────────────┐     ┌──────────────────┐           │
│  Air Quality    │────▶│  check-alerts    │───────────┘
│  API            │     │  (DO Function)   │
└─────────────────┘     │  [Every 10 min]  │
                        └──────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `packages/airquality/webhook/` | WhatsApp webhook - handles Twilio messages |
| `packages/airquality/telegram-webhook/` | Telegram webhook - handles Telegram messages |
| `packages/airquality/check-alerts/` | Alert checker - fetches air quality, sends alerts |
| `project.yml` | DigitalOcean Functions deployment config |
| `.env.example` | Environment variables template |

## Pollutants Monitored

| Pollutant | Unit | Good | Moderate | Unhealthy |
|-----------|------|------|----------|-----------|
| PM2.5 | µg/m³ | ≤12 | 12-35 | >35 |
| PM10 | µg/m³ | ≤50 | 50-100 | >100 |
| O3 | ppb | ≤60 | 60-80 | >80 |
| NO2 | ppb | ≤53 | 53-100 | >100 |
| **Benzene** | µg/m³ | ≤1 | 1-5 | >5 (EU limit) |

## Alert Levels

| Level | Hebrew | AQI Threshold | Description |
|-------|--------|---------------|-------------|
| GOOD | טוב | < 51 | Alert when drops from good (most sensitive) |
| MODERATE | בינוני | < 0 | Alert when drops from moderate (recommended) |
| LOW | לא בריא | < -200 | Alert only when unhealthy |
| VERY_LOW | מסוכן | < -400 | Alert only in dangerous conditions |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TWILIO_ACCOUNT_SID` | For WhatsApp | Twilio account SID |
| `TWILIO_AUTH_TOKEN` | For WhatsApp | Twilio auth token |
| `TELEGRAM_BOT_TOKEN` | For Telegram | Telegram bot token from @BotFather |
| `REDIS_URL` | Yes | Redis/Valkey connection URL |
| `LANGUAGE` | No | Default: `he` |

## Quick Start

### 1. Clone and Configure

```bash
git clone https://github.com/amimimor/air-quality-bot.git
cd air-quality-bot/packages/airquality/alert

# Copy and edit environment variables
cp .env.example .env
# Edit .env with your credentials
```

### 2. Deploy to DigitalOcean Functions

```bash
source .env
export TWILIO_ACCOUNT_SID TWILIO_AUTH_TOKEN TELEGRAM_BOT_TOKEN REDIS_URL
doctl serverless deploy .
```

### 3. Configure Telegram Bot

```bash
# Get webhook URL
WEBHOOK_URL=$(doctl sls fn get airquality/telegram-webhook --url)

# Register with Telegram
curl "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/setWebhook?url=$WEBHOOK_URL"
```

### 4. Configure WhatsApp (Twilio)

1. Get webhook URL: `doctl sls fn get airquality/webhook --url`
2. In Twilio Console → Messaging → WhatsApp Sandbox
3. Set "When a message comes in" to your webhook URL

## Telegram Commands

| Command | Action |
|---------|--------|
| `/start` | Start registration or show status |
| `/status` | View current settings |
| `/change` | Change all settings |
| `/regions` | Change monitored regions/cities |
| `/level` | Change alert threshold |
| `/hours` | Change alert hours |
| `/stop` | Unsubscribe |
| `/help` | Show help |

## WhatsApp Commands (Hebrew)

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

## Redis Data Structure

```
# WhatsApp users (hash)
users: {
  "+972501234567": {"phone": "...", "regions": [...], "level": "MODERATE", "hours": [...]}
}

# WhatsApp region index (sets)
region:tel_aviv: ["+972501234567", ...]
station:339: ["+972501234567", ...]

# Telegram users (individual keys)
telegram:user:{chat_id}: {"chat_id": "...", "regions": [...], "stations": [...], "level": "...", "hours": [...]}
telegram:users: {chat_id1, chat_id2, ...}

# Anti-spam tracking
last_alert:{phone}: {station_id: timestamp}
telegram:last_alert:{chat_id}: {station_id: timestamp}
```

## Branches

| Branch | Description |
|--------|-------------|
| `main` | Full version with WhatsApp + Telegram + Benzene |
| `whatsapp` | WhatsApp-only version |
| `telegram-bot` | Development branch for Telegram features |

## Cost Comparison

| Users | WhatsApp (Twilio) | Telegram | Savings |
|------:|------------------:|---------:|--------:|
| 100 | ~$65/mo | ~$15/mo | 77% |
| 1,000 | ~$520/mo | ~$20/mo | 96% |
| 10,000 | ~$5,000/mo | ~$50/mo | 99% |

*Telegram is free for messaging. Costs are for infrastructure (Redis + Functions) only.*

## License

MIT
