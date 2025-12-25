# Israel Air Quality Alert Bot

A Telegram bot that sends real-time air quality alerts based on data from Israeli monitoring stations.

## Features

- Real-time alerts when air quality deteriorates
- Covers 230+ monitoring stations across Israel
- Tracks AQI (Air Quality Index) and Benzene levels
- Customizable alert sensitivity and quiet hours
- "All clear" notifications when air quality improves
- Hebrew interface

## Data Source

Air quality data is fetched from the **Israeli Ministry of Environmental Protection** API:
- Endpoint: `https://air-api.sviva.gov.il/v1/envista/stations`
- Updates every 10 minutes from monitoring stations
- Pollutants tracked: PM2.5, PM10, O3, NO2, SO2, CO, NOX, Benzene

## How It Works

### Israeli AQI Scale

The bot uses the official Israeli AQI formula where **100 = best air quality** and values decrease (even going negative) as air quality worsens:

| AQI Range | Quality | Alert Level |
|-----------|---------|-------------|
| > 50 | Good | GOOD |
| 0 to 50 | Moderate | MODERATE |
| -100 to 0 | Unhealthy for sensitive groups | LOW |
| < -100 | Unhealthy | VERY_LOW |

### Benzene Monitoring

Benzene is a known carcinogen with no safe threshold (WHO). The bot alerts at these levels:

| Benzene (ppb) | Level |
|---------------|-------|
| < 1.0 | No alert |
| 1.0 - 1.55 | Elevated |
| 1.55 - 2.10 | High |
| 2.10 - 2.64 | Very High |
| > 2.64 | Dangerous |

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────┐
│  Cron Trigger   │────>│  check-alerts    │────>│  Telegram   │
│  (every 10 min) │     │  function        │     │  Bot API    │
└─────────────────┘     └────────┬─────────┘     └─────────────┘
                                 │
                                 v
                        ┌──────────────────┐
                        │  Redis/Valkey    │
                        │  (user prefs)    │
                        └──────────────────┘
                                 ^
                                 │
┌─────────────────┐     ┌────────┴─────────┐
│  Telegram User  │────>│ telegram-webhook │
│  Commands       │     │  function        │
└─────────────────┘     └──────────────────┘
```

## Deployment

Deployed on DigitalOcean Functions.

### Prerequisites

- DigitalOcean account with Functions enabled
- Telegram Bot Token (from @BotFather)
- Redis/Valkey database for user storage

### Environment Variables

Set these in DigitalOcean Functions settings:

| Variable | Description |
|----------|-------------|
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather |
| `REDIS_URL` | Redis connection URL |
| `TZ` | Timezone (default: `Asia/Jerusalem`) |
| `LANGUAGE` | Interface language (default: `he`) |

### Deploy

```bash
# Install doctl and connect to serverless
doctl serverless install
doctl serverless connect

# Deploy
doctl serverless deploy .
```

### Set Telegram Webhook

```bash
# Get your function URL
doctl sls fn get airquality/telegram-webhook --url

# Set webhook (replace URL)
curl "https://api.telegram.org/bot<TOKEN>/setWebhook?url=<FUNCTION_URL>"
```

## Bot Commands

| Command | Description |
|---------|-------------|
| `/start` | Start registration |
| `/status` | View current settings |
| `/level` | Change alert sensitivity |
| `/hours` | Set quiet hours |
| `/now` | Get current air quality |
| `/thresholds` | View alert thresholds |
| `/stop` | Unsubscribe |

## Configuration

Alert thresholds and AQI breakpoints are defined in `packages/airquality/check-alerts/aqi_config.yaml`.

## Testing

```bash
cd packages/airquality/check-alerts
python -m pytest test_aqi.py -v
```

## License

MIT
