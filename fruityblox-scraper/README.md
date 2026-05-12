# FruityBlox Stock Monitor

Automated Blox Fruits stock monitoring system with Discord notifications.

## Features

✅ **Automatic Stock Monitoring**
- Fetches stock data from GitHub API every 4 hours
- Monitors both Normal and Mirage stock
- Detects rotation changes automatically

✅ **Discord Notifications**
- Rich embeds with fruit details (name, price, rarity, image)
- Grouped by rarity (Mythical, Legendary, Rare, Uncommon, Common)
- Configurable role mentions
- Test notification button

✅ **Web Dashboard**
- Real-time stock display with countdown timer
- Stock history with charts (7 days)
- Configuration page for Discord webhook
- Service monitoring and logs

✅ **Lightweight & Efficient**
- RAM usage: ~37MB (vs 300MB+ with browser automation)
- Uses public GitHub API (no scraping overhead)
- SQLite database for history tracking

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              FruityBlox Stock Monitor (API-based)            │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  API Poller  │───▶│   Database   │───▶│ Discord Bot  │  │
│  │  (requests)  │    │   (SQLite)   │    │  (Webhook)   │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                    │                    │          │
│    Every 4h              History              Rich Embed     │
│   (~37MB RAM)            Tracking             + Mentions     │
└─────────────────────────────────────────────────────────────┘
```

## Installation

Already installed and running! Service managed by Supervisor.

## Configuration

1. Access dashboard: https://panel.ldctesting.my.id/fruityblox/config
2. Set Discord Webhook URL (from Discord: Server Settings → Integrations → Webhooks)
3. (Optional) Add Role IDs for mentions
4. Enable/disable notifications for Normal/Mirage stock
5. Click "Test Notification" to verify setup

## API Endpoints

### Stock Data Source
- **GitHub API**: `https://raw.githubusercontent.com/iamishan877-max/Blox-Fruits-Stock/main/data/stock.json`
- **Fruit Metadata**: `https://blox-fruits-api.onrender.com/api/bloxfruits`

### Dashboard API
- `GET /fruityblox` - Stock monitor page
- `GET /fruityblox/history` - Stock history with charts
- `GET /fruityblox/config` - Configuration page
- `POST /fruityblox/config` - Update configuration
- `POST /api/fruityblox/test-notification` - Send test notification
- `GET /api/fruityblox/current-stock` - Get current stock (JSON)
- `GET /services/fruityblox` - Service detail page

## Database Schema

### Tables
- `fruityblox_stock` - Stock data with rotation tracking
- `fruityblox_rotations` - Rotation history
- `fruityblox_config` - Bot configuration
- `fruityblox_scrape_runs` - Scrape run logs

## Files & Directories

```
/opt/services/fruityblox-scraper/
├── scraper.py              # Main poller script
├── config.yaml             # Configuration
├── requirements.txt        # Python dependencies
├── logs/
│   ├── output.log         # Service logs
│   └── error.log          # Error logs
└── venv/                  # Virtual environment

/opt/services/dashboard/app/templates/
├── fruityblox.html        # Stock monitor page
├── fruityblox_history.html # History page
└── fruityblox_config.html  # Configuration page

/opt/services/shared/
└── app.db                 # SQLite database

/etc/supervisor/conf.d/
└── fruityblox-scraper.conf # Supervisor config
```

## Service Management

```bash
# Check status
supervisorctl status fruityblox-scraper

# Start/Stop/Restart
supervisorctl start fruityblox-scraper
supervisorctl stop fruityblox-scraper
supervisorctl restart fruityblox-scraper

# View logs
tail -f /opt/services/fruityblox-scraper/logs/output.log
tail -f /opt/services/fruityblox-scraper/logs/error.log
```

## Monitoring

- **Dashboard**: https://panel.ldctesting.my.id/fruityblox
- **Service Details**: https://panel.ldctesting.my.id/services/fruityblox
- **History**: https://panel.ldctesting.my.id/fruityblox/history

## Resource Usage

- **RAM**: ~37MB (idle), ~40MB (during poll)
- **Disk**: ~40MB (code + dependencies)
- **CPU**: Negligible (2-3 seconds every 4 hours)
- **Network**: ~7KB per poll (very light)

## Troubleshooting

### Service not running
```bash
supervisorctl restart fruityblox-scraper
tail -f /opt/services/fruityblox-scraper/logs/error.log
```

### Discord notifications not working
1. Check webhook URL in configuration
2. Test notification from config page
3. Verify role IDs are correct (if using mentions)
4. Check error logs for webhook failures

### Stock data not updating
1. Check if service is running: `supervisorctl status fruityblox-scraper`
2. Check logs: `tail -f /opt/services/fruityblox-scraper/logs/output.log`
3. Verify API is accessible: `curl https://raw.githubusercontent.com/iamishan877-max/Blox-Fruits-Stock/main/data/stock.json`

## Credits

- Stock data source: [iamishan877-max/Blox-Fruits-Stock](https://github.com/iamishan877-max/Blox-Fruits-Stock)
- Fruit metadata: [ProcapYT/blox-fruits-stock-api](https://github.com/ProcapYT/blox-fruits-stock-api)

## License

Part of the Service Manager Dashboard project.
