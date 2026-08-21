# 🍪 Snickerdoodle Checker v2.0

Automated Minecraft account checker with Telegram integration, Discord results delivery, and a modern web dashboard. Designed for one-command deployment on Linux.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌───────────┐     ┌─────────┐
│  Telegram    │────▶│  Downloader  │────▶│ Extractor  │────▶│ Checker │
│  Monitor     │     │  (Queue)     │     │ ZIP/RAR/7Z │     │         │
└─────────────┘     └──────────────┘     └───────────┘     └────┬────┘
                                                                 │
┌─────────────┐     ┌──────────────┐     ┌───────────┐          │
│  Cleanup    │◀────│   Discord    │◀────│  Results   │◀────────┘
│  Temp Files │     │   Webhook    │     │  Collector │
└─────────────┘     └──────────────┘     └───────────┘
```

**Tech Stack:** Python 3.10+ · FastAPI · SQLite · Telethon · Alpine.js · Tailwind CSS

## Quick Start

```bash
# 1. Clone the repository
git clone <repository-url>
cd audo_check

# 2. Make the startup script executable
chmod +x start.sh

# 3. Run (handles everything automatically)
./start.sh
```

That's it. The script will:
- Detect your Linux distribution
- Install missing dependencies (Python, pip, unrar, 7z)
- Create a virtual environment
- Install Python packages
- Create configuration files
- Initialize the database
- Install cloudflared (if tunnel token is set)
- Start the web server

## Configuration

### First-Time Setup

1. Copy the example config: `cp .env.example .env`
2. Edit `.env` with your settings
3. Run `./start.sh`

Or just run `./start.sh` — it creates `.env` from the example automatically.

### Environment Variables

| Variable | Description | Default |
|---|---|---|
| `PORT` | Web server port | `7777` |
| `SECRET_KEY` | Session signing key | Auto-generated |
| `TELEGRAM_API_ID` | Telegram API ID | — |
| `TELEGRAM_API_HASH` | Telegram API hash | — |
| `TELEGRAM_PHONE` | Telegram phone number | — |
| `TELEGRAM_CHANNEL_IDS` | Comma-separated channel IDs | — |
| `DISCORD_WEBHOOK_URL` | Discord webhook URL | — |
| `DISCORD_MAX_FILE_SIZE_MB` | Max Discord upload size | `25` |
| `CLOUDFLARE_DOMAIN` | Public domain | `checker.ravealts.com` |
| `CLOUDFLARE_TUNNEL_TOKEN` | Cloudflare tunnel token | — |
| `CHECKER_COMMAND` | External checker command | Built-in scanner |
| `CHECKER_WORKERS` | Worker thread count | `20` |
| `MAX_RETRIES` | Job retry attempts | `3` |
| `LOG_LEVEL` | Logging level | `INFO` |

### Telegram Setup

1. Go to [my.telegram.org/apps](https://my.telegram.org/apps)
2. Create an application
3. Copy the **API ID** and **API Hash**
4. Set them in `.env`:
   ```
   TELEGRAM_API_ID=12345678
   TELEGRAM_API_HASH=abc123def456...
   TELEGRAM_PHONE=+1234567890
   TELEGRAM_CHANNEL_IDS=-1001234567890,-1009876543210
   ```
5. On first run, enter the verification code in the web UI (Settings → Telegram)

### Discord Setup

1. Go to your Discord server → Settings → Integrations → Webhooks
2. Create a webhook in your results channel
3. Copy the webhook URL
4. Set it in `.env`:
   ```
   DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
   ```

### Cloudflare Tunnel Setup

1. Go to [Cloudflare Zero Trust Dashboard](https://one.dash.cloudflare.com/)
2. Create a tunnel
3. Configure the tunnel to point to `http://localhost:7777`
4. Copy the tunnel token
5. Set it in `.env`:
   ```
   CLOUDFLARE_DOMAIN=checker.ravealts.com
   CLOUDFLARE_TUNNEL_TOKEN=eyJ...
   ```

The application will automatically install `cloudflared` and start the tunnel.

### Checker Command

By default, the built-in scanner finds cookie files in extracted archives. For full Minecraft authentication checking, configure an external checker:

```
CHECKER_COMMAND=/path/to/checker --input {input} --output {output}
```

Use `{input}` and `{output}` as placeholders for the extracted directory and results directory.

## First Login

- **URL:** `http://your-ip:7777` or `https://checker.ravealts.com`
- **Password:** `ship2026`
- **⚠️ Change the password immediately** via the web UI

## User Management

### Permission Levels

| Role | Dashboard | Queue Control | Worker Control | Settings | Users |
|---|---|---|---|---|---|
| **Admin** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Operator** | ✅ | ✅ | ✅ (limited) | ❌ | ❌ |
| **Viewer** | ✅ | View only | ❌ | ❌ | ❌ |

### Creating Users (Admin only)

1. Go to **Users** page
2. Click **+ Add User**
3. Set username, password, and role
4. Click **Create**

## Processing Pipeline

```
1. Telegram file detected
2. Added to queue
3. Download ONE file
4. Verify download complete
5. Extract archive (ZIP/RAR/7Z/TAR)
6. Run checker on extracted files
7. Collect results
8. Send results to Discord
9. Split large files if > 50MB
10. Delete temporary files
11. Mark job complete
12. Process next file
```

**Only one file is processed at a time.** The next file does not start until the current one is fully complete.

## Web Dashboard

- **Dashboard** — Live system status, current job, resource usage
- **Queue** — Job queue with pause/resume/retry/cancel controls
- **Statistics** — Processing history, daily stats, totals
- **Logs** — Live log viewer with filtering and search
- **System** — Worker health, restart buttons, emergency reset
- **Settings** — Telegram, Discord, Cloudflare, checker configuration
- **Users** — User management (admin only)

## Starting & Stopping

```bash
# Start
./start.sh

# Stop
Ctrl+C

# Run in background
nohup ./start.sh > /dev/null 2>&1 &

# Run with systemd (recommended)
sudo cp scripts/snickerdoodle.service /etc/systemd/system/
sudo systemctl enable snickerdoodle
sudo systemctl start snickerdoodle
```

## Troubleshooting

### Application won't start
- Check Python version: `python3 --version` (needs 3.10+)
- Check `.env` file exists and is valid
- Check port isn't in use: `lsof -i :7777`
- Check logs: `tail -f logs/audo_check.log`

### Telegram won't connect
- Verify API ID and hash are correct
- Check phone number format (include country code)
- Try disconnecting and reconnecting in Settings
- Delete `data/telegram_session*` and re-authenticate

### Discord messages fail
- Verify webhook URL is correct
- Check Discord server permissions
- Test webhook in Settings → Discord → Test

### Cloudflare Tunnel disconnects
- Check tunnel token is valid
- Verify `cloudflared` is installed: `which cloudflared`
- Check Cloudflare dashboard for tunnel status
- Use System → Restart Cloudflare button

### Jobs stuck
- Use System → Emergency Reset
- Check logs for error details
- Restart individual workers from System page

## Reset Controls

Each subsystem has a dedicated restart button on the System page:

| Button | Effect |
|---|---|
| Restart Telegram | Reconnects Telegram only |
| Restart Queue | Restarts the download/processing queue |
| Restart Checker | Stops the current check (if running) |
| Restart Discord | Tests the Discord webhook connection |
| Restart Cloudflare | Restarts the Cloudflare tunnel |
| **Emergency Reset** | Stops everything, cleans temp files, resets stuck jobs, restarts |

## Database

- **Location:** `data/audo_check.db` (SQLite)
- **Backup:** `cp data/audo_check.db data/backup_$(date +%Y%m%d).db`
- **Restore:** Stop the app, replace the DB file, restart

## Project Structure

```
audo_check/
├── start.sh              # One-command startup
├── .env                   # Configuration (not in git)
├── .env.example           # Configuration template
├── requirements.txt       # Python dependencies
├── README.md
│
├── backend/
│   ├── main.py            # FastAPI application
│   ├── config.py          # Configuration loader
│   ├── database.py        # Database setup
│   ├── models.py          # SQLAlchemy models
│   ├── websocket_hub.py   # WebSocket broadcasting
│   ├── logging_config.py  # Logging system
│   ├── api/               # REST API routes
│   ├── auth/              # Authentication
│   ├── services/          # Telegram, Discord, Cloudflare, Checker
│   └── workers/           # Queue manager, extractor, watchdog
│
├── frontend/
│   ├── templates/         # Jinja2 HTML templates
│   └── static/            # CSS, JavaScript
│
├── data/                  # Database files
├── downloads/             # Downloaded archives
├── temp/                  # Temporary extraction
├── results/               # Checker results
├── logs/                  # Log files
└── config/                # Additional config files
```

## Updating

```bash
cd audo_check
git pull
./start.sh  # Automatically updates dependencies
```

## License

Private — not for redistribution.
#   C h e c k e r _ v 2  
 