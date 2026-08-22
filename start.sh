#!/usr/bin/env bash
# ============================================================================
#  Snickerdoodle Checker v2.0 — One-Command Startup
# ============================================================================
set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Helpers ─────────────────────────────────────────────────────────────────

log_step() {
    echo -e "\n${BLUE}[$1]${NC} $2"
}

log_ok() {
    echo -e "  ${GREEN}✓${NC} $1"
}

log_warn() {
    echo -e "  ${YELLOW}⚠${NC} $1"
}

log_fail() {
    echo -e "  ${RED}✗${NC} $1"
}

check_command() {
    command -v "$1" &>/dev/null
}

# ── Step 1: Check Linux ─────────────────────────────────────────────────────

log_step "1/8" "Checking Linux..."
if [[ "$(uname -s)" != "Linux" ]]; then
    log_fail "This application is designed for Linux."
    log_fail "Current OS: $(uname -s)"
    exit 1
fi
log_ok "Linux detected: $(uname -sr)"

# ── Step 2: Check Dependencies ──────────────────────────────────────────────

log_step "2/8" "Checking dependencies..."

MISSING=()

if ! check_command python3; then
    MISSING+=("python3")
fi

if ! check_command pip3 && ! python3 -m pip --version &>/dev/null; then
    MISSING+=("python3-pip")
fi

# Optional dependencies
if ! check_command unrar && ! check_command unrar-free; then
    log_warn "unrar not found — RAR extraction may not work"
fi

if ! check_command 7z && ! check_command 7za; then
    log_warn "7z not found — 7Z extraction may not work"
fi

# Check Python version
if check_command python3; then
    PY_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "0.0")
    PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
    PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)
    if [[ "$PY_MAJOR" -lt 3 ]] || [[ "$PY_MAJOR" -eq 3 && "$PY_MINOR" -lt 10 ]]; then
        log_fail "Python 3.10+ required, found $PY_VERSION"
        MISSING+=("python3.10+")
    else
        log_ok "Python $PY_VERSION"
    fi
fi

# ── Step 3: Install Missing Dependencies ─────────────────────────────────────

log_step "3/8" "Installing missing dependencies..."

if [[ ${#MISSING[@]} -gt 0 ]]; then
    log_warn "Missing: ${MISSING[*]}"

    # Detect package manager
    if check_command apt-get; then
        PKG_MGR="apt"
        log_warn "Installing via apt..."
        sudo apt-get update -qq
        for pkg in "${MISSING[@]}"; do
            case "$pkg" in
                python3)       sudo apt-get install -y -qq python3 python3-venv ;;
                python3-pip)   sudo apt-get install -y -qq python3-pip python3-venv ;;
                python3.10+)   sudo apt-get install -y -qq python3 python3-venv ;;
            esac
        done
        # Install optional deps
        sudo apt-get install -y -qq unrar-free p7zip-full 2>/dev/null || true
    elif check_command dnf; then
        PKG_MGR="dnf"
        for pkg in "${MISSING[@]}"; do
            case "$pkg" in
                python3|python3-pip|python3.10+)
                    sudo dnf install -y python3 python3-pip ;;
            esac
        done
    elif check_command yum; then
        PKG_MGR="yum"
        for pkg in "${MISSING[@]}"; do
            case "$pkg" in
                python3|python3-pip|python3.10+)
                    sudo yum install -y python3 python3-pip 2>/dev/null || true ;;
            esac
        done
        sudo yum install -y unrar p7zip 2>/dev/null || true
    elif check_command pacman; then
        PKG_MGR="pacman"
        for pkg in "${MISSING[@]}"; do
            case "$pkg" in
                python3|python3-pip|python3.10+)
                    sudo pacman -S --noconfirm python python-pip ;;
            esac
        done
    else
        log_fail "No supported package manager found (apt, dnf, yum, pacman)"
        log_fail "Please install manually: ${MISSING[*]}"
        exit 1
    fi
    log_ok "Dependencies installed"
else
    log_ok "All dependencies present"
fi

# ── Step 4: Check Configuration ──────────────────────────────────────────────

log_step "4/8" "Checking configuration..."

if [[ ! -f ".env" ]]; then
    log_warn ".env file not found — creating from example..."
    cp .env.example .env
    log_ok ".env created — edit it with your Telegram/Discord/Cloudflare settings"
else
    log_ok ".env file found"
fi

# Create required directories
for dir in downloads temp results logs data config; do
    mkdir -p "$dir"
done
log_ok "Directories ready"

# ── Step 5: Set Up Python Environment ────────────────────────────────────────

log_step "5/8" "Setting up Python environment..."

if [[ ! -d "venv" ]]; then
    python3 -m venv venv
    log_ok "Virtual environment created"
else
    log_ok "Virtual environment exists"
fi

source venv/bin/activate

# Install/upgrade pip
pip install --upgrade pip -q 2>/dev/null

# Install requirements
pip install -r requirements.txt -q 2>&1 | tail -1
log_ok "Python packages installed"

# ── Step 6: Initialize Database ──────────────────────────────────────────────

log_step "6/8" "Starting database..."
log_ok "SQLite database will be initialized on startup"

# ── Step 7: Cloudflare Tunnel ────────────────────────────────────────────────

log_step "7/8" "Checking Cloudflare Tunnel..."

CF_TOKEN=$(grep -oP '^CLOUDFLARE_TUNNEL_TOKEN=\K.+' .env 2>/dev/null | tr -d '"' | tr -d "'")

if [[ -n "$CF_TOKEN" && "$CF_TOKEN" != "" ]]; then
    if ! check_command cloudflared; then
        log_warn "cloudflared not installed — installing..."

        # Detect architecture
        ARCH=$(uname -m)
        case "$ARCH" in
            x86_64)  CF_ARCH="amd64" ;;
            aarch64) CF_ARCH="arm64" ;;
            armv7l)  CF_ARCH="arm" ;;
            *)       CF_ARCH="amd64" ;;
        esac

        # Download cloudflared
        CF_URL="https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${CF_ARCH}"
        if curl -sSL "$CF_URL" -o /usr/local/bin/cloudflared 2>/dev/null; then
            chmod +x /usr/local/bin/cloudflared
            log_ok "cloudflared installed"
        elif sudo curl -sSL "$CF_URL" -o /usr/local/bin/cloudflared 2>/dev/null; then
            sudo chmod +x /usr/local/bin/cloudflared
            log_ok "cloudflared installed (via sudo)"
        else
            log_warn "Failed to install cloudflared — tunnel will be disabled"
        fi
    else
        log_ok "cloudflared found: $(cloudflared --version 2>/dev/null | head -1)"
    fi
else
    log_warn "Cloudflare Tunnel token not configured — tunnel disabled"
fi

# ── Step 8: Start Application ────────────────────────────────────────────────

log_step "8/8" "Starting web server..."

# Read port from .env
PORT=$(grep -oP '^PORT=\K\d+' .env 2>/dev/null || echo "7777")

# Read domain from .env
DOMAIN=$(grep -oP '^CLOUDFLARE_DOMAIN=\K.+' .env 2>/dev/null | tr -d '"' | tr -d "'" || echo "")

echo ""
echo -e "${BOLD}================================${NC}"
echo -e "${BOLD} 🍪 Snickerdoodle Checker v2.0${NC}"
echo -e "${BOLD}================================${NC}"
echo ""

if [[ -n "$DOMAIN" && -n "$CF_TOKEN" ]]; then
    echo -e "  ${CYAN}Web:${NC}   https://${DOMAIN}"
fi
echo -e "  ${CYAN}Local:${NC} http://127.0.0.1:${PORT}"
echo ""
echo -e "  ${CYAN}Login:${NC} password is ${BOLD}ship2026${NC} (change after first login)"
echo ""
echo -e "  ${CYAN}Status:${NC} ${GREEN}RUNNING${NC}"
echo ""
echo -e "  Press ${BOLD}Ctrl+C${NC} to stop."
echo ""

# Run the application
exec python3 -m uvicorn backend.main:app \
    --host 0.0.0.0 \
    --port "$PORT" \
    --log-level warning \
    --no-access-log
