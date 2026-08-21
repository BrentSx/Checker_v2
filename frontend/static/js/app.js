/**
 * Snickerdoodle Checker v2 — Frontend Application
 */

// ── WebSocket Manager ──────────────────────────────────────────────────────

class WSManager {
    constructor() {
        this.ws = null;
        this.handlers = {};
        this.reconnectDelay = 1000;
        this.maxReconnectDelay = 30000;
        this.connected = false;
    }

    connect() {
        const token = getCookie('_ac_session');
        const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
        const url = `${protocol}//${location.host}/ws?token=${encodeURIComponent(token)}`;

        try {
            this.ws = new WebSocket(url);
        } catch (e) {
            console.error('WebSocket creation failed:', e);
            this.scheduleReconnect();
            return;
        }

        this.ws.onopen = () => {
            this.connected = true;
            this.reconnectDelay = 1000;
            console.log('WebSocket connected');
            this.emit('ws_connected', {});
        };

        this.ws.onmessage = (event) => {
            try {
                const msg = JSON.parse(event.data);
                this.emit(msg.type, msg.data);
            } catch (e) {
                console.error('WS message parse error:', e);
            }
        };

        this.ws.onclose = () => {
            this.connected = false;
            console.log('WebSocket disconnected');
            this.emit('ws_disconnected', {});
            this.scheduleReconnect();
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };
    }

    scheduleReconnect() {
        setTimeout(() => {
            this.reconnectDelay = Math.min(this.reconnectDelay * 1.5, this.maxReconnectDelay);
            this.connect();
        }, this.reconnectDelay);
    }

    on(type, handler) {
        if (!this.handlers[type]) this.handlers[type] = [];
        this.handlers[type].push(handler);
    }

    off(type, handler) {
        if (this.handlers[type]) {
            this.handlers[type] = this.handlers[type].filter(h => h !== handler);
        }
    }

    emit(type, data) {
        if (this.handlers[type]) {
            this.handlers[type].forEach(h => h(data));
        }
    }

    send(type, data) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({ type, data }));
        }
    }
}

const ws = new WSManager();

// ── API Helper ─────────────────────────────────────────────────────────────

async function api(path, options = {}) {
    const defaults = {
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
    };
    const opts = { ...defaults, ...options };

    const resp = await fetch(path, opts);

    if (resp.status === 401) {
        window.location.href = '/login';
        throw new Error('Unauthorized');
    }

    if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || `HTTP ${resp.status}`);
    }

    const ct = resp.headers.get('content-type') || '';
    if (ct.includes('application/json')) {
        return resp.json();
    }
    return resp.text();
}

// ── Toast Notifications ────────────────────────────────────────────────────

function showToast(message, type = 'info', duration = 4000) {
    const container = document.getElementById('toasts');
    if (!container) return;

    const icons = {
        success: '✓',
        error: '✗',
        warning: '⚠',
        info: 'ℹ',
    };

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `
        <span class="text-lg">${icons[type] || 'ℹ'}</span>
        <span class="flex-1">${escapeHtml(message)}</span>
        <button onclick="this.parentElement.remove()" class="text-gray-500 hover:text-white ml-2">&times;</button>
    `;

    container.appendChild(toast);

    if (duration > 0) {
        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateX(100%)';
            toast.style.transition = 'all 0.3s';
            setTimeout(() => toast.remove(), 300);
        }, duration);
    }
}

// ── Utility Functions ──────────────────────────────────────────────────────

function getCookie(name) {
    const match = document.cookie.match(new RegExp('(^| )' + name + '=([^;]+)'));
    return match ? decodeURIComponent(match[2]) : '';
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

function formatBytes(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatDuration(seconds) {
    if (!seconds || seconds < 0) return '—';
    if (seconds < 60) return `${Math.round(seconds)}s`;
    const m = Math.floor(seconds / 60);
    const s = Math.round(seconds % 60);
    if (m < 60) return `${m}m ${s}s`;
    const h = Math.floor(m / 60);
    return `${h}h ${m % 60}m`;
}

function formatNumber(n) {
    if (n == null) return '0';
    return n.toLocaleString();
}

function timeAgo(isoString) {
    if (!isoString) return '—';
    const d = new Date(isoString);
    const now = new Date();
    const diff = (now - d) / 1000;
    if (diff < 60) return 'just now';
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    return `${Math.floor(diff / 86400)}d ago`;
}

function statusColor(status) {
    const map = {
        queued: 'gray', waiting: 'gray',
        downloading: 'blue', downloaded: 'blue',
        extracting: 'blue', checking: 'blue',
        processing_results: 'blue', sending_discord: 'blue',
        cleaning_up: 'blue',
        completed: 'green',
        failed: 'red',
        cancelled: 'gray',
        paused: 'yellow',
    };
    return map[status] || 'gray';
}

function statusBadge(status) {
    return `<span class="badge badge-${statusColor(status)}">${status.toUpperCase().replace('_', ' ')}</span>`;
}

// ── Confirm Dialog ─────────────────────────────────────────────────────────

function confirmAction(title, message) {
    return new Promise((resolve) => {
        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay';
        overlay.innerHTML = `
            <div class="modal">
                <h3 class="text-lg font-bold text-white mb-2">${escapeHtml(title)}</h3>
                <p class="text-sm text-gray-400 mb-6">${escapeHtml(message)}</p>
                <div class="flex gap-3 justify-end">
                    <button class="btn btn-ghost cancel-btn">Cancel</button>
                    <button class="btn btn-danger confirm-btn">Confirm</button>
                </div>
            </div>
        `;
        document.body.appendChild(overlay);
        overlay.querySelector('.cancel-btn').onclick = () => { overlay.remove(); resolve(false); };
        overlay.querySelector('.confirm-btn').onclick = () => { overlay.remove(); resolve(true); };
        overlay.onclick = (e) => { if (e.target === overlay) { overlay.remove(); resolve(false); } };
    });
}

// ── Alpine.js Base App ─────────────────────────────────────────────────────

function app() {
    return {
        sidebarOpen: false,
        wsConnected: false,

        init() {
            ws.connect();
            ws.on('ws_connected', () => { this.wsConnected = true; });
            ws.on('ws_disconnected', () => { this.wsConnected = false; });

            // Close sidebar on navigation (mobile)
            document.querySelectorAll('.nav-link').forEach(link => {
                link.addEventListener('click', () => { this.sidebarOpen = false; });
            });
        },

        async logout() {
            try {
                await api('/api/auth/logout', { method: 'POST' });
            } catch (e) {}
            window.location.href = '/login';
        },
    };
}
