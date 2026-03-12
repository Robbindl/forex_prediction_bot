/**
 * Trading Intelligence Gateway — Node.js WebSocket Server
 * =========================================================
 * Sits between Python bot and all clients (browser, mobile, external).
 * Subscribes to Redis pub/sub channels from the Python services.
 * Broadcasts to every connected WebSocket client instantly.
 *
 * Port: 8080  (WebSocket ws://localhost:8080)
 * Also proxies REST calls to Flask on :5000 at /api/* paths
 *
 * Channels subscribed from Redis:
 *   signals       — trading signals from quality gate
 *   prices        — live price ticks per asset
 *   whale_alerts  — whale movement events
 *   sentiment     — sentiment score updates
 *   orderflow     — bid/ask delta and imbalance
 *   alpha         — alpha discovery engine signals
 *   predictions   — AI price predictions with targets
 *   positions     — open position updates
 */

const http     = require('http');
const WebSocket = require('ws');
const Redis    = require('ioredis');
const express  = require('express');
const cors     = require('cors');
const { createProxyMiddleware } = require('http-proxy-middleware');

// ── Config ─────────────────────────────────────────────────────────────────
const WS_PORT      = 8080;
const FLASK_URL    = 'http://localhost:5000';
const REDIS_HOST   = process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PORT   = parseInt(process.env.REDIS_PORT || '6379');
const REDIS_PASS   = process.env.REDIS_PASSWORD || null;

// Channels that go to ALL subscribers
const BROADCAST_CHANNELS = [
  'signals',
  'prices',
  'whale_alerts',
  'sentiment',
  'orderflow',
  'alpha',
  'predictions',
  'positions',
];

// ── Express app (REST proxy to Flask) ──────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

// Proxy /api/* → Flask :5000
app.use('/api', createProxyMiddleware({
  target: FLASK_URL,
  changeOrigin: true,
  pathRewrite: { '^/api': '/api' },
  on: {
    error: (err, req, res) => {
      res.status(502).json({ error: 'Flask unavailable', detail: err.message });
    }
  }
}));

// Health endpoint (gateway-only, no Flask needed)
app.get('/health', (req, res) => {
  res.json({
    gateway:    'ok',
    clients:    wss ? wss.clients.size : 0,
    redis:      redisConnected ? 'ok' : 'disconnected',
    uptime:     process.uptime(),
    timestamp:  new Date().toISOString(),
  });
});

// Stats endpoint
app.get('/stats', (req, res) => {
  res.json({
    total_clients:    wss ? wss.clients.size : 0,
    messages_sent:    stats.messagesSent,
    messages_received:stats.messagesReceived,
    redis_connected:  redisConnected,
    channels:         BROADCAST_CHANNELS,
    uptime_seconds:   process.uptime(),
  });
});

// ── HTTP + WebSocket server ─────────────────────────────────────────────────
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

// ── Stats ───────────────────────────────────────────────────────────────────
const stats = { messagesSent: 0, messagesReceived: 0 };

// ── Redis subscriber ────────────────────────────────────────────────────────
let redisConnected = false;

const redisOpts = {
  host:            REDIS_HOST,
  port:            REDIS_PORT,
  password:        REDIS_PASS || undefined,
  retryStrategy:   (times) => Math.min(times * 500, 5000),
  lazyConnect:     false,
  enableOfflineQueue: true,
};

const sub = new Redis(redisOpts);
const pub = new Redis(redisOpts);   // separate connection for publishing

sub.on('connect', () => {
  redisConnected = true;
  console.log(`[Redis] Connected to ${REDIS_HOST}:${REDIS_PORT}`);
  sub.subscribe(...BROADCAST_CHANNELS, (err, count) => {
    if (err) {
      console.error('[Redis] Subscribe error:', err.message);
    } else {
      console.log(`[Redis] Subscribed to ${count} channels: ${BROADCAST_CHANNELS.join(', ')}`);
    }
  });
});

sub.on('error', (err) => {
  redisConnected = false;
  console.warn('[Redis] Error:', err.message);
});

sub.on('reconnecting', () => {
  console.log('[Redis] Reconnecting…');
});

// When Redis publishes a message → broadcast to all WebSocket clients
sub.on('message', (channel, message) => {
  try {
    // Parse the message (Python publishes JSON strings)
    let payload;
    try {
      payload = JSON.parse(message);
    } catch {
      payload = { raw: message };
    }

    // Wrap in a standard envelope
    const envelope = JSON.stringify({
      channel,
      data:      payload,
      timestamp: Date.now(),
    });

    // Broadcast to all connected clients
    let sent = 0;
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        // Check if client is subscribed to this channel (or all channels)
        const subs = client._subscriptions;
        if (!subs || subs.has('*') || subs.has(channel)) {
          client.send(envelope);
          sent++;
        }
      }
    });

    stats.messagesSent += sent;
  } catch (err) {
    console.error('[Gateway] Broadcast error:', err.message);
  }
});

// ── WebSocket client handling ───────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  const ip = req.socket.remoteAddress;
  console.log(`[WS] Client connected  ip=${ip}  total=${wss.clients.size}`);

  // Default: subscribe to all channels
  ws._subscriptions = new Set(['*']);
  ws._isAlive = true;

  // Send welcome message with available channels
  ws.send(JSON.stringify({
    channel:   'system',
    data: {
      type:     'welcome',
      message:  'Connected to Trading Intelligence Gateway',
      channels: BROADCAST_CHANNELS,
      version:  '1.0.0',
    },
    timestamp: Date.now(),
  }));

  // Handle messages from client (e.g. channel subscription control)
  ws.on('message', (raw) => {
    stats.messagesReceived++;
    try {
      const msg = JSON.parse(raw.toString());

      // Client can subscribe to specific channels: { action: 'subscribe', channels: ['signals','prices'] }
      if (msg.action === 'subscribe' && Array.isArray(msg.channels)) {
        ws._subscriptions = new Set(msg.channels);
        ws.send(JSON.stringify({
          channel: 'system',
          data: { type: 'subscribed', channels: [...ws._subscriptions] },
          timestamp: Date.now(),
        }));
        return;
      }

      // Client can subscribe to all: { action: 'subscribe_all' }
      if (msg.action === 'subscribe_all') {
        ws._subscriptions = new Set(['*']);
        ws.send(JSON.stringify({
          channel: 'system',
          data: { type: 'subscribed', channels: ['*'] },
          timestamp: Date.now(),
        }));
        return;
      }

      // Ping / pong
      if (msg.type === 'ping') {
        ws.send(JSON.stringify({ channel: 'system', data: { type: 'pong' }, timestamp: Date.now() }));
        return;
      }

      // Client can publish to a channel (for admin/testing)
      if (msg.action === 'publish' && msg.channel && msg.data) {
        pub.publish(msg.channel, JSON.stringify(msg.data));
        return;
      }

    } catch (err) {
      // Ignore malformed messages
    }
  });

  ws.on('pong', () => { ws._isAlive = true; });

  ws.on('close', () => {
    console.log(`[WS] Client disconnected  total=${wss.clients.size}`);
  });

  ws.on('error', (err) => {
    console.warn(`[WS] Client error: ${err.message}`);
  });
});

// ── Heartbeat (ping all clients every 30s, drop dead ones) ─────────────────
const heartbeat = setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws._isAlive === false) {
      ws.terminate();
      return;
    }
    ws._isAlive = false;
    ws.ping();
  });
}, 30_000);

wss.on('close', () => clearInterval(heartbeat));

// ── Start server ────────────────────────────────────────────────────────────
server.listen(WS_PORT, () => {
  console.log('');
  console.log('╔══════════════════════════════════════════════╗');
  console.log('║   Trading Intelligence Gateway               ║');
  console.log(`║   WebSocket  :  ws://localhost:${WS_PORT}          ║`);
  console.log(`║   REST proxy : http://localhost:${WS_PORT}/api/*   ║`);
  console.log(`║   Health     : http://localhost:${WS_PORT}/health  ║`);
  console.log('╚══════════════════════════════════════════════╝');
  console.log('');
  console.log(`[Redis] Connecting to ${REDIS_HOST}:${REDIS_PORT}…`);
});

// ── Graceful shutdown ────────────────────────────────────────────────────────
process.on('SIGINT',  shutdown);
process.on('SIGTERM', shutdown);

function shutdown() {
  console.log('\n[Gateway] Shutting down…');
  clearInterval(heartbeat);
  wss.clients.forEach(ws => ws.close(1001, 'Server shutting down'));
  server.close(() => {
    sub.quit();
    pub.quit();
    console.log('[Gateway] Done.');
    process.exit(0);
  });
}
