/**
 * Trading Intelligence Gateway — Node.js WebSocket Server
 * =========================================================
 * Professional version with auto-resubscribe and message queue
 */

const http     = require('http');
const WebSocket = require('ws');
const Redis    = require('ioredis');
const express  = require('express');
const cors     = require('cors');
const { createProxyMiddleware } = require('http-proxy-middleware');

// ── Config ─────────────────────────────────────────────────────────────────
const WS_PORT      = 8081;
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

// ── Stats ───────────────────────────────────────────────────────────────────
const stats = { messagesSent: 0, messagesReceived: 0 };

// ── Message Queue Buffer (stores last 100 messages per channel) ────────────
const messageBuffer = new Map(); // channel -> array of last 100 messages
BROADCAST_CHANNELS.forEach(channel => messageBuffer.set(channel, []));

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

// ── HTTP + WebSocket server ─────────────────────────────────────────────────
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

// ── Redis connection options ────────────────────────────────────────────────
const redisOpts = {
  host:            REDIS_HOST,
  port:            REDIS_PORT,
  password:        REDIS_PASS || undefined,
  retryStrategy:   (times) => {
    if (times > 5) {
      // Stop retrying after 5 attempts — gateway runs in polling-only mode
      return null;
    }
    return Math.min(times * 500, 3000);
  },
  maxRetriesPerRequest: null,
  enableReadyCheck: true,
  lazyConnect: true,   // don't connect until we explicitly call connect()
};

// ── Redis connections ───────────────────────────────────────────────────────
const sub = new Redis(redisOpts);
const pub = new Redis(redisOpts);

let redisConnected = false;
let redisAttempted = false;

// Attempt Redis connection — gateway starts regardless of outcome
function tryConnectRedis() {
  if (redisAttempted) return;
  redisAttempted = true;
  console.log(`[Redis] Connecting to ${REDIS_HOST}:${REDIS_PORT}…`);
  sub.connect().catch(() => {});
  pub.connect().catch(() => {});
}

// ── Helper to subscribe to all channels ────────────────────────────────────
async function subscribeChannels() {
  try {
    const count = await sub.subscribe(...BROADCAST_CHANNELS);
    console.log(`[Redis Subscriber] Subscribed to ${count} channels: ${BROADCAST_CHANNELS.join(', ')}`);
    return true;
  } catch (err) {
    console.error('[Redis Subscriber] Subscribe error:', err.message);
    return false;
  }
}

// ── Subscriber connection events ───────────────────────────────────────────
sub.on('connect', async () => {
  console.log(`[Redis] Connected to ${REDIS_HOST}:${REDIS_PORT}`);
  redisConnected = true;
  await subscribeChannels();
});

sub.on('ready', async () => {
  if (!redisConnected) {
    console.log('[Redis] Ready after reconnect, resubscribing…');
    redisConnected = true;
    await subscribeChannels();
    broadcastSystemMessage('redis_reconnected', 'Redis connection restored');
  }
});

sub.on('reconnecting', () => {
  console.log('[Redis] Reconnecting…');
  redisConnected = false;
});

sub.on('error', (err) => {
  if (!redisConnected) {
    // First-time failure — log once clearly, don't spam
    console.warn(`[Redis] Unavailable (${err.message}) — gateway running in polling-only mode`);
  }
});

sub.on('end', () => {
  console.log('[Redis] Connection ended — gateway continues without pub/sub');
  redisConnected = false;
});

// ── Publisher connection ────────────────────────────────────────────────────
pub.on('connect', () => {
  console.log('[Redis Publisher] Connected');
});

pub.on('error', (err) => {
  console.warn('[Redis Publisher] Error:', err.message);
});

// ── Helper to broadcast system messages ────────────────────────────────────
function broadcastSystemMessage(type, message) {
  const envelope = JSON.stringify({
    channel: 'system',
    data: {
      type,
      message,
      timestamp: Date.now(),
    },
    timestamp: Date.now(),
  });
  
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(envelope);
    }
  });
}

// ── Redis messages → broadcast + buffer ────────────────────────────────────
sub.on('message', (channel, message) => {
  try {
    // Parse the message
    let payload;
    try {
      payload = JSON.parse(message);
    } catch {
      payload = { raw: message };
    }

    // Add to buffer (keep last 100 messages per channel)
    const buffer = messageBuffer.get(channel) || [];
    buffer.push({ payload, timestamp: Date.now() });
    if (buffer.length > 100) buffer.shift();
    messageBuffer.set(channel, buffer);

    // Wrap in a standard envelope
    const envelope = JSON.stringify({
      channel,
      data: payload,
      timestamp: Date.now(),
    });

    // Broadcast to all connected clients
    let sent = 0;
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
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

  // Handle messages from client
  ws.on('message', (raw) => {
    stats.messagesReceived++;
    try {
      const msg = JSON.parse(raw.toString());

      // Subscribe to specific channels
      if (msg.action === 'subscribe' && Array.isArray(msg.channels)) {
        ws._subscriptions = new Set(msg.channels);
        ws.send(JSON.stringify({
          channel: 'system',
          data: { type: 'subscribed', channels: [...ws._subscriptions] },
          timestamp: Date.now(),
        }));
        
        // Send buffered messages for these channels
        if (msg.replayBuffer) {
          msg.channels.forEach(channel => {
            const buffer = messageBuffer.get(channel) || [];
            buffer.forEach(bufferedMsg => {
              ws.send(JSON.stringify({
                channel,
                data: bufferedMsg.payload,
                timestamp: bufferedMsg.timestamp,
                replay: true,
              }));
            });
          });
        }
        return;
      }

      // Subscribe to all channels
      if (msg.action === 'subscribe_all') {
        ws._subscriptions = new Set(['*']);
        ws.send(JSON.stringify({
          channel: 'system',
          data: { type: 'subscribed', channels: ['*'] },
          timestamp: Date.now(),
        }));
        
        // Send buffered messages for all channels
        if (msg.replayBuffer) {
          BROADCAST_CHANNELS.forEach(channel => {
            const buffer = messageBuffer.get(channel) || [];
            buffer.forEach(bufferedMsg => {
              ws.send(JSON.stringify({
                channel,
                data: bufferedMsg.payload,
                timestamp: bufferedMsg.timestamp,
                replay: true,
              }));
            });
          });
        }
        return;
      }

      // Ping / pong
      if (msg.type === 'ping') {
        ws.send(JSON.stringify({ 
          channel: 'system', 
          data: { type: 'pong' }, 
          timestamp: Date.now() 
        }));
        return;
      }

      // Client can publish (for testing)
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

// ── Heartbeat ───────────────────────────────────────────────────────────────
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

// ── Health endpoint ─────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({
    gateway:    'ok',
    clients:    wss ? wss.clients.size : 0,
    redis:      redisConnected ? 'ok' : 'disconnected',
    uptime:     process.uptime(),
    timestamp:  new Date().toISOString(),
  });
});

// ── Stats endpoint ──────────────────────────────────────────────────────────
app.get('/stats', (req, res) => {
  res.json({
    total_clients:    wss ? wss.clients.size : 0,
    messages_sent:    stats.messagesSent,
    messages_received:stats.messagesReceived,
    redis_connected:  redisConnected,
    channels:         BROADCAST_CHANNELS,
    buffer_sizes:     Object.fromEntries(
      [...messageBuffer.entries()].map(([k, v]) => [k, v.length])
    ),
    uptime_seconds:   process.uptime(),
  });
});

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

  // Attempt Redis after server is up — if it fails, gateway still works
  // for WebSocket proxying and client connections
  tryConnectRedis();
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