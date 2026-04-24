# Dukascopy Depth Bridge

This sidecar connects a JForex demo/live session, subscribes to the configured
instruments, and emits line-delimited JSON snapshots of the latest quote plus
available order-book depth to stdout.

The Python bot starts it automatically when these env vars are set:

```env
DUKASCOPY_LIVE_DEPTH_ENABLED=true
DUKASCOPY_LIVE_DEPTH_JNLP_URL=http://platform.dukascopy.com/demo_3/jforex_3.jnlp
DUKASCOPY_LIVE_DEPTH_USERNAME=your_demo_login
DUKASCOPY_LIVE_DEPTH_PASSWORD=your_demo_password
DUKASCOPY_LIVE_DEPTH_PIN=
```

Optional:

```env
DUKASCOPY_LIVE_DEPTH_ASSETS=EUR/USD,GBP/USD,XAU/USD,US500
DUKASCOPY_LIVE_DEPTH_AUTO_BUILD=true
DUKASCOPY_LIVE_DEPTH_MIN_EMIT_MS=150
DUKASCOPY_LIVE_DEPTH_MAX_LEVELS=20
```

If you prefer to run the sidecar yourself, point the bot at a custom command:

```env
DUKASCOPY_LIVE_DEPTH_CMD=java -jar integrations/dukascopy_depth_bridge/target/dukascopy-depth-bridge-1.0-shaded.jar
```

Build manually:

```bash
mvn -q -DskipTests package
```

Notes:

- Stdout is reserved for JSON snapshots. Logs go to stderr.
- The bot will merge fresh Dukascopy true depth into existing IG/Deriv pricing
  paths instead of replacing those quote routes.
- Crypto stays on the existing exchange order-book path.
