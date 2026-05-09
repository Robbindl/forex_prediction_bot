package com.robbie.dukascopy;

import com.dukascopy.api.system.ClientFactory;
import com.dukascopy.api.system.IClient;
import com.dukascopy.api.system.ISystemListener;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class DepthBridgeMain {
    private static final AtomicLong STRATEGY_ID = new AtomicLong(-1L);

    private DepthBridgeMain() {
    }

    public static void main(String[] args) throws Exception {
        String jnlpUrl = env("DUKASCOPY_BRIDGE_JNLP_URL");
        String username = env("DUKASCOPY_BRIDGE_USERNAME");
        String password = env("DUKASCOPY_BRIDGE_PASSWORD");
        String pin = env("DUKASCOPY_BRIDGE_PIN");
        String assetMapRaw = env("DUKASCOPY_BRIDGE_ASSET_MAP");
        long minEmitMs = parseLong(env("DUKASCOPY_BRIDGE_MIN_EMIT_MS"), 150L, 50L);
        int maxLevels = (int) parseLong(env("DUKASCOPY_BRIDGE_MAX_LEVELS"), 20L, 1L);
        KeepaliveConfig keepaliveConfig = new KeepaliveConfig(
            parseBoolean(env("DUKASCOPY_BRIDGE_KEEPALIVE_ENABLED"), false),
            defaultString(env("DUKASCOPY_BRIDGE_KEEPALIVE_SYMBOL"), "EUR/USD"),
            parseDouble(env("DUKASCOPY_BRIDGE_KEEPALIVE_AMOUNT"), 0.001d, 0.000001d),
            TimeUnit.DAYS.toMillis(parseLong(env("DUKASCOPY_BRIDGE_KEEPALIVE_INTERVAL_DAYS"), 7L, 1L)),
            TimeUnit.SECONDS.toMillis(parseLong(env("DUKASCOPY_BRIDGE_KEEPALIVE_HOLD_SECONDS"), 15L, 2L)),
            env("DUKASCOPY_BRIDGE_KEEPALIVE_STATE_PATH"),
            parseBoolean(env("DUKASCOPY_BRIDGE_KEEPALIVE_ALLOW_LIVE"), false)
        );

        if (isBlank(jnlpUrl) || isBlank(username) || isBlank(password)) {
            log("Missing Dukascopy credentials. Set DUKASCOPY_BRIDGE_JNLP_URL, USERNAME, PASSWORD.");
            System.exit(2);
            return;
        }

        LinkedHashMap<String, String> assetMap = parseAssetMap(assetMapRaw);
        if (assetMap.isEmpty()) {
            log("No assets configured. Set DUKASCOPY_BRIDGE_ASSET_MAP=ASSET=SYMBOL;...");
            System.exit(2);
            return;
        }

        final CountDownLatch exitLatch = new CountDownLatch(1);
        final IClient client = ClientFactory.getDefaultInstance();

        client.setSystemListener(new ISystemListener() {
            @Override
            public void onStart(long processId) {
                STRATEGY_ID.set(processId);
                log("Strategy started. processId=" + processId);
            }

            @Override
            public void onStop(long processId) {
                STRATEGY_ID.compareAndSet(processId, -1L);
                log("Strategy stopped. processId=" + processId);
            }

            @Override
            public void onConnect() {
                log("Connected to Dukascopy.");
            }

            @Override
            public void onDisconnect() {
                log("Disconnected from Dukascopy.");
                exitLatch.countDown();
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                stopStrategyQuietly(client, STRATEGY_ID.get());
                disconnectQuietly(client);
                exitLatch.countDown();
            }
        }, "dukascopy-depth-shutdown"));

        connect(client, jnlpUrl, username, password, pin);
        waitForConnection(client, 45);

        DepthRelayStrategy strategy = new DepthRelayStrategy(
            assetMap,
            environmentFromJnlp(jnlpUrl),
            minEmitMs,
            maxLevels,
            keepaliveConfig
        );

        long processId = client.startStrategy(strategy);
        STRATEGY_ID.set(processId);
        log("Depth bridge running for " + assetMap.size() + " assets. processId=" + processId);

        while (!exitLatch.await(2, TimeUnit.SECONDS)) {
            if (!client.isConnected()) {
                log("Client connection lost.");
                break;
            }
        }

        stopStrategyQuietly(client, STRATEGY_ID.get());
        disconnectQuietly(client);
    }

    private static LinkedHashMap<String, String> parseAssetMap(String raw) {
        LinkedHashMap<String, String> result = new LinkedHashMap<String, String>();
        if (isBlank(raw)) {
            return result;
        }
        String[] entries = raw.split(";");
        for (String entry : entries) {
            if (isBlank(entry)) {
                continue;
            }
            int split = entry.indexOf('=');
            if (split <= 0 || split >= entry.length() - 1) {
                continue;
            }
            String asset = entry.substring(0, split).trim();
            String symbol = entry.substring(split + 1).trim();
            if (!asset.isEmpty() && !symbol.isEmpty()) {
                result.put(asset, symbol);
            }
        }
        return result;
    }

    private static void connect(IClient client, String jnlpUrl, String username, String password, String pin) throws Exception {
        if (!isBlank(pin)) {
            try {
                Method fourArg = client.getClass().getMethod(
                    "connect",
                    String.class,
                    String.class,
                    String.class,
                    String.class
                );
                fourArg.invoke(client, jnlpUrl, username, password, pin);
                return;
            } catch (NoSuchMethodException ignored) {
                log("PIN-aware connect method not available in this JForex library. Falling back to username/password login.");
            }
        }
        client.connect(jnlpUrl, username, password);
    }

    private static void waitForConnection(IClient client, int timeoutSeconds) throws InterruptedException {
        for (int i = 0; i < timeoutSeconds; i++) {
            if (client.isConnected()) {
                return;
            }
            Thread.sleep(1000L);
        }
        throw new IllegalStateException("Dukascopy client did not connect within " + timeoutSeconds + " seconds");
    }

    private static void stopStrategyQuietly(IClient client, long processId) {
        if (processId <= 0L) {
            return;
        }
        try {
            Method stopStrategy = client.getClass().getMethod("stopStrategy", long.class);
            stopStrategy.invoke(client, processId);
        } catch (Exception ignored) {
        }
    }

    private static void disconnectQuietly(IClient client) {
        try {
            client.disconnect();
        } catch (Exception ignored) {
        }
    }

    private static String environmentFromJnlp(String jnlpUrl) {
        String value = jnlpUrl == null ? "" : jnlpUrl.toLowerCase();
        return value.contains("demo") ? "demo" : "live";
    }

    private static long parseLong(String raw, long fallback, long floor) {
        if (raw == null || raw.trim().isEmpty()) {
            return fallback;
        }
        try {
            return Math.max(floor, Long.parseLong(raw.trim()));
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static double parseDouble(String raw, double fallback, double floor) {
        if (raw == null || raw.trim().isEmpty()) {
            return fallback;
        }
        try {
            return Math.max(floor, Double.parseDouble(raw.trim()));
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static boolean parseBoolean(String raw, boolean fallback) {
        if (raw == null || raw.trim().isEmpty()) {
            return fallback;
        }
        String value = raw.trim().toLowerCase();
        if ("1".equals(value) || "true".equals(value) || "yes".equals(value) || "on".equals(value)) {
            return true;
        }
        if ("0".equals(value) || "false".equals(value) || "no".equals(value) || "off".equals(value)) {
            return false;
        }
        return fallback;
    }

    private static String defaultString(String raw, String fallback) {
        return isBlank(raw) ? fallback : raw.trim();
    }

    private static String env(String key) {
        String value = System.getenv(key);
        return value == null ? "" : value.trim();
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static void log(String message) {
        System.err.println("[DepthBridge] " + message);
        System.err.flush();
    }
}
