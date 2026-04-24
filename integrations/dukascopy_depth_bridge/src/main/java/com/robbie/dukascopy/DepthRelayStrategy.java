package com.robbie.dukascopy;

import com.dukascopy.api.IAccount;
import com.dukascopy.api.IBar;
import com.dukascopy.api.IConsole;
import com.dukascopy.api.IContext;
import com.dukascopy.api.IMessage;
import com.dukascopy.api.IStrategy;
import com.dukascopy.api.ITick;
import com.dukascopy.api.Instrument;
import com.dukascopy.api.JFException;
import com.dukascopy.api.Period;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

final class DepthRelayStrategy implements IStrategy {
    private static final SimpleDateFormat ISO_UTC = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);

    static {
        ISO_UTC.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private final LinkedHashMap<String, String> assetSymbols;
    private final String environment;
    private final long minEmitMs;
    private final int maxLevels;
    private final Map<Instrument, String> instrumentToAsset = new LinkedHashMap<Instrument, String>();
    private final Map<Instrument, String> instrumentToSymbol = new LinkedHashMap<Instrument, String>();
    private final ConcurrentHashMap<String, Long> lastEmitAt = new ConcurrentHashMap<String, Long>();
    private IContext context;
    private IConsole console;

    DepthRelayStrategy(Map<String, String> assetSymbols, String environment, long minEmitMs, int maxLevels) {
        this.assetSymbols = new LinkedHashMap<String, String>(assetSymbols);
        this.environment = environment == null || environment.trim().isEmpty() ? "demo" : environment.trim();
        this.minEmitMs = Math.max(50L, minEmitMs);
        this.maxLevels = Math.max(1, maxLevels);
    }

    @Override
    public void onStart(IContext context) throws JFException {
        this.context = context;
        this.console = context.getConsole();

        Set<Instrument> subscribed = new LinkedHashSet<Instrument>();
        for (Map.Entry<String, String> entry : assetSymbols.entrySet()) {
            Instrument instrument = resolveInstrument(entry.getValue());
            if (instrument == null) {
                log("Skipping unsupported instrument mapping: " + entry.getKey() + "=" + entry.getValue());
                continue;
            }
            instrumentToAsset.put(instrument, entry.getKey());
            instrumentToSymbol.put(instrument, entry.getValue());
            subscribed.add(instrument);
        }

        subscribe(context, subscribed);
        log("Subscribed instruments: " + subscribed);
    }

    @Override
    public void onTick(Instrument instrument, ITick tick) throws JFException {
        String asset = instrumentToAsset.get(instrument);
        if (asset == null || tick == null) {
            return;
        }

        long now = System.currentTimeMillis();
        Long previous = lastEmitAt.get(asset);
        if (previous != null && now - previous.longValue() < minEmitMs) {
            return;
        }
        lastEmitAt.put(asset, now);

        List<PriceLevel> levels = extractLevels(tick);
        double bid = tick.getBid();
        double ask = tick.getAsk();
        double price = midpoint(bid, ask);
        Double bidSize = firstFinite(invokeDouble(tick, "getBidVolume"), firstBidSize(levels));
        Double askSize = firstFinite(invokeDouble(tick, "getAskVolume"), firstAskSize(levels));
        Double totalBidVolume = firstFinite(invokeDouble(tick, "getTotalBidVolume"), sumSizes(levels, true), bidSize);
        Double totalAskVolume = firstFinite(invokeDouble(tick, "getTotalAskVolume"), sumSizes(levels, false), askSize);

        String payload = buildJson(
            asset,
            instrumentToSymbol.get(instrument),
            instrument.toString(),
            tick.getTime(),
            bid,
            ask,
            price,
            bidSize,
            askSize,
            totalBidVolume,
            totalAskVolume,
            levels
        );
        PrintStream out = System.out;
        out.println(payload);
        out.flush();
    }

    @Override
    public void onBar(Instrument instrument, Period period, IBar askBar, IBar bidBar) {
    }

    @Override
    public void onMessage(IMessage message) {
        if (message != null) {
            log("Message: " + String.valueOf(message.getType()));
        }
    }

    @Override
    public void onAccount(IAccount account) {
    }

    @Override
    public void onStop() {
        log("Depth relay strategy stopping.");
    }

    private void subscribe(IContext context, Set<Instrument> instruments) {
        if (instruments.isEmpty()) {
            return;
        }
        try {
            Method withFlag = context.getClass().getMethod("setSubscribedInstruments", Set.class, boolean.class);
            withFlag.invoke(context, instruments, Boolean.TRUE);
            return;
        } catch (Exception ignored) {
        }
        try {
            Method withoutFlag = context.getClass().getMethod("setSubscribedInstruments", Set.class);
            withoutFlag.invoke(context, instruments);
        } catch (Exception error) {
            throw new IllegalStateException("Could not subscribe instruments: " + error.getMessage(), error);
        }
    }

    private Instrument resolveInstrument(String symbol) {
        if (symbol == null || symbol.trim().isEmpty()) {
            return null;
        }
        try {
            return Instrument.fromString(symbol.trim());
        } catch (Exception ignored) {
        }
        try {
            return Instrument.valueOf(symbol.trim().replace("/", "").replace(".", "").replace("-", "_"));
        } catch (Exception ignored) {
        }
        return null;
    }

    private List<PriceLevel> extractLevels(ITick tick) {
        double[] bids = tick.getBids();
        double[] asks = tick.getAsks();
        double[] bidVolumes = tick.getBidVolumes();
        double[] askVolumes = tick.getAskVolumes();

        int levelCount = Math.max(
            Math.max(bids == null ? 0 : bids.length, asks == null ? 0 : asks.length),
            Math.max(bidVolumes == null ? 0 : bidVolumes.length, askVolumes == null ? 0 : askVolumes.length)
        );

        List<PriceLevel> levels = new ArrayList<PriceLevel>();
        for (int i = 0; i < levelCount && levels.size() < maxLevels; i++) {
            PriceLevel level = new PriceLevel();
            level.bid = bids != null && i < bids.length ? finiteOrNull(bids[i]) : null;
            level.ask = asks != null && i < asks.length ? finiteOrNull(asks[i]) : null;
            level.bidSize = bidVolumes != null && i < bidVolumes.length ? finiteOrNull(bidVolumes[i]) : null;
            level.askSize = askVolumes != null && i < askVolumes.length ? finiteOrNull(askVolumes[i]) : null;

            if (level.bid != null || level.ask != null || level.bidSize != null || level.askSize != null) {
                levels.add(level);
            }
        }

        if (levels.isEmpty()) {
            PriceLevel top = new PriceLevel();
            top.bid = finiteOrNull(tick.getBid());
            top.ask = finiteOrNull(tick.getAsk());
            top.bidSize = invokeDouble(tick, "getBidVolume");
            top.askSize = invokeDouble(tick, "getAskVolume");
            if (top.bid != null || top.ask != null) {
                levels.add(top);
            }
        }
        return levels;
    }

    private static Object invokeAny(Object target, String... methodNames) {
        if (target == null) {
            return null;
        }
        for (String methodName : methodNames) {
            try {
                Method method = target.getClass().getMethod(methodName);
                return method.invoke(target);
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    private static Double invokeDouble(Object target, String methodName) {
        Object value = invokeAny(target, methodName);
        if (value instanceof Number) {
            return finiteOrNull(((Number) value).doubleValue());
        }
        return null;
    }

    private static Double firstFinite(Double... values) {
        if (values == null) {
            return null;
        }
        for (Double value : values) {
            if (value != null && !Double.isNaN(value.doubleValue()) && !Double.isInfinite(value.doubleValue())) {
                return value;
            }
        }
        return null;
    }

    private static Double firstBidSize(List<PriceLevel> levels) {
        if (levels == null) {
            return null;
        }
        for (PriceLevel level : levels) {
            if (level != null && level.bidSize != null) {
                return level.bidSize;
            }
        }
        return null;
    }

    private static Double firstAskSize(List<PriceLevel> levels) {
        if (levels == null) {
            return null;
        }
        for (PriceLevel level : levels) {
            if (level != null && level.askSize != null) {
                return level.askSize;
            }
        }
        return null;
    }

    private static Double sumSizes(List<PriceLevel> levels, boolean bids) {
        if (levels == null || levels.isEmpty()) {
            return null;
        }
        double total = 0.0;
        boolean seen = false;
        for (PriceLevel level : levels) {
            if (level == null) {
                continue;
            }
            Double size = bids ? level.bidSize : level.askSize;
            if (size == null) {
                continue;
            }
            total += size.doubleValue();
            seen = true;
        }
        return seen ? total : null;
    }

    private static double midpoint(double bid, double ask) {
        if (Double.isFinite(bid) && Double.isFinite(ask) && bid > 0.0 && ask > 0.0) {
            return (bid + ask) / 2.0;
        }
        if (Double.isFinite(ask) && ask > 0.0) {
            return ask;
        }
        if (Double.isFinite(bid) && bid > 0.0) {
            return bid;
        }
        return 0.0;
    }

    private String buildJson(
        String asset,
        String symbol,
        String instrumentName,
        long timestampMillis,
        double bid,
        double ask,
        double price,
        Double bidSize,
        Double askSize,
        Double totalBidVolume,
        Double totalAskVolume,
        List<PriceLevel> levels
    ) {
        StringBuilder sb = new StringBuilder(512);
        sb.append('{');
        appendStringField(sb, "asset", asset, true);
        appendStringField(sb, "dukascopy_symbol", symbol, false);
        appendStringField(sb, "instrument_name", instrumentName, false);
        appendNumberField(sb, "bid", bid, false);
        appendNumberField(sb, "ask", ask, false);
        appendNumberField(sb, "price", price, false);
        appendNullableNumberField(sb, "bid_size", bidSize, false);
        appendNullableNumberField(sb, "ask_size", askSize, false);
        appendNullableNumberField(sb, "total_bid_volume", totalBidVolume, false);
        appendNullableNumberField(sb, "total_ask_volume", totalAskVolume, false);
        appendLevels(sb, levels);
        appendNumberField(sb, "timestamp", timestampMillis / 1000.0d, false);
        appendStringField(sb, "as_of_utc", ISO_UTC.format(new Date(timestampMillis > 0L ? timestampMillis : System.currentTimeMillis())), false);
        appendStringField(sb, "environment", environment, false);
        sb.append('}');
        return sb.toString();
    }

    private static void appendLevels(StringBuilder sb, List<PriceLevel> levels) {
        sb.append(",\"levels\":[");
        boolean first = true;
        for (PriceLevel level : levels) {
            if (level == null) {
                continue;
            }
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('{');
            appendNullableNumberField(sb, "bid", level.bid, true);
            appendNullableNumberField(sb, "ask", level.ask, false);
            appendNullableNumberField(sb, "bid_size", level.bidSize, false);
            appendNullableNumberField(sb, "ask_size", level.askSize, false);
            sb.append('}');
        }
        sb.append(']');
    }

    private static void appendStringField(StringBuilder sb, String key, String value, boolean first) {
        if (!first) {
            sb.append(',');
        }
        sb.append('"').append(escapeJson(key)).append("\":");
        if (value == null) {
            sb.append("null");
        } else {
            sb.append('"').append(escapeJson(value)).append('"');
        }
    }

    private static void appendNumberField(StringBuilder sb, String key, double value, boolean first) {
        if (!first) {
            sb.append(',');
        }
        sb.append('"').append(escapeJson(key)).append("\":");
        sb.append(Double.isFinite(value) ? trimNumber(value) : "null");
    }

    private static void appendNullableNumberField(StringBuilder sb, String key, Double value, boolean first) {
        if (!first) {
            sb.append(',');
        }
        sb.append('"').append(escapeJson(key)).append("\":");
        if (value == null || !Double.isFinite(value.doubleValue())) {
            sb.append("null");
        } else {
            sb.append(trimNumber(value.doubleValue()));
        }
    }

    private static String trimNumber(double value) {
        String raw = Double.toString(value);
        if (raw.contains("E") || raw.contains("e")) {
            return String.format(Locale.US, "%.10f", value).replaceAll("0+$", "").replaceAll("\\.$", "");
        }
        return raw;
    }

    private static String escapeJson(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '"':
                    sb.append("\\\"");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 32) {
                        sb.append(String.format(Locale.US, "\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                    break;
            }
        }
        return sb.toString();
    }

    private static Double finiteOrNull(double value) {
        return Double.isFinite(value) && value > 0.0 ? value : null;
    }

    private void log(String message) {
        if (console != null && console.getOut() != null) {
            console.getOut().println("[DepthRelay] " + message);
        } else {
            System.err.println("[DepthRelay] " + message);
            System.err.flush();
        }
    }

    private static final class PriceLevel {
        private Double bid;
        private Double ask;
        private Double bidSize;
        private Double askSize;
    }
}
