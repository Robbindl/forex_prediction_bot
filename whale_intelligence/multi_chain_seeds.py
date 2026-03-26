"""
multi_chain_seeds.py — Seed wallet list across all supported blockchains.
Add your whale addresses here organized by chain.
"""

MULTI_CHAIN_SEED_WALLETS = [
    # ── ETHEREUM ──────────────────────────────────────────────────────────
    {
        "address": "0x28c6c06298d514db089934071355e5743bf21d60",
        "label": "Binance ETH Hot Wallet",
        "chain": "eth",
        "type": "exchange",
    },
    {
        "address": "0x21a31ee1afc51d94c2efccaa2092ad1028285549",
        "label": "Binance ETH Cold Wallet",
        "chain": "eth",
        "type": "exchange",
    },
    # ── BITCOIN ───────────────────────────────────────────────────────────
    {
        "address": "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo",
        "label": "Binance BTC Hot Wallet",
        "chain": "btc",
        "type": "exchange",
    },
    # ── BNB CHAIN (BSC) ───────────────────────────────────────────────────
    {
        "address": "0xF977814e90dA44bFA03b6295C0ED0e169474b3dd",
        "label": "Binance BNB Hot Wallet",
        "chain": "bnb",
        "type": "exchange",
    },
    {
        "address": "0x0b4d5196b4aa12f13a0de3c6f4a6f2f64e7f6b4d",
        "label": "Pancake Swap BNB Pool",
        "chain": "bnb",
        "type": "dex",
    },
    # ── SOLANA ────────────────────────────────────────────────────────────
    {
        "address": "5V1zdUfHbikpY6R7a4XYJVYsBt6bnH3cJbS2nM2RkGbB",
        "label": "Magic Eden Escrow",
        "chain": "sol",
        "type": "marketplace",
    },
    {
        "address": "Cbvk83cn5fKmzKUHsMmT5mevMT7KYfHwKKEzEqiJEV9j",
        "label": "Marinade Finance",
        "chain": "sol",
        "type": "defi",
    },
    # ── XRP LEDGER ────────────────────────────────────────────────────────
    {
        "address": "rN7n7otQDd6FczFgLdSqtcsAUxDkw6fzRH",
        "label": "Binance XRP Hot Wallet",
        "chain": "xrp",
        "type": "exchange",
    },
    {
        "address": "raFcdz1g8LWJDJWJ33zZ4tNKQXvxXu9RWH",
        "label": "Ripple Escrow",
        "chain": "xrp",
        "type": "foundation",
    },
]
