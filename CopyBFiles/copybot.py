"""
copybot.py — copy-trade tracked Solana wallets with TP / SL / copy-sell rules.

How it works:
  1. Opens a websocket to Helius and subscribes to logs mentioning each
     tracked wallet -> we see their transactions ~1-2s after confirmation.
  2. Each tx is fetched from Helius' parse API and classified as a
     token BUY or SELL by that wallet.
  3. Whale BUY (>= MIN_WHALE_BUY_SOL)  -> we buy with BUY_PCT % of our
     wallet's SOL balance (capped by MAX_BUY_SOL), via Jupiter.
  4. Whale SELL of >= COPY_SELL_PCT % of the position we've seen him build
     -> we sell our entire position in that token.
  5. A monitor loop checks prices every POLL_SECONDS and closes positions
     at TAKE_PROFIT_PCT or STOP_LOSS_PCT.
  6. Every action -> Telegram. DRY_RUN=true (default) = alerts only, no money.

Telegram commands:  /positions        show open positions + PnL
                    /sell <mint>      force-close one position
Run: python copybot.py
"""

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import websockets
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------- config ---
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]
HELIUS_API_KEY     = os.environ["HELIUS_API_KEY"]

# Comma-separated wallet addresses to copy, e.g. "abc...,def..."
TRACKED_WALLETS = [w.strip() for w in os.environ["TRACKED_WALLETS"].split(",") if w.strip()]

DRY_RUN            = os.getenv("DRY_RUN", "true").lower() == "true"
BUY_PCT            = float(os.getenv("BUY_PCT", "5"))        # % of SOL balance per buy
MAX_BUY_SOL        = float(os.getenv("MAX_BUY_SOL", "0.1"))  # absolute cap per buy
MIN_WHALE_BUY_SOL  = float(os.getenv("MIN_WHALE_BUY_SOL", "1.0"))  # ignore whale dust
MAX_POSITIONS      = int(os.getenv("MAX_POSITIONS", "3"))
TAKE_PROFIT_PCT    = float(os.getenv("TAKE_PROFIT_PCT", "20"))   # +20% -> sell
STOP_LOSS_PCT      = float(os.getenv("STOP_LOSS_PCT", "20"))     # -20% -> sell
COPY_SELL_PCT      = float(os.getenv("COPY_SELL_PCT", "25"))     # whale sells >=25% of
                                                                 # his stack -> we exit
POLL_SECONDS       = int(os.getenv("POLL_SECONDS", "10"))
SLIPPAGE_BPS       = int(os.getenv("SLIPPAGE_BPS", "300"))       # 3%

HELIUS_WS    = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_PARSE = f"https://api.helius.xyz/v0/transactions?api-key={HELIUS_API_KEY}"
SOL_MINT     = "So11111111111111111111111111111111111111112"
STATE_FILE   = Path("positions.json")

# ------------------------------------------------------------------ state ---
# positions[mint] = {entry_price_sol, my_note, whale, opened_at, tokens_note}
positions: dict = {}
# whale_holdings[whale][mint] = tokens we have SEEN him accumulate.
# If he bought before we started tracking, this undercounts -> we treat
# unknown-holding sells conservatively (any sell = exit signal).
whale_holdings: dict = {}
seen_signatures: set = set()


def save_state() -> None:
    STATE_FILE.write_text(json.dumps({"positions": positions,
                                      "whale_holdings": whale_holdings}))


def load_state() -> None:
    global positions, whale_holdings
    if STATE_FILE.exists():
        data = json.loads(STATE_FILE.read_text())
        positions.update(data.get("positions", {}))
        whale_holdings.update(data.get("whale_holdings", {}))


# ------------------------------------------------------------- telegram ---
async def tg_send(session: aiohttp.ClientSession, text: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                                       "parse_mode": "HTML",
                                       "disable_web_page_preview": True}) as r:
        if r.status != 200:
            print("Telegram error:", await r.text())


# --------------------------------------------------------- tx classification ---
async def fetch_parsed_tx(session: aiohttp.ClientSession, signature: str) -> dict | None:
    """Ask Helius to parse a transaction into a structured object."""
    try:
        async with session.post(HELIUS_PARSE, json={"transactions": [signature]},
                                timeout=aiohttp.ClientTimeout(total=20)) as r:
            arr = await r.json()
            return arr[0] if isinstance(arr, list) and arr else None
    except Exception as e:
        print("parse error:", e)
        return None


def classify_swap(tx: dict, whale: str) -> tuple | None:
    """
    Return ("buy", mint, sol_spent, tokens_received)
        or ("sell", mint, tokens_sold, sol_received)
        or None if this tx is not a swap by the whale.

    Primary source: Helius events.swap (clean). Fallback: tokenTransfers +
    native balance change heuristic (covers DEXes Helius doesn't label).
    """
    # --- clean path: parsed swap event -----------------------------------
    swap = (tx.get("events") or {}).get("swap")
    if swap:
        ni, no = swap.get("nativeInput"), swap.get("nativeOutput")
        t_in  = [t for t in swap.get("tokenInputs", [])  if t.get("userAccount") == whale]
        t_out = [t for t in swap.get("tokenOutputs", []) if t.get("userAccount") == whale]
        if ni and ni.get("account") == whale and t_out:        # SOL -> token = BUY
            t = t_out[0]
            return ("buy", t["mint"], int(ni["amount"]) / 1e9,
                    float(t["rawTokenAmount"]["tokenAmount"]))
        if no and no.get("account") == whale and t_in:         # token -> SOL = SELL
            t = t_in[0]
            return ("sell", t["mint"], float(t["rawTokenAmount"]["tokenAmount"]),
                    int(no["amount"]) / 1e9)

    # --- fallback heuristic ----------------------------------------------
    native = 0.0
    for a in tx.get("accountData", []):
        if a.get("account") == whale:
            native = a.get("nativeBalanceChange", 0) / 1e9
    got, sent = None, None
    for t in tx.get("tokenTransfers", []):
        if t.get("mint") == SOL_MINT:
            continue
        if t.get("toUserAccount") == whale:
            got = t
        elif t.get("fromUserAccount") == whale:
            sent = t
    if got and native < -0.01:                                  # paid SOL, got token
        return ("buy", got["mint"], abs(native), float(got.get("tokenAmount", 0)))
    if sent and native > 0.001:                                 # sent token, got SOL
        return ("sell", sent["mint"], float(sent.get("tokenAmount", 0)), native)
    return None


# ------------------------------------------------------------- execution ---
def _short(mint: str) -> str:
    return mint[:6] + "…" + mint[-4:]


async def open_position(session, whale: str, mint: str,
                        whale_sol: float, whale_tokens: float) -> None:
    # Track whale's stack size regardless of whether we trade.
    whale_holdings.setdefault(whale, {})
    whale_holdings[whale][mint] = whale_holdings[whale].get(mint, 0) + whale_tokens

    if mint in positions:
        save_state()
        return                       # already in — don't average up on every add
    if len(positions) >= MAX_POSITIONS:
        await tg_send(session, f"⏭ Skipped {_short(mint)}: MAX_POSITIONS reached")
        save_state()
        return

    price_sol = await get_price_sol(session, mint)   # entry reference for TP/SL
    if DRY_RUN:
        positions[mint] = {"entry_price_sol": price_sol, "whale": whale,
                           "opened_at": datetime.now(timezone.utc).isoformat(),
                           "dry": True}
        save_state()
        await tg_send(session,
            f"📋 <b>DRY RUN — would BUY</b> {_short(mint)}\n"
            f"Whale {_short(whale)} bought {whale_sol:.2f} SOL\n"
            f"Entry ref: {price_sol:.10f} SOL\n"
            f"https://dexscreener.com/solana/{mint}")
        return

    from jup_trade import buy_with_sol, get_sol_balance
    size = min(get_sol_balance() * BUY_PCT / 100, MAX_BUY_SOL)
    if size < 0.005:
        await tg_send(session, "⚠️ Wallet balance too low to size a buy.")
        return
    try:
        sig, quote = await asyncio.to_thread(buy_with_sol, mint, size, SLIPPAGE_BPS)
        # Real entry price from what we actually paid / received.
        entry = size / (int(quote["outAmount"]) or 1)
        positions[mint] = {"entry_price_sol": entry * 1,  # SOL per raw unit
                           "raw_entry": True, "whale": whale,
                           "opened_at": datetime.now(timezone.utc).isoformat(),
                           "dry": False}
        # For simplicity TP/SL uses DexScreener price vs. reference below.
        positions[mint]["entry_price_sol"] = price_sol or entry
        save_state()
        await tg_send(session,
            f"✅ <b>BOUGHT</b> {_short(mint)} for {size:.4f} SOL "
            f"(copying {_short(whale)})\nhttps://solscan.io/tx/{sig}")
    except Exception as e:
        await tg_send(session, f"⚠️ Buy failed {_short(mint)}: {e}")


async def close_position(session, mint: str, reason: str) -> None:
    pos = positions.pop(mint, None)
    save_state()
    if pos is None:
        return
    if pos.get("dry"):
        await tg_send(session, f"📋 <b>DRY RUN — would SELL</b> {_short(mint)} ({reason})")
        return
    from jup_trade import sell_all
    try:
        sig, _ = await asyncio.to_thread(sell_all, mint, max(SLIPPAGE_BPS, 500))
        await tg_send(session, f"🔴 <b>SOLD</b> {_short(mint)} — {reason}\n"
                               f"https://solscan.io/tx/{sig}")
    except Exception as e:
        positions[mint] = pos            # sell failed -> keep tracking it
        save_state()
        await tg_send(session, f"⚠️ SELL FAILED {_short(mint)} ({reason}): {e}\n"
                               f"Retry manually: /sell {mint}")


# ------------------------------------------------------------ price feed ---
async def get_price_sol(session: aiohttp.ClientSession, mint: str) -> float:
    """Token price in SOL from DexScreener (free). 0.0 if no pair yet."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            pairs = (await r.json()).get("pairs") or []
        if not pairs:
            return 0.0
        best = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
        return float(best.get("priceNative") or 0)
    except Exception:
        return 0.0


async def monitor_positions(session: aiohttp.ClientSession) -> None:
    """TP / SL loop. Runs forever."""
    while True:
        for mint in list(positions.keys()):
            pos = positions[mint]
            entry = pos.get("entry_price_sol") or 0
            if entry <= 0:
                continue
            price = await get_price_sol(session, mint)
            if price <= 0:
                continue
            pnl = (price / entry - 1) * 100
            if pnl >= TAKE_PROFIT_PCT:
                await close_position(session, mint, f"take-profit +{pnl:.1f}%")
            elif pnl <= -STOP_LOSS_PCT:
                await close_position(session, mint, f"stop-loss {pnl:.1f}%")
        await asyncio.sleep(POLL_SECONDS)


# --------------------------------------------------------- whale handling ---
async def handle_whale_tx(session, whale: str, signature: str) -> None:
    if signature in seen_signatures:
        return
    seen_signatures.add(signature)
    if len(seen_signatures) > 5000:          # keep memory bounded
        seen_signatures.clear()

    tx = await fetch_parsed_tx(session, signature)
    if not tx or tx.get("transactionError"):
        return
    result = classify_swap(tx, whale)
    if not result:
        return

    action, mint, a, b = result
    if action == "buy":
        sol_spent, tokens = a, b
        print(f"[whale] {_short(whale)} BUY {_short(mint)} {sol_spent:.2f} SOL")
        if sol_spent >= MIN_WHALE_BUY_SOL:
            await open_position(session, whale, mint, sol_spent, tokens)
    else:
        tokens_sold, sol_got = a, b
        print(f"[whale] {_short(whale)} SELL {_short(mint)} -> {sol_got:.2f} SOL")
        held = whale_holdings.get(whale, {}).get(mint, 0)
        # Reduce our record of his stack either way.
        if held > 0:
            whale_holdings[whale][mint] = max(0, held - tokens_sold)
            save_state()
        if mint not in positions:
            return
        if held > 0:
            pct = tokens_sold / held * 100
            if pct >= COPY_SELL_PCT:
                await close_position(session, mint,
                    f"copy-sell: whale sold {pct:.0f}% of tracked stack")
            else:
                await tg_send(session, f"ℹ️ Whale trimmed {pct:.0f}% of "
                                       f"{_short(mint)} (below {COPY_SELL_PCT}% "
                                       f"threshold — holding)")
        else:
            # He held tokens we never saw him buy -> can't size it -> exit.
            await close_position(session, mint,
                                 "copy-sell: whale sold (unknown stack size)")


# ----------------------------------------------- telegram command listener ---
async def telegram_listener(session: aiohttp.ClientSession) -> None:
    offset = 0
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    while True:
        try:
            async with session.get(url, params={"timeout": 25, "offset": offset},
                                   timeout=aiohttp.ClientTimeout(total=35)) as r:
                updates = (await r.json()).get("result", [])
        except Exception:
            await asyncio.sleep(5)
            continue
        for u in updates:
            offset = u["update_id"] + 1
            text = (u.get("message") or {}).get("text", "")
            chat = str((u.get("message") or {}).get("chat", {}).get("id", ""))
            if chat != str(TELEGRAM_CHAT_ID):
                continue
            if text.startswith("/positions"):
                if not positions:
                    await tg_send(session, "No open positions.")
                    continue
                lines = []
                for mint, pos in positions.items():
                    price = await get_price_sol(session, mint)
                    entry = pos.get("entry_price_sol") or 0
                    pnl = (price / entry - 1) * 100 if entry and price else 0
                    tag = "DRY" if pos.get("dry") else "LIVE"
                    lines.append(f"[{tag}] {_short(mint)}  {pnl:+.1f}%")
                await tg_send(session, "\n".join(lines))
            elif text.startswith("/sell "):
                mint = text.split()[1]
                await close_position(session, mint, "manual /sell")


# ------------------------------------------------------------- main loop ---
async def main() -> None:
    load_state()
    async with aiohttp.ClientSession() as session:
        mode = "DRY RUN (no real trades)" if DRY_RUN else "LIVE TRADING"
        await tg_send(session, f"🤖 Copybot online — <b>{mode}</b>\n"
                               f"Tracking {len(TRACKED_WALLETS)} wallet(s)")
        asyncio.create_task(telegram_listener(session))
        asyncio.create_task(monitor_positions(session))

        while True:                                # websocket reconnect loop
            try:
                async with websockets.connect(HELIUS_WS, ping_interval=20) as ws:
                    # One logsSubscribe per tracked wallet.
                    for i, w in enumerate(TRACKED_WALLETS):
                        await ws.send(json.dumps({
                            "jsonrpc": "2.0", "id": i + 1,
                            "method": "logsSubscribe",
                            "params": [{"mentions": [w]},
                                       {"commitment": "confirmed"}],
                        }))
                    # Map Helius subscription id -> wallet address.
                    sub_to_wallet: dict[int, str] = {}
                    print("Subscribed. Watching wallets…")
                    async for raw in ws:
                        msg = json.loads(raw)
                        if "result" in msg and isinstance(msg.get("id"), int):
                            sub_to_wallet[msg["result"]] = TRACKED_WALLETS[msg["id"] - 1]
                            continue
                        if msg.get("method") != "logsNotification":
                            continue
                        p = msg["params"]
                        whale = sub_to_wallet.get(p["subscription"])
                        val = p["result"]["value"]
                        if whale and not val.get("err"):
                            asyncio.create_task(
                                handle_whale_tx(session, whale, val["signature"]))
            except Exception as e:
                print("WS dropped, reconnecting in 5s:", e)
                await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
