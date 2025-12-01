#!/usr/bin/env python3
import time
import requests
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import traceback

# ============================= CONFIG =============================
TELEGRAM_TOKEN = "8589870096:AAHahTpg6LNXbUwUMdt3q2EqVa2McIo14h8"
TELEGRAM_CHAT_IDS = ["5054484162", "497819952"]

SCAN_THRESHOLD = 0.25
ALERT_THRESHOLD = 5.0
ALERT_COOLDOWN = 60            # back to 60 as you asked
SUMMARY_INTERVAL = 300
MAX_WORKERS = 4                # reduced to save CPU
MONITOR_DURATION = 60
MONITOR_POLL = 5               # reduced API calls
CONFIRM_RETRY_DELAY = 0.5
CONFIRM_RETRIES = 1            # fewer confirmation calls
SYMBOL_REFRESH_INTERVAL = 900  # 15 minutes
# ==================================================================

# API endpoints
BINANCE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_BOOK_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
BINANCE_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={symbol}"
KUCOIN_ACTIVE_URL = "https://api-futures.kucoin.com/api/v1/contracts/active"
KUCOIN_TICKER_URL = "https://api-futures.kucoin.com/api/v1/ticker?symbol={symbol}"

# -------------------- Logging setup --------------------
logger = logging.getLogger("arb_monitor")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
ch.setFormatter(ch_formatter)
logger.addHandler(ch)

fh = RotatingFileHandler("arb_bot.log", maxBytes=5_000_000, backupCount=5)
fh.setLevel(logging.DEBUG)
fh_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)

def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# -------------------- Telegram helper --------------------
def send_telegram(message):
    for chat_id in TELEGRAM_CHAT_IDS:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            resp = requests.get(url, params={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }, timeout=10)
            if resp.status_code != 200:
                logger.warning("Telegram non-200 response: %s %s", resp.status_code, resp.text[:200])
        except Exception:
            logger.exception("Failed to send Telegram message")


# -------------------- Utility functions --------------------
def normalize(sym):
    if not sym:
        return sym
    s = sym.upper()
    if s.endswith("USDTM"): return s[:-1]
    if s.endswith("USDTP"): return s[:-1]
    if s.endswith("M"): return s[:-1]
    return s


def get_binance_symbols(retries=2):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(BINANCE_INFO_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            syms = [s["symbol"] for s in data.get("symbols", [])
                    if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"]
            return syms
        except:
            if attempt == retries:
                return []
            time.sleep(0.7)


def get_kucoin_symbols(retries=2):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(KUCOIN_ACTIVE_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            raw = data.get("data", []) if isinstance(data, dict) else []
            syms = [s["symbol"] for s in raw if s.get("status", "").lower() == "open"]
            return syms
        except:
            if attempt == retries:
                return []
            time.sleep(0.7)


def get_common_symbols():
    bin_syms = get_binance_symbols()
    ku_syms = get_kucoin_symbols()

    bin_set = {normalize(s) for s in bin_syms}
    ku_set = {normalize(s) for s in ku_syms}
    common = bin_set.intersection(ku_set)

    ku_map = {}
    for s in ku_syms:
        n = normalize(s)
        if n not in ku_map:
            ku_map[n] = s

    logger.info("Common symbols: %d (sample: %s)", len(common), list(common)[:8])
    return common, ku_map


def get_binance_book(retries=1):
    for attempt in range(1, retries+1):
        try:
            r = requests.get(BINANCE_BOOK_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            out = {d["symbol"]: {"bid": float(d["bidPrice"]), "ask": float(d["askPrice"])}
                   for d in data if "symbol" in d}
            return out
        except:
            if attempt == retries:
                return {}
            time.sleep(0.5)


def get_binance_price(symbol, session, retries=1):
    for attempt in range(1, retries+1):
        try:
            url = BINANCE_TICKER_URL.format(symbol=symbol)
            r = session.get(url, timeout=6)
            if r.status_code != 200:
                return None, None
            d = r.json()
            bid = float(d.get("bidPrice") or 0)
            ask = float(d.get("askPrice") or 0)
            if bid <= 0 or ask <= 0:
                return None, None
            return bid, ask
        except:
            if attempt == retries:
                return None, None
            time.sleep(0.2)


def get_kucoin_price_once(symbol, session, retries=1):
    for attempt in range(1, retries+1):
        try:
            url = KUCOIN_TICKER_URL.format(symbol=symbol)
            r = session.get(url, timeout=6)
            if r.status_code != 200:
                return None, None
            data = r.json()
            d = data.get("data", {})
            bid = float(d.get("bestBidPrice") or d.get("bid") or 0)
            ask = float(d.get("bestAskPrice") or d.get("ask") or 0)
            if bid <= 0 or ask <= 0:
                return None, None
            return bid, ask
        except:
            if attempt == retries:
                return None, None
            time.sleep(0.2)


def threaded_kucoin_prices(symbols):
    prices = {}
    if not symbols:
        return prices
    workers = min(MAX_WORKERS, max(4, len(symbols)))
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(get_kucoin_price_once, s, session): s for s in symbols}
            for fut in as_completed(futures):
                s = futures[fut]
                try:
                    bid, ask = fut.result()
                    if bid and ask:
                        prices[s] = {"bid": bid, "ask": ask}
                except:
                    pass
    return prices


# -------------------- Spread calculation --------------------
def calculate_spread(bin_bid, bin_ask, ku_bid, ku_ask):
    try:
        if not all([bin_bid, bin_ask, ku_bid, ku_ask]) or bin_ask <= 0:
            return None
        pos = ((ku_bid - bin_ask) / bin_ask) * 100
        neg = ((ku_ask - bin_bid) / bin_bid) * 100
        if pos > 0.01:
            return pos
        if neg < -0.01:
            return neg
        return None
    except:
        return None


# -------------------- Main Loop --------------------
def main():
    logger.info("Binance <-> KuCoin Monitor STARTED - %s", timestamp())
    send_telegram("Bot started — monitoring spreads. Instant alerts enabled.")

    last_alert = {}
    heartbeat_counter = 0
    http_session = requests.Session()

    last_symbol_refresh = 0
    common_symbols, ku_map = get_common_symbols()  # initial load

    while True:
        window_start = time.time()

        try:
            # 🔄 Refresh symbol list every 15 minutes
            if time.time() - last_symbol_refresh > SYMBOL_REFRESH_INTERVAL:
                common_symbols, ku_map = get_common_symbols()
                last_symbol_refresh = time.time()

            # 1) Full scan
            bin_book = get_binance_book()
            ku_symbols = [ku_map.get(sym, sym + "M") for sym in common_symbols]
            ku_prices = threaded_kucoin_prices(ku_symbols)

            candidates = {}
            for sym in common_symbols:
                bin_tick = bin_book.get(sym)
                ku_sym = ku_map.get(sym, sym + "M")
                ku_tick = ku_prices.get(ku_sym)
                if not bin_tick or not ku_tick:
                    continue
                spread = calculate_spread(bin_tick["bid"], bin_tick["ask"], ku_tick["bid"], ku_tick["ask"])
                if spread is not None and abs(spread) >= SCAN_THRESHOLD:
                    candidates[sym] = {
                        "ku_sym": ku_sym,
                        "start_spread": spread,
                        "max_spread": spread,
                        "min_spread": spread,
                        "alerted": False
                    }

            if not candidates:
                time.sleep(max(1, MONITOR_DURATION - (time.time() - window_start)))
                continue

            # 2) Focused monitoring for 60s
            window_end = window_start + MONITOR_DURATION
            while time.time() < window_end and candidates:
                round_start = time.time()
                workers = min(MAX_WORKERS, max(4, len(candidates)))
                latest = {s: {"bin": None, "ku": None} for s in candidates}

                # parallel fetch prices
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    fut_map = {}
                    for sym, info in candidates.items():
                        ku_sym = info["ku_sym"]
                        fut_map[
                            ex.submit(get_binance_price, sym, http_session)
                        ] = ("bin", sym)
                        fut_map[
                            ex.submit(get_kucoin_price_once, ku_sym, http_session)
                        ] = ("ku", sym)

                    for fut in as_completed(fut_map):
                        typ, sym = fut_map[fut]
                        try:
                            bid, ask = fut.result()
                            if bid and ask:
                                latest[sym][typ] = {"bid": bid, "ask": ask}
                        except:
                            pass

                # evaluate spreads
                for sym in list(candidates.keys()):
                    info = candidates[sym]
                    b = latest[sym].get("bin")
                    k = latest[sym].get("ku")
                    if not b or not k:
                        continue

                    spread = calculate_spread(b["bid"], b["ask"], k["bid"], k["ask"])
                    if spread is None:
                        continue

                    # track min/max
                    if spread > info["max_spread"]:
                        info["max_spread"] = spread
                    if spread < info["min_spread"]:
                        info["min_spread"] = spread

                    # alert logic
                    if abs(spread) >= ALERT_THRESHOLD:
                        now = time.time()
                        if sym in last_alert and now - last_alert[sym] < ALERT_COOLDOWN:
                            continue

                        confirmed = False
                        for attempt in range(CONFIRM_RETRIES):
                            time.sleep(CONFIRM_RETRY_DELAY)
                            b2_bid, b2_ask = get_binance_price(sym, http_session)
                            k2_bid, k2_ask = get_kucoin_price_once(info["ku_sym"], http_session)
                            if b2_bid and b2_ask and k2_bid and k2_ask:
                                spread2 = calculate_spread(b2_bid, b2_ask, k2_bid, k2_ask)
                                if spread2 is not None and abs(spread2) >= ALERT_THRESHOLD:
                                    confirmed = True
                                    b_confirm, k_confirm = {"bid": b2_bid, "ask": b2_ask}, {"bid": k2_bid, "ask": k2_ask}
                                    break

                        if not confirmed:
                            continue

                        direction = "Long Binance / Short KuCoin" if spread2 > 0 else "Long KuCoin / Short Binance"

                        msg = (
                            f"*BIG SPREAD ALERT*\n"
                            f"`{sym}` → *{spread2:+.4f}%*\n"
                            f"Direction → {direction}\n"
                            f"Binance: `{b_confirm['bid']:.6f}` ↔ `{b_confirm['ask']:.6f}`\n"
                            f"KuCoin : `{k_confirm['bid']:.6f}` ↔ `{k_confirm['ask']:.6f}`\n"
                            f"{timestamp()}"
                        )
                        send_telegram(msg)
                        last_alert[sym] = time.time()
                        candidates.pop(sym, None)

                # control polling speed
                elapsed = time.time() - round_start
                sleep_for = MONITOR_POLL - elapsed
                if sleep_for > 0:
                    time.sleep(sleep_for)

            heartbeat_counter += 1
            if heartbeat_counter % 20 == 0:
                logger.info("Bot alive — %s", timestamp())

        except Exception:
            logger.exception("Fatal error in main loop")
            time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down.")
    except Exception:
        logger.exception("Unhandled exception at top level")
