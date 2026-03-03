import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import math

# =====================
# SETTINGS
# =====================
SYMBOL = "BTCUSDT"
INTERVAL = "5m"
API_URL = "https://api.binance.com/api/v3/klines"

DAYS_BACK = 365

# Fixed UTC times (no DST handling)
US_PREMARKET_OPEN_UTC = "09:00"
US_MARKET_OPEN_UTC = "14:30"
US_MARKET_CLOSE_UTC = "21:00"
US_POSTMARKET_CLOSE_UTC = "01:00"  # next day

# =====================
# UTILITIES
# =====================
def to_ms(dt):
    return int(dt.timestamp() * 1000)

def download_klines(symbol, interval, start_ms, end_ms):
    rows = []
    limit = 1000
    current = start_ms

    while current < end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current,
            "endTime": end_ms,
            "limit": limit
        }
        r = requests.get(API_URL, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        rows.extend(data)
        current = data[-1][0] + 300000

    df = pd.DataFrame(rows, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","qav","num_trades","taker_base",
        "taker_quote","ignore"
    ])

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for c in ["open","close"]:
        df[c] = pd.to_numeric(df[c])

    return df.set_index("open_time").sort_index()

def generate_us_events(start_date, end_date):
    events = []
    current = start_date

    while current <= end_date:
        if current.weekday() < 5:
            base = current.strftime("%Y-%m-%d")

            premarket = datetime.strptime(base + " " + US_PREMARKET_OPEN_UTC, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            open_time = datetime.strptime(base + " " + US_MARKET_OPEN_UTC, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            close_time = datetime.strptime(base + " " + US_MARKET_CLOSE_UTC, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)

            postmarket = datetime.strptime(base + " " + US_POSTMARKET_CLOSE_UTC, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            postmarket += timedelta(days=1)

            events.append(("premarket_open", premarket))
            events.append(("market_open", open_time))
            events.append(("market_close", close_time))
            events.append(("postmarket_close", postmarket))

        current += timedelta(days=1)

    return events

def analyze_strategy(df, label):
    if len(df) == 0:
        print(f"\n{label}: No data")
        return

    n = len(df)
    up = (df["direction"] > 0).sum()
    down = (df["direction"] < 0).sum()

    winrate_up = up / n
    winrate_down = down / n

    avg_return = df["return"].mean()
    avg_abs = df["abs_return"].mean()

    # Z-score for direction bias vs 50%
    se = math.sqrt(0.5 * 0.5 / n)
    z = (winrate_up - 0.5) / se

    print(f"\n=== {label.upper()} ===")
    print("Events:", n)
    print("Up moves:", up)
    print("Down moves:", down)
    print("Winrate Up:", round(winrate_up, 4))
    print("Winrate Down:", round(winrate_down, 4))
    print("Average return:", round(avg_return, 6))
    print("Average |return|:", round(avg_abs, 6))
    print("Direction Z-score:", round(z, 2))

# =====================
# MAIN
# =====================
end_date = datetime.now(timezone.utc)
start_date = end_date - timedelta(days=DAYS_BACK)

print("Downloading BTC 5m data...")
df = download_klines(
    SYMBOL,
    INTERVAL,
    to_ms(start_date),
    to_ms(end_date)
)

print("Bars downloaded:", len(df))

events = generate_us_events(start_date.date(), end_date.date())

results = []

for label, ev_time in events:
    ev_bar = ev_time.replace(
        minute=ev_time.minute - ev_time.minute % 5,
        second=0,
        microsecond=0
    )

    if ev_bar not in df.index:
        continue

    entry_price = df.at[ev_bar, "open"]
    exit_time = ev_bar + timedelta(minutes=5)

    if exit_time not in df.index:
        continue

    exit_price = df.at[exit_time, "close"]
    ret = (exit_price / entry_price) - 1

    results.append({
        "event_type": label,
        "return": ret,
        "abs_return": abs(ret),
        "direction": np.sign(ret)
    })

res = pd.DataFrame(results)

# =====================
# STRATEGY CHECK
# =====================
analyze_strategy(res[res["event_type"] == "premarket_open"], "Premarket Open")
analyze_strategy(res[res["event_type"] == "market_open"], "Market Open")
analyze_strategy(res[res["event_type"] == "market_close"], "Market Close")
analyze_strategy(res[res["event_type"] == "postmarket_close"], "Postmarket Close")

res.to_csv("btc_us_session_direction_analysis.csv", index=False)
print("\nCSV saved: btc_us_session_direction_analysis.csv")

input("\nPress Enter to exit...")