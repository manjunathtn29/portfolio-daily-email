import pandas as pd
import yfinance as yf
from datetime import datetime
from zoneinfo import ZoneInfo

# ---------------- CONFIG ----------------
HOLDINGS_FILE = "holdings-RM6481.xlsx"   # your current holdings file
HIST_DAYS = 120
PANIC_STREAK_DAYS = 7

IST = ZoneInfo("Asia/Kolkata")

# Sector keywords used as govt/structural growth proxies
GROWTH_SECTORS = [
    "DEFENCE", "RAIL", "INFRA", "POWER", "RENEWABLE",
    "CAPITAL GOODS", "MANUFACTURING", "EV", "INSURANCE"
]

ALIASES = {
    "SILVERBEE": "SILVERBEES",
    "GOLDBEE": "GOLDBEES"
}
# ---------------------------------------


def normalize_symbol(sym):
    s = str(sym).upper().strip()
    for suf in ["-EQ", "-BE"]:
        if s.endswith(suf):
            s = s[:-len(suf)]
    return ALIASES.get(s, s)


def fetch_price_data(symbol):
    for tk in [f"{symbol}.NS", f"{symbol}.BO"]:
        try:
            t = yf.Ticker(tk)
            hist = t.history(period=f"{HIST_DAYS}d")
            if hist is not None and len(hist) > 10:
                return tk, hist
        except Exception:
            pass
    return None, None


def compute_down_streak(closes):
    down = 0
    for i in range(len(closes)-1, 0, -1):
        if closes.iloc[i] < closes.iloc[i-1]:
            down += 1
        else:
            break
    return down


def get_fundamentals(ticker):
    info = yf.Ticker(ticker).info
    return {
        "roe": info.get("returnOnEquity"),
        "debt_to_equity": info.get("debtToEquity"),
        "earnings_growth": info.get("earningsGrowth"),
        "revenue_growth": info.get("revenueGrowth"),
        "sector": str(info.get("sector", "")).upper()
    }


def score_panic(hist):
    closes = hist["Close"]
    streak = compute_down_streak(closes)
    high = closes.max()
    last = closes.iloc[-1]
    drawdown = (high - last) / high if high else 0

    score = 0
    if streak >= PANIC_STREAK_DAYS:
        score += 2
    if drawdown >= 0.20:
        score += 2
    elif drawdown >= 0.10:
        score += 1

    return score, streak, round(drawdown * 100, 2)


def score_growth(f):
    score = 0
    if f["earnings_growth"] and f["earnings_growth"] > 0.15:
        score += 2
    if f["revenue_growth"] and f["revenue_growth"] > 0.10:
        score += 1
    if f["roe"] and f["roe"] > 0.15:
        score += 1
    if f["debt_to_equity"] is not None and f["debt_to_equity"] < 1:
        score += 1
    if any(k in f["sector"] for k in GROWTH_SECTORS):
        score += 1

    return score


def classify(panic_score, growth_score):
    if panic_score >= 2 and growth_score >= 3:
        return "🔥 STRONG ADD", "30–40%"
    if panic_score >= 2:
        return "📉 PANIC ADD", "20–30%"
    if growth_score >= 3:
        return "🚀 GROWTH ADD", "20–30%"
    return "⏸️ WAIT", "0–10%"


def main():
    df = pd.read_excel(HOLDINGS_FILE)
    df["Symbol"] = df["Symbol"].apply(normalize_symbol)

    rows = []

    for sym in df["Symbol"]:
        tk, hist = fetch_price_data(sym)
        if hist is None:
            continue

        panic_score, streak, drawdown = score_panic(hist)
        fundamentals = get_fundamentals(tk)
        growth_score = score_growth(fundamentals)

        action, alloc = classify(panic_score, growth_score)

        rows.append({
            "Symbol": sym,
            "Yahoo": tk,
            "Down Streak": streak,
            "Drawdown %": drawdown,
            "Panic Score": panic_score,
            "Growth Score": growth_score,
            "Action": action,
            "Suggested Allocation": alloc
        })

    result = pd.DataFrame(rows).sort_values(
        ["Action", "Panic Score", "Growth Score"],
        ascending=[True, False, False]
    )

    today = datetime.now(IST).strftime("%Y-%m-%d")
    out_file = f"investment_opportunities_{today}.csv"
    result.to_csv(out_file, index=False)

    print("\n=== INVESTMENT CANDIDATES ===")
    print(result.to_string(index=False))
    print(f"\nSaved to: {out_file}")


if __name__ == "__main__":
    main()
