import os
import pandas as pd
import yfinance as yf
from datetime import datetime
from zoneinfo import ZoneInfo
import smtplib
from email.message import EmailMessage
from html import escape

# ---------------- CONFIG ----------------
HOLDINGS_FILE = os.environ.get("HOLDINGS_FILE", "holdings-RM6481.xlsx")

SMTP_HOST = os.environ["SMTP_HOST"]
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ["SMTP_USER"]
SMTP_PASS = os.environ["SMTP_PASS"]
MAIL_TO = os.environ["MAIL_TO"]
MAIL_FROM = os.environ.get("MAIL_FROM", SMTP_USER)

TOP_N = int(os.environ.get("TOP_N", "10"))
PANIC_STREAK_DAYS = int(os.environ.get("PANIC_STREAK_DAYS", "7"))
HIST_DAYS = 120

IST = ZoneInfo("Asia/Kolkata")

ALIASES = {
    "SILVERBEE": "SILVERBEES",
    "GOLDBEE": "GOLDBEES",
}

GROWTH_SECTORS = [
    "DEFENCE", "RAIL", "INFRA", "POWER", "RENEWABLE",
    "CAPITAL GOODS", "MANUFACTURING", "EV", "INSURANCE"
]
# ---------------------------------------


def normalize_symbol(sym):
    s = str(sym).upper().strip()
    for suf in ["-EQ", "-BE"]:
        if s.endswith(suf):
            s = s[:-len(suf)]
    return ALIASES.get(s, s)


def fetch_price(symbol):
    for tk in [f"{symbol}.NS", f"{symbol}.BO"]:
        try:
            t = yf.Ticker(tk)
            hist = t.history(period=f"{HIST_DAYS}d")
            if hist is not None and len(hist) > 10:
                return tk, hist
        except Exception:
            pass
    return None, None


def down_streak(closes):
    streak = 0
    for i in range(len(closes)-1, 0, -1):
        if closes.iloc[i] < closes.iloc[i-1]:
            streak += 1
        else:
            break
    return streak


def get_fundamentals(ticker):
    info = yf.Ticker(ticker).info
    return {
        "roe": info.get("returnOnEquity"),
        "debt": info.get("debtToEquity"),
        "earnings_growth": info.get("earningsGrowth"),
        "revenue_growth": info.get("revenueGrowth"),
        "sector": str(info.get("sector", "")).upper()
    }


def panic_score(hist):
    closes = hist["Close"]
    streak = down_streak(closes)
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


def growth_score(f):
    score = 0
    if f["earnings_growth"] and f["earnings_growth"] > 0.15:
        score += 2
    if f["revenue_growth"] and f["revenue_growth"] > 0.10:
        score += 1
    if f["roe"] and f["roe"] > 0.15:
        score += 1
    if f["debt"] is not None and f["debt"] < 1:
        score += 1
    if any(k in f["sector"] for k in GROWTH_SECTORS):
        score += 1
    return score


def classify(panic, growth):
    if panic >= 2 and growth >= 3:
        return "🔥 STRONG ADD", "30–40%"
    if panic >= 2:
        return "📉 PANIC ADD", "20–30%"
    if growth >= 3:
        return "🚀 GROWTH ADD", "20–30%"
    return "⏸️ WAIT", "0–10%"


def send_email(subject, html):
    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    msg["Subject"] = subject
    msg.set_content("Please view this email in HTML format.")
    msg.add_alternative(html, subtype="html")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)


def df_to_html(df, title):
    if df.empty:
        return f"<h3>{title}</h3><p>No stocks.</p>"

    headers = "".join(f"<th>{escape(c)}</th>" for c in df.columns)
    rows = ""
    for _, r in df.iterrows():
        rows += "<tr>" + "".join(f"<td>{escape(str(v))}</td>" for v in r) + "</tr>"

    return f"""
    <h3>{title}</h3>
    <table border="1" cellspacing="0" cellpadding="6" style="border-collapse:collapse;font-size:13px;">
      <thead><tr>{headers}</tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def main():
    holdings = pd.read_excel(HOLDINGS_FILE)
    holdings["Symbol"] = holdings["Symbol"].apply(normalize_symbol)

    rows = []

    for sym in holdings["Symbol"]:
        tk, hist = fetch_price(sym)
        if hist is None:
            continue

        panic, streak, dd = panic_score(hist)
        f = get_fundamentals(tk)
        growth = growth_score(f)
        action, alloc = classify(panic, growth)

        rows.append({
            "Symbol": sym,
            "Down Streak": streak,
            "Drawdown %": dd,
            "Panic": panic,
            "Growth": growth,
            "Action": action,
            "Allocation": alloc
        })

    df = pd.DataFrame(rows)

    strong = df[df["Action"].str.contains("STRONG")]
    panic_only = df[df["Action"].str.contains("PANIC")]
    growth_only = df[df["Action"].str.contains("GROWTH")]

    now = datetime.now(IST)
    subject = f"Investment Opportunities – {now:%Y-%m-%d}"

    html = f"""
    <p><b>{escape(subject)}</b></p>
    {df_to_html(strong, "🔥 STRONG ADD (Panic + Growth)")}
    {df_to_html(panic_only, "📉 PANIC ADD (Price-led)")}
    {df_to_html(growth_only, "🚀 GROWTH ADD (Future-led)")}
    """

    send_email(subject, html)
    print("Email sent successfully.")


if __name__ == "__main__":
    main()
