# portfolio_allocator.py
#
# What it does:
# - Reads your approved stock universe from an Excel file (Top 60.xlsx by default)
# - Scores each stock as:
#     1) Panic Opportunity (price-led)
#     2) Growth Opportunity (business-led)
#   and recommends stocks if (Panic OR Growth) is true
# - Sends an EMAIL ONLY (no file attachments, no CSV output)
#
# Required Excel format:
# - Top 60.xlsx must have a column named: Symbol
#   Example:
#     Symbol
#     HDFCBANK
#     ICICIBANK
#     TCS
#     ...
#
# Required environment variables (GitHub Secrets):
# - SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, MAIL_TO
# Optional:
# - MAIL_FROM (defaults to SMTP_USER)
# - TOP_N (default 10)
# - PANIC_STREAK_DAYS (default 7)
# - UNIVERSE_FILE (default "Top 60.xlsx")
# - HOLDINGS_FILE (not used)

import os
import pandas as pd
import yfinance as yf
from datetime import datetime
from zoneinfo import ZoneInfo
import smtplib
from email.message import EmailMessage
from html import escape

# ---------------- CONFIG ----------------
UNIVERSE_FILE = os.environ.get("UNIVERSE_FILE", "Top 60.xlsx")

SMTP_HOST = os.environ["SMTP_HOST"]
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ["SMTP_USER"]
SMTP_PASS = os.environ["SMTP_PASS"]
MAIL_TO = os.environ["MAIL_TO"]
MAIL_FROM = os.environ.get("MAIL_FROM", SMTP_USER)

TOP_N = int(os.environ.get("TOP_N", "10"))
PANIC_STREAK_DAYS = int(os.environ.get("PANIC_STREAK_DAYS", "7"))

HIST_DAYS = int(os.environ.get("HIST_DAYS", "120"))
IST = ZoneInfo("Asia/Kolkata")

# Fix common symbol issues
ALIASES = {
    "SILVERBEE": "SILVERBEES",
    "GOLDBEE": "GOLDBEES",
}

# Simple govt/structural growth proxies (optional heuristic)
GROWTH_SECTOR_KEYWORDS = [
    "DEFENCE", "RAIL", "INFRA", "POWER", "RENEWABLE",
    "CAPITAL GOODS", "MANUFACTURING", "INSURANCE"
]
# ---------------------------------------


def normalize_symbol(sym: str) -> str:
    s = str(sym).strip().upper()

    # Remove common Zerodha suffixes / noise
    for suf in ["-EQ", "-BE", "-BZ", "-BL", "-SM"]:
        if s.endswith(suf):
            s = s[: -len(suf)]

    # Remove exchange prefixes
    if s.startswith("NSE:"):
        s = s[4:]
    if s.startswith("BSE:"):
        s = s[4:]

    s = s.replace(" ", "")
    s = ALIASES.get(s, s)
    return s


def read_universe(path: str) -> list[str]:
    df = pd.read_excel(path)

    # Drop completely empty columns
    df = df.dropna(axis=1, how="all")

    # Normalize column names for matching
    col_map = {c.lower().strip(): c for c in df.columns}

    # Try common column names
    preferred_cols = [
        "symbol",
        "stock",
        "stock name",
        "name",
        "ticker",
    ]

    symbol_col = None
    for key in preferred_cols:
        if key in col_map:
            symbol_col = col_map[key]
            break

    # If still not found, fall back to first column
    if symbol_col is None:
        symbol_col = df.columns[0]
        print(f"[INFO] Using first column '{symbol_col}' as universe symbols")

    symbols = (
        df[symbol_col]
        .dropna()
        .astype(str)
        .apply(normalize_symbol)
        .unique()
        .tolist()
    )

    symbols = [s for s in symbols if s and s.upper() != "NAN"]

    if not symbols:
        raise ValueError("No valid symbols found in universe file.")

    return symbols

    # Remove blanks / weirdness
    syms = [s for s in syms if s and s != "NAN"]
    return syms


def try_fetch_history(ticker: str) -> pd.DataFrame | None:
    try:
        t = yf.Ticker(ticker)
        h = t.history(period=f"{HIST_DAYS}d", interval="1d")
        if h is not None and (not h.empty) and "Close" in h.columns and h["Close"].dropna().shape[0] >= 15:
            return h
    except Exception:
        return None
    return None


def resolve_yahoo_ticker(symbol: str) -> str | None:
    """
    Resolves to NSE/BSE Yahoo ticker. Prefers NSE.
    """
    base = normalize_symbol(symbol)
    candidates = []

    # If already has suffix
    if base.endswith(".NS") or base.endswith(".BO"):
        candidates.append(base)
        base2 = base[:-3]
        candidates.extend([f"{base2}.NS", f"{base2}.BO"])
    else:
        candidates.extend([f"{base}.NS", f"{base}.BO"])

    for tk in candidates:
        h = try_fetch_history(tk)
        if h is not None:
            return tk
    return None


def compute_down_streak(closes: pd.Series) -> int:
    closes = closes.dropna()
    if len(closes) < 2:
        return 0
    streak = 0
    for i in range(len(closes) - 1, 0, -1):
        if closes.iloc[i] < closes.iloc[i - 1]:
            streak += 1
        else:
            break
    return streak


def compute_drawdown_pct(closes: pd.Series) -> float:
    closes = closes.dropna()
    if closes.empty:
        return 0.0
    peak = float(closes.max())
    last = float(closes.iloc[-1])
    if peak <= 0:
        return 0.0
    return (peak - last) / peak * 100.0


def get_fundamentals(yahoo_ticker: str) -> dict:
    """
    Uses Yahoo info. Some fields may be missing for Indian stocks.
    """
    try:
        info = yf.Ticker(yahoo_ticker).info or {}
    except Exception:
        info = {}

    sector = str(info.get("sector", "")).upper()
    return {
        "sector": sector,
        "roe": info.get("returnOnEquity"),        # decimal (0.15 => 15%)
        "earnings_growth": info.get("earningsGrowth"),  # decimal
        "revenue_growth": info.get("revenueGrowth"),    # decimal
        "debt_to_equity": info.get("debtToEquity"),     # sometimes ratio or %
        "quoteType": str(info.get("quoteType", "")).upper(),
    }


def is_etf_like(symbol: str, fundamentals: dict) -> bool:
    qt = fundamentals.get("quoteType", "")
    s = symbol.upper()
    if "ETF" in qt:
        return True
    # Your universe may include ETFs like MON100/MASPTOP50; exclude them from “stock growth” scoring
    if s.endswith("BEES") or s in {"MON100", "MASPTOP50"}:
        return True
    return False


def score_panic(hist: pd.DataFrame) -> tuple[int, int, float]:
    closes = hist["Close"].dropna()
    streak = compute_down_streak(closes)
    dd = compute_drawdown_pct(closes)

    score = 0
    # Streak-based panic
    if streak >= PANIC_STREAK_DAYS:
        score += 2
    elif streak >= max(3, PANIC_STREAK_DAYS // 2):
        score += 1

    # Drawdown-based panic
    if dd >= 20:
        score += 2
    elif dd >= 10:
        score += 1

    return score, streak, round(dd, 2)


def score_growth(f: dict, etf: bool) -> int:
    """
    Growth score for stocks (ETFs excluded from growth scoring).
    """
    if etf:
        return 0

    score = 0
    eg = f.get("earnings_growth")
    rg = f.get("revenue_growth")
    roe = f.get("roe")
    de = f.get("debt_to_equity")
    sector = f.get("sector", "")

    # Growth (heuristics)
    if isinstance(eg, (int, float)) and eg > 0.15:
        score += 2
    elif isinstance(eg, (int, float)) and eg > 0.05:
        score += 1

    if isinstance(rg, (int, float)) and rg > 0.10:
        score += 1

    # Quality
    if isinstance(roe, (int, float)) and roe > 0.15:
        score += 1

    # Health (lower debt better)
    # debtToEquity sometimes comes as percentage. We'll treat < 1 or < 100 as "ok-ish".
    if isinstance(de, (int, float)):
        if de <= 1:
            score += 1
        elif de <= 100:
            score += 1

    # Sector proxy for govt/structural support
    if any(k in sector for k in GROWTH_SECTOR_KEYWORDS):
        score += 1

    return score


def classify(panic_score: int, growth_score: int) -> tuple[str, str]:
    """
    Your rule: recommend if (panic OR growth).
    Labels are for convenience in the email.
    """
    if panic_score >= 2 and growth_score >= 3:
        return "🔥 STRONG ADD", "30–40%"
    if panic_score >= 2:
        return "📉 PANIC ADD", "20–30%"
    if growth_score >= 3:
        return "🚀 GROWTH ADD", "20–30%"
    return "⏸️ WAIT", "0–10%"


def df_to_html_table(df: pd.DataFrame, title: str) -> str:
    if df.empty:
        return f"<h3>{escape(title)}</h3><p>No stocks in this bucket.</p>"

    # Simple HTML table
    cols = list(df.columns)
    th = "".join(f"<th style='text-align:left;border:1px solid #ccc;padding:6px'>{escape(c)}</th>" for c in cols)
    trs = []
    for _, r in df.iterrows():
        tds = "".join(
            f"<td style='border:1px solid #ccc;padding:6px'>{escape(str(r[c]))}</td>"
            for c in cols
        )
        trs.append(f"<tr>{tds}</tr>")

    return f"""
    <h3>{escape(title)}</h3>
    <table style="border-collapse:collapse;font-family:Arial;font-size:13px">
      <thead><tr>{th}</tr></thead>
      <tbody>{''.join(trs)}</tbody>
    </table>
    """


def send_email(subject: str, html_body: str):
    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    msg["Subject"] = subject
    msg.set_content("Please view this email in HTML format.")
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)


def main():
    now = datetime.now(IST)
    run_date = now.strftime("%Y-%m-%d")

    symbols = read_universe(UNIVERSE_FILE)

    rows = []
    unresolved = []

    for sym in symbols:
        yahoo = resolve_yahoo_ticker(sym)
        if not yahoo:
            unresolved.append(sym)
            continue

        hist = try_fetch_history(yahoo)
        if hist is None:
            unresolved.append(sym)
            continue

        f = get_fundamentals(yahoo)
        etf = is_etf_like(sym, f)

        p_score, streak, dd = score_panic(hist)
        g_score = score_growth(f, etf)

        action, alloc = classify(p_score, g_score)

        rows.append({
            "Symbol": sym,
            "Yahoo": yahoo,
            "Down Streak": streak,
            "Drawdown %": f"{dd:.2f}",
            "Panic": p_score,
            "Growth": g_score,
            "Action": action,
            "Allocation": alloc,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        subject = f"Opportunity Allocator – {run_date} (No Data)"
        html = f"""
        <div style="font-family:Arial">
          <p><b>{escape(subject)}</b></p>
          <p>No stocks could be processed. Check ticker resolution / Yahoo availability.</p>
          <p>Unresolved symbols count: {len(unresolved)}</p>
          <pre style="white-space:pre-wrap">{escape(', '.join(unresolved[:200]))}</pre>
        </div>
        """
        send_email(subject, html)
        print("Email sent (no data).")
        return

    # Buckets
    strong = df[df["Action"].str.contains("STRONG")].copy()
    panic_only = df[(df["Action"].str.contains("PANIC")) & (~df["Action"].str.contains("STRONG"))].copy()
    growth_only = df[df["Action"].str.contains("GROWTH")].copy()
    wait = df[df["Action"].str.contains("WAIT")].copy()

    # Sort each bucket nicely
    def sort_bucket(d: pd.DataFrame) -> pd.DataFrame:
        if d.empty:
            return d
        d2 = d.copy()
        # Convert numeric-like strings
        d2["Drawdown %"] = pd.to_numeric(d2["Drawdown %"], errors="coerce")
        return d2.sort_values(["Panic", "Growth", "Drawdown %"], ascending=[False, False, False])

    strong = sort_bucket(strong).head(TOP_N)
    panic_only = sort_bucket(panic_only).head(TOP_N)
    growth_only = sort_bucket(growth_only).head(TOP_N)
    wait = sort_bucket(wait).head(TOP_N)

    subject = f"Investment Opportunities (Universe) – {run_date}"

    html = f"""
    <div style="font-family:Arial">
      <p><b>{escape(subject)}</b></p>
      <p style="color:#555">
        Rule: Recommend if <b>Panic OR Growth</b>. Use allocation ranges as guidance. Not financial advice.
      </p>

      {df_to_html_table(strong, "🔥 STRONG ADD (Panic + Growth) — Top picks")}
      {df_to_html_table(panic_only, "📉 PANIC ADD (Price-led) — Top picks")}
      {df_to_html_table(growth_only, "🚀 GROWTH ADD (Future-led) — Top picks")}
      {df_to_html_table(wait, "⏸️ WAIT — Top picks (for awareness)")}

      <h3>Diagnostics</h3>
      <p>Total processed: {len(df)} / Universe size: {len(symbols)} | Unresolved: {len(unresolved)}</p>
      {"<pre style='white-space:pre-wrap'>" + escape(", ".join(unresolved[:200])) + "</pre>" if unresolved else "<p>No unresolved symbols.</p>"}
    </div>
    """

    send_email(subject, html)
    print("Email sent successfully.")


if __name__ == "__main__":
    main()
