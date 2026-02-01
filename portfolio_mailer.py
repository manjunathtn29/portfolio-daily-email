import os
import pandas as pd
import yfinance as yf
from datetime import datetime
from zoneinfo import ZoneInfo
import smtplib
from email.message import EmailMessage
from html import escape

# -------------------- CONFIG --------------------
INPUT_FILE = os.environ.get("HOLDINGS_FILE", "holdings-RM6481.xlsx")

IST = ZoneInfo("Asia/Kolkata")

SMTP_HOST = os.environ["SMTP_HOST"]
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ["SMTP_USER"]
SMTP_PASS = os.environ["SMTP_PASS"]
MAIL_TO = os.environ["MAIL_TO"]
MAIL_FROM = os.environ.get("MAIL_FROM", SMTP_USER)

TOP_N = int(os.environ.get("TOP_N", "10"))
RUN_LABEL = os.environ.get("RUN_LABEL", "RUN").strip().upper()

# Long-term investor rule: only care if down >= 7 consecutive trading days
ALERT_STREAK_DAYS = int(os.environ.get("ALERT_STREAK_DAYS", "7"))

# Enough history for streak detection
HIST_DAYS = int(os.environ.get("HIST_DAYS", "120"))

# Common symbol aliases / corrections
ALIASES = {
    "SILVERBEE": "SILVERBEES",
    "GOLDBEE": "GOLDBEES",
}
# ------------------------------------------------


def normalize_symbol(sym: str) -> str:
    s = str(sym).strip().upper()

    # Remove common Zerodha suffixes
    for suf in ["-EQ", "-BE", "-BZ", "-BL", "-SM"]:
        if s.endswith(suf):
            s = s[: -len(suf)]

    # Remove exchange prefixes
    if s.startswith("NSE:"):
        s = s[4:]
    if s.startswith("BSE:"):
        s = s[4:]

    s = s.replace(" ", "")

    # Apply alias
    s = ALIASES.get(s, s)

    return s


def safe_pct(numerator, denominator):
    if denominator is None or pd.isna(denominator) or float(denominator) == 0:
        return 0.0
    return float(numerator) / float(denominator) * 100.0


def compute_down_streak(closes: pd.Series) -> int:
    closes = closes.dropna()
    if len(closes) < 2:
        return 0

    down = 0
    for i in range(len(closes) - 1, 0, -1):
        if closes.iloc[i] < closes.iloc[i - 1]:
            down += 1
        else:
            break
    return down


def fetch_symbol_bundle(symbol_raw: str):
    """
    Returns: used_ticker, prev_close, today_price, closes_series
    Tries: base, base.NS, base.BO
    """
    base = normalize_symbol(symbol_raw)

    candidates = []
    if base.endswith(".NS") or base.endswith(".BO"):
        candidates.append(base)
        base2 = base[:-3]
        candidates.extend([f"{base2}.NS", f"{base2}.BO"])
    else:
        candidates.extend([base, f"{base}.NS", f"{base}.BO"])

    used = None
    hist = None

    for tk in candidates:
        try:
            t = yf.Ticker(tk)
            h = t.history(period=f"{HIST_DAYS}d", interval="1d")
            if h is not None and not h.empty and "Close" in h.columns and h["Close"].dropna().shape[0] >= 2:
                used = tk
                hist = h
                break
        except Exception:
            continue

    if used is None or hist is None:
        return None, None, None, None

    closes = hist["Close"].dropna()
    prev_close = float(closes.iloc[-2])

    today_price = None
    try:
        fi = yf.Ticker(used).fast_info
        lp = fi.get("last_price", None)
        if lp is not None:
            today_price = float(lp)
    except Exception:
        pass

    # Fallback to last close if fast_info missing
    if today_price is None:
        today_price = float(closes.iloc[-1])

    return used, prev_close, today_price, closes


def read_holdings_excel(path: str):
    """
    Expects columns (as in holdings-RM6481.xlsx):
      Symbol, Sector, Quantity Available, Average Price
    """
    df = pd.read_excel(path)
    df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed", case=False)].copy()

    if "Symbol" not in df.columns:
        raise ValueError("Excel file must contain a 'Symbol' column.")
    if "Quantity Available" not in df.columns:
        raise ValueError("Excel file must contain 'Quantity Available' column.")
    if "Average Price" not in df.columns:
        raise ValueError("Excel file must contain 'Average Price' column.")

    df["Symbol"] = df["Symbol"].apply(normalize_symbol)
    df["Quantity Available"] = pd.to_numeric(df["Quantity Available"], errors="coerce").fillna(0)
    df["Average Price"] = pd.to_numeric(df["Average Price"], errors="coerce").fillna(0)

    return df


def df_to_html_table(d, title, cols, align_right):
    rows = []
    for _, r in d.iterrows():
        tds = []
        for c in cols:
            align = "right" if c in align_right else "left"
            tds.append(f"<td style='text-align:{align}'>{escape(str(r.get(c, '')))}</td>")
        rows.append("<tr>" + "".join(tds) + "</tr>")

    ths = []
    for c in cols:
        align = "right" if c in align_right else "left"
        ths.append(f"<th style='text-align:{align}'>{escape(c)}</th>")

    return f"""
    <h3>{escape(title)}</h3>
    <table border="1" cellspacing="0" cellpadding="6" style="border-collapse:collapse;font-size:13px;">
      <thead><tr>{''.join(ths)}</tr></thead>
      <tbody>{''.join(rows) if rows else "<tr><td colspan='99'>No data</td></tr>"}</tbody>
    </table>
    """


def main():
    df_raw = read_holdings_excel(INPUT_FILE)

    used_tickers = []
    prev_closes = []
    today_prices = []
    down_streaks = []

    for sym in df_raw["Symbol"]:
        used, prev, today, closes = fetch_symbol_bundle(sym)
        used_tickers.append(used)
        prev_closes.append(prev)
        today_prices.append(today)

        if closes is None:
            down_streaks.append(0)
        else:
            down_streaks.append(compute_down_streak(closes))

    df = df_raw.copy()
    df["Yahoo Ticker Used"] = used_tickers
    df["Previous Close"] = pd.to_numeric(prev_closes, errors="coerce")
    df["Today Price"] = pd.to_numeric(today_prices, errors="coerce")
    df["Down Streak"] = down_streaks

    qty_col = "Quantity Available"
    avg_col = "Average Price"

    df["Todays Profit"] = (df["Today Price"] - df["Previous Close"]) * df[qty_col]
    df["Total Profit"] = (df["Today Price"] - df[avg_col]) * df[qty_col]

    df["Todays Profit %"] = df.apply(
        lambda r: safe_pct(r["Today Price"] - r["Previous Close"], r["Previous Close"]), axis=1
    )
    df["Total Profit %"] = df.apply(
        lambda r: safe_pct(r["Today Price"] - r[avg_col], r[avg_col]), axis=1
    )

    # ---------- TABLES ----------
    alerts = df[df["Down Streak"] >= ALERT_STREAK_DAYS].copy()
    alerts = alerts.sort_values(["Down Streak", "Todays Profit"], ascending=[False, True])

    # Ensure losers/gainers sorting doesn't hide NaNs:
    df_sort = df.copy()
    df_sort["Todays Profit"] = pd.to_numeric(df_sort["Todays Profit"], errors="coerce")
    losers = df_sort.sort_values("Todays Profit", ascending=True, na_position="last").head(TOP_N)
    gainers = df_sort.sort_values("Todays Profit", ascending=False, na_position="last").head(TOP_N)

    # Missing data should include any unresolved prices, not just missing ticker
    missing = df[
        df["Yahoo Ticker Used"].isna()
        | df["Previous Close"].isna()
        | df["Today Price"].isna()
    ][["Symbol", "Yahoo Ticker Used", "Previous Close", "Today Price"]].copy()

    # Debug: show SILVER-related rows in Actions logs
    dbg = df[df["Symbol"].str.contains("SILVER", na=False)]
    if not dbg.empty:
        print("DEBUG SILVER ROWS:")
        print(dbg[["Symbol", "Yahoo Ticker Used", "Previous Close", "Today Price", "Todays Profit"]].to_string(index=False))

    now = datetime.now(IST)
    subject = f"Portfolio Update ({RUN_LABEL}) - {now:%Y-%m-%d %H:%M} IST"

    html = f"""
    <p><b>{escape(subject)}</b></p>

    {df_to_html_table(alerts, f"🚨 Action Required: Continuous Down ≥ {ALERT_STREAK_DAYS} Days",
      ["Symbol","Down Streak","Todays Profit","Todays Profit %","Total Profit","Total Profit %"],
      {"Down Streak","Todays Profit","Todays Profit %","Total Profit","Total Profit %"})}

    {df_to_html_table(losers, f"Top {TOP_N} Losers (Today)",
      ["Symbol","Todays Profit","Todays Profit %","Total Profit","Total Profit %"],
      {"Todays Profit","Todays Profit %","Total Profit","Total Profit %"})}

    {df_to_html_table(gainers, f"Top {TOP_N} Gainers (Today)",
      ["Symbol","Todays Profit","Todays Profit %","Total Profit","Total Profit %"],
      {"Todays Profit","Todays Profit %","Total Profit","Total Profit %"})}

    {df_to_html_table(missing, "⚠ Missing / Invalid Price Data (Needs Fix)",
      ["Symbol","Yahoo Ticker Used","Previous Close","Today Price"],
      {"Previous Close","Today Price"}) if not missing.empty else ""}
    """

    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    msg["Subject"] = subject
    msg.set_content("Please view this email in HTML format.")
    msg.add_alternative(html, subtype="html")

    print("RUN_LABEL:", RUN_LABEL)
    print("Holdings file:", INPUT_FILE)
    print("Rows:", len(df))
    print("MAIL_FROM:", MAIL_FROM)
    print("MAIL_TO:", MAIL_TO)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

    print("Email sent successfully.")


if __name__ == "__main__":
    main()
