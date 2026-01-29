import os
import pandas as pd
import yfinance as yf
from datetime import datetime
from zoneinfo import ZoneInfo
import smtplib
from email.message import EmailMessage
from html import escape

# -------------------- CONFIG --------------------
INPUT_FILE = os.environ.get("HOLDINGS_FILE", "holdings.csv")

IST = ZoneInfo("Asia/Kolkata")

SMTP_HOST = os.environ["SMTP_HOST"]
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ["SMTP_USER"]
SMTP_PASS = os.environ["SMTP_PASS"]
MAIL_TO = os.environ["MAIL_TO"]
MAIL_FROM = os.environ.get("MAIL_FROM", SMTP_USER)

TOP_N = int(os.environ.get("TOP_N", "10"))
RUN_LABEL = os.environ.get("RUN_LABEL", "RUN").strip().upper()

# Alerts: ONLY continuous down >= 7 trading days (long-term investor, action only on sustained down)
ALERT_STREAK_DAYS = int(os.environ.get("ALERT_STREAK_DAYS", "7"))

# History needed for streak computation
HIST_DAYS = int(os.environ.get("HIST_DAYS", "120"))  # gives enough trading days for 7+ streak detection
# ------------------------------------------------


def safe_pct(numerator, denominator):
    if denominator is None or pd.isna(denominator) or float(denominator) == 0.0:
        return 0.0
    return float(numerator) / float(denominator) * 100.0


def fmt_money(x):
    if pd.isna(x):
        return ""
    return f"{float(x):,.2f}"


def fmt_pct(x):
    if pd.isna(x):
        return ""
    return f"{float(x):.2f}%"


def compute_streak(closes: pd.Series):
    """
    Returns (up_streak, down_streak) counting consecutive days from most recent close backwards.
    """
    closes = closes.dropna()
    if len(closes) < 2:
        return 0, 0

    up = down = 0
    for i in range(len(closes) - 1, 0, -1):
        if closes.iloc[i] > closes.iloc[i - 1]:
            if down > 0:
                break
            up += 1
        elif closes.iloc[i] < closes.iloc[i - 1]:
            if up > 0:
                break
            down += 1
        else:
            break
    return up, down


def fetch_symbol_bundle(base_symbol: str):
    """
    Returns:
      used_ticker, prev_close, today_price, closes_series

    - Tries NSE (.NS) then BSE (.BO)
    - prev_close: previous trading day's close
    - today_price: fast_info.last_price if available else latest close
    """
    base_symbol = str(base_symbol).strip().upper()
    candidates = [f"{base_symbol}.NS", f"{base_symbol}.BO"]

    used = None
    hist = None

    for tk in candidates:
        t = yf.Ticker(tk)
        h = t.history(period=f"{HIST_DAYS}d", interval="1d")
        if h is not None and (not h.empty) and "Close" in h.columns and h["Close"].dropna().shape[0] >= 2:
            used = tk
            hist = h
            break

    if used is None or hist is None:
        return {"used_ticker": None, "prev_close": None, "today_price": None, "closes": None}

    closes = hist["Close"].dropna()
    last_close = float(closes.iloc[-1])
    prev_close = float(closes.iloc[-2]) if len(closes) >= 2 else last_close

    today_price = None
    try:
        fi = yf.Ticker(used).fast_info
        lp = fi.get("last_price", None)
        if lp is not None:
            today_price = float(lp)
    except Exception:
        pass

    if today_price is None:
        today_price = last_close

    return {"used_ticker": used, "prev_close": prev_close, "today_price": today_price, "closes": closes}


def read_holdings_csv(path: str):
    """
    Expects Zerodha-like holdings CSV with at least:
      Instrument, Qty., Avg. cost
    """
    df = pd.read_csv(path)
    df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed")].copy()

    required = {"Instrument", "Qty.", "Avg. cost"}
    if not required.issubset(set(df.columns)):
        raise ValueError("CSV format not recognized. Expected columns: Instrument, Qty., Avg. cost")

    df["Symbol"] = df["Instrument"].astype(str).str.strip()
    qty_col = "Qty."
    avg_col = "Avg. cost"

    df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)
    df[avg_col] = pd.to_numeric(df[avg_col], errors="coerce").fillna(0)

    return df, qty_col, avg_col


def df_to_html_table(d: pd.DataFrame, title: str, cols: list[str], align_right: set[str]):
    def cell(v, is_right=False):
        style = "text-align:right;" if is_right else "text-align:left;"
        return f"<td style='{style}'>{escape(str(v))}</td>"

    ths = []
    for h in cols:
        th_style = "text-align:right;" if h in align_right else "text-align:left;"
        ths.append(f"<th style='{th_style}'>{escape(h)}</th>")

    rows = []
    for _, r in d.iterrows():
        tds = [cell(r.get(h, ""), h in align_right) for h in cols]
        rows.append("<tr>" + "".join(tds) + "</tr>")

    return f"""
    <h3 style="margin:16px 0 8px 0;">{escape(title)}</h3>
    <table cellpadding="6" cellspacing="0" border="1" style="border-collapse:collapse; font-family:Arial, sans-serif; font-size:13px; width:100%;">
      <thead><tr style="background:#f2f2f2;">{''.join(ths)}</tr></thead>
      <tbody>{''.join(rows) if rows else "<tr><td colspan='99' style='color:#666;'>No data.</td></tr>"}</tbody>
    </table>
    """


def build_email(df: pd.DataFrame, now_ist: datetime, qty_col: str):
    # Always keep top losers/gainers tables
    losers = df.sort_values("Todays Profit", ascending=True).head(TOP_N)
    gainers = df.sort_values("Todays Profit", ascending=False).head(TOP_N)

    # Alerts: ONLY continuous down >= ALERT_STREAK_DAYS
    alerts_df = df[df["Down Streak"] >= ALERT_STREAK_DAYS].copy()
    alerts_df = alerts_df.sort_values(["Down Streak", "Todays Profit"], ascending=[False, True])

    alerts_view = alerts_df[[
        "Symbol",
        "Down Streak",
        "Previous Closing Price",
        "Today Price",
        "Todays Profit",
        "Todays Profit %",
        "Total Profit",
        "Total Profit %",
        "Alert",
    ]].copy()

    # Format for email
    alerts_view["Previous Closing Price"] = alerts_view["Previous Closing Price"].apply(fmt_money)
    alerts_view["Today Price"] = alerts_view["Today Price"].apply(fmt_money)
    alerts_view["Todays Profit"] = alerts_view["Todays Profit"].apply(fmt_money)
    alerts_view["Total Profit"] = alerts_view["Total Profit"].apply(fmt_money)
    alerts_view["Todays Profit %"] = alerts_view["Todays Profit %"].apply(fmt_pct)
    alerts_view["Total Profit %"] = alerts_view["Total Profit %"].apply(fmt_pct)

    def format_block(d: pd.DataFrame, title: str):
        view = d[[
            "Symbol", qty_col, "Previous Closing Price", "Today Price",
            "Todays Profit", "Todays Profit %", "Total Profit", "Total Profit %"
        ]].copy()

        view[qty_col] = view[qty_col].astype(int)
        view["Previous Closing Price"] = view["Previous Closing Price"].apply(fmt_money)
        view["Today Price"] = view["Today Price"].apply(fmt_money)
        view["Todays Profit"] = view["Todays Profit"].apply(fmt_money)
        view["Total Profit"] = view["Total Profit"].apply(fmt_money)
        view["Todays Profit %"] = view["Todays Profit %"].apply(fmt_pct)
        view["Total Profit %"] = view["Total Profit %"].apply(fmt_pct)

        return df_to_html_table(
            view,
            title,
            cols=["Symbol", qty_col, "Previous Closing Price", "Today Price",
                  "Todays Profit", "Todays Profit %", "Total Profit", "Total Profit %"],
            align_right={qty_col, "Previous Closing Price", "Today Price",
                         "Todays Profit", "Todays Profit %", "Total Profit", "Total Profit %"},
        )

    alerts_table = df_to_html_table(
        alerts_view,
        f"🚨 Attention Required — Continuous DOWN ≥ {ALERT_STREAK_DAYS} trading days ({len(alerts_view)} stock(s))",
        cols=list(alerts_view.columns),
        align_right={"Down Streak", "Previous Closing Price", "Today Price",
                     "Todays Profit", "Todays Profit %", "Total Profit", "Total Profit %"},
    )

    text_body = (
        f"Portfolio Update ({RUN_LABEL}) - {now_ist.strftime('%Y-%m-%d %H:%M')} IST\n"
        f"Alerts (Down streak >= {ALERT_STREAK_DAYS}): {len(alerts_df)}\n"
        f"Email contains alerts + Top {TOP_N} losers/gainers.\n"
    )

    html_body = f"""
    <div style="font-family:Arial, sans-serif;">
      <p style="margin:0 0 10px 0;">
        <b>Portfolio Update ({escape(RUN_LABEL)})</b><br/>
        {escape(now_ist.strftime('%Y-%m-%d %H:%M'))} IST
      </p>

      {alerts_table}
      {format_block(losers, f"Top {TOP_N} LOSERS (sorted by Today's P&L ₹)")}
      {format_block(gainers, f"Top {TOP_N} GAINERS (sorted by Today's P&L ₹)")}
    </div>
    """

    subject = f"Portfolio Update ({RUN_LABEL}) - {now_ist.strftime('%Y-%m-%d %H:%M')} IST"
    return subject, text_body, html_body


def send_email(subject: str, text_body: str, html_body: str):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO

    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)


def main():
    df_raw, qty_col, avg_col = read_holdings_csv(INPUT_FILE)

    used_list, prev_list, today_list = [], [], []
    down_streaks, alerts = [], []

    for sym in df_raw["Symbol"]:
        bundle = fetch_symbol_bundle(sym)
        used = bundle["used_ticker"]
        prev_close = bundle["prev_close"]
        today_price = bundle["today_price"]
        closes = bundle["closes"]

        used_list.append(used)
        prev_list.append(prev_close)
        today_list.append(today_price)

        down_s = 0
        if closes is not None and closes.dropna().shape[0] >= 2:
            _, down_s = compute_streak(closes)

        down_streaks.append(down_s)
        alerts.append(f"Down {down_s} days" if down_s >= ALERT_STREAK_DAYS else "")

    df = df_raw.copy()
    df["Yahoo Ticker Used"] = used_list
    df["Previous Closing Price"] = pd.to_numeric(prev_list, errors="coerce")
    df["Today Price"] = pd.to_numeric(today_list, errors="coerce")

    # Absolute P&L
    df["Todays Profit/Share"] = df["Today Price"] - df["Previous Closing Price"]
    df["Total Profit/Share"] = df["Today Price"] - df[avg_col]

    df["Todays Profit"] = df["Todays Profit/Share"] * df[qty_col]
    df["Total Profit"] = df["Total Profit/Share"] * df[qty_col]

    # Percent P&L
    df["Todays Profit %"] = df.apply(
        lambda r: safe_pct(r["Today Price"] - r["Previous Closing Price"], r["Previous Closing Price"]),
        axis=1,
    )
    df["Total Profit %"] = df.apply(
        lambda r: safe_pct(r["Today Price"] - r[avg_col], r[avg_col]),
        axis=1,
    )

    # Down-only alert fields
    df["Down Streak"] = down_streaks
    df["Alert"] = alerts

    # Rounding
    money_cols = [
        "Previous Closing Price", "Today Price", avg_col,
        "Todays Profit/Share", "Total Profit/Share", "Todays Profit", "Total Profit"
    ]
    for c in money_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(2)
    for c in ["Todays Profit %", "Total Profit %"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

    now_ist = datetime.now(IST)
    subject, text_body, html_body = build_email(df, now_ist, qty_col=qty_col)

    # Helpful logs in GitHub Actions
    print("RUN_LABEL:", RUN_LABEL)
    print("MAIL_FROM:", MAIL_FROM)
    print("MAIL_TO:", MAIL_TO)
    print("Subject:", subject)

    send_email(subject, text_body, html_body)

    print("Email sent successfully.")


if __name__ == "__main__":
    main()
