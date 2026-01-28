import os
import pandas as pd
import yfinance as yf
from datetime import datetime
from zoneinfo import ZoneInfo
import smtplib
from email.message import EmailMessage
from html import escape

# ---------- CONFIG ----------
INPUT_FILE = os.environ.get("HOLDINGS_FILE", "holdings.xlsx")

sym_col = "Symbol"
qty_col = "Quantity Available"
avg_col = "Average Price"

IST = ZoneInfo("Asia/Kolkata")

SMTP_HOST = os.environ["SMTP_HOST"]            # smtp.gmail.com
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ["SMTP_USER"]            # your gmail
SMTP_PASS = os.environ["SMTP_PASS"]            # gmail app password
MAIL_TO   = os.environ["MAIL_TO"]
MAIL_FROM = os.environ.get("MAIL_FROM", SMTP_USER)

TOP_N = int(os.environ.get("TOP_N", "10"))
RUN_LABEL = os.environ.get("RUN_LABEL", "RUN").strip().upper()

# --- Alert knobs (defaults are sensible; tune later if you want) ---
ALERT_STREAK_DAYS = int(os.environ.get("ALERT_STREAK_DAYS", "3"))   # continuous up/down days
ALERT_DRAWDOWN_PCT = float(os.environ.get("ALERT_DRAWDOWN_PCT", "8")) # alert if drawdown <= -8%
ALERT_RUNUP_PCT = float(os.environ.get("ALERT_RUNUP_PCT", "10"))      # alert if run-up >= +10%
DRAWDOWN_LOOKBACK = int(os.environ.get("DRAWDOWN_LOOKBACK", "20"))    # days high/low window
HIST_DAYS = int(os.environ.get("HIST_DAYS", "90"))                    # candles to compute indicators
# ---------------------------


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
    Count consecutive up or down days from the most recent close backwards.
    Returns: (up_streak, down_streak)
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
    Returns dict with:
      used_ticker, prev_close, today_price, closes_series (daily close history)
    - Try NSE: SYMBOL.NS then fallback BSE: SYMBOL.BO
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

    # Today price: prefer fast_info.last_price, fallback to last close
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


def df_to_html_table(d: pd.DataFrame, title: str, cols: list[str], align_right: set[str]):
    headers = cols

    def cell(v, is_right=False):
        style = "text-align:right;" if is_right else "text-align:left;"
        return f"<td style='{style}'>{escape(str(v))}</td>"

    ths = []
    for h in headers:
        th_style = "text-align:right;" if h in align_right else "text-align:left;"
        ths.append(f"<th style='{th_style}'>{escape(h)}</th>")

    rows = []
    for _, r in d.iterrows():
        tds = []
        for h in headers:
            val = r.get(h, "")
            tds.append(cell(val, h in align_right))
        rows.append("<tr>" + "".join(tds) + "</tr>")

    return f"""
    <h3 style="margin:16px 0 8px 0;">{escape(title)}</h3>
    <table cellpadding="6" cellspacing="0" border="1" style="border-collapse:collapse; font-family:Arial, sans-serif; font-size:13px; width:100%;">
      <thead>
        <tr style="background:#f2f2f2;">{''.join(ths)}</tr>
      </thead>
      <tbody>
        {''.join(rows) if rows else "<tr><td colspan='99' style='color:#666;'>No alerts.</td></tr>"}
      </tbody>
    </table>
    """


def build_email(df: pd.DataFrame, now_ist: datetime):
    # Top gainers/losers by absolute today's P&L
    losers = df.sort_values("Todays Profit", ascending=True).head(TOP_N)
    gainers = df.sort_values("Todays Profit", ascending=False).head(TOP_N)

    # Attention: only rows with non-empty Alert
    alerts_df = df[df["Alert"].astype(str).str.len() > 0].copy()

    # Make alert table more readable
    alerts_view = alerts_df[[
        "Symbol",
        "Trend",
        "Up Streak",
        "Down Streak",
        "Drawdown %",
        "Run-up %",
        "Todays Profit",
        "Todays Profit %",
        "Total Profit",
        "Total Profit %",
        "Alert",
    ]].copy()

    # Sort alerts by severity (downtrend + drawdown first, then down streak)
    # crude but effective
    alerts_view["__sev"] = 0
    alerts_view.loc[alerts_view["Trend"].eq("DOWN"), "__sev"] += 2
    alerts_view.loc[alerts_view["Drawdown %"] <= -abs(ALERT_DRAWDOWN_PCT), "__sev"] += 2
    alerts_view.loc[alerts_view["Down Streak"] >= ALERT_STREAK_DAYS, "__sev"] += 1
    alerts_view = alerts_view.sort_values(["__sev", "Drawdown %", "Down Streak"], ascending=[False, True, False]).drop(columns=["__sev"])

    # Format view columns for email
    for c in ["Todays Profit", "Total Profit"]:
        alerts_view[c] = alerts_view[c].apply(fmt_money)
    for c in ["Todays Profit %", "Total Profit %", "Drawdown %", "Run-up %"]:
        alerts_view[c] = alerts_view[c].apply(fmt_pct)

    def format_gainer_loser_block(d: pd.DataFrame, title: str):
        view = d[[
            "Symbol", qty_col, "Previous Closing Price", "Today Price",
            "Todays Profit", "Todays Profit %",
            "Total Profit", "Total Profit %"
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
            cols=["Symbol", qty_col, "Previous Closing Price", "Today Price", "Todays Profit", "Todays Profit %", "Total Profit", "Total Profit %"],
            align_right={qty_col, "Previous Closing Price", "Today Price", "Todays Profit", "Todays Profit %", "Total Profit", "Total Profit %"},
        )

    # Plain-text fallback (minimal)
    text_body = (
        f"Portfolio Update ({RUN_LABEL}) - {now_ist.strftime('%Y-%m-%d %H:%M')} IST\n"
        f"Alerts: {len(alerts_df)}\n"
        f"Top {TOP_N} gainers/losers + full portfolio attached.\n"
    )

    # HTML body
    alerts_table = df_to_html_table(
        alerts_view,
        f"🚨 Attention Required (alerts only) — {len(alerts_view)} stock(s)",
        cols=list(alerts_view.columns),
        align_right={"Up Streak", "Down Streak", "Drawdown %", "Run-up %", "Todays Profit", "Todays Profit %", "Total Profit", "Total Profit %"},
    )

    html_body = f"""
    <div style="font-family:Arial, sans-serif;">
      <p style="margin:0 0 10px 0;">
        <b>Portfolio Update ({escape(RUN_LABEL)})</b><br/>
        {escape(now_ist.strftime('%Y-%m-%d %H:%M'))} IST
      </p>

      {alerts_table}

      {format_gainer_loser_block(losers, f"Top {TOP_N} LOSERS (sorted by Today's P&L ₹)")}
      {format_gainer_loser_block(gainers, f"Top {TOP_N} GAINERS (sorted by Today's P&L ₹)")}

      <p style="margin-top:12px; color:#666; font-size:12px;">
        Full portfolio is attached as Excel.
        Alerts are based on streaks (≥{ALERT_STREAK_DAYS} days), EMA5 vs EMA10 trend, and drawdown/run-up over {DRAWDOWN_LOOKBACK} days.
      </p>
    </div>
    """

    subject = f"Portfolio Update ({RUN_LABEL}) - {now_ist.strftime('%Y-%m-%d %H:%M')} IST"
    return subject, text_body, html_body


def send_email(subject: str, text_body: str, html_body: str, attachment_path: str):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO

    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    with open(attachment_path, "rb") as f:
        data = f.read()

    filename = os.path.basename(attachment_path)
    msg.add_attachment(
        data,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
    )

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)


def main():
    df = pd.read_excel(INPUT_FILE)

    # Clean & types
    df[sym_col] = df[sym_col].astype(str).str.strip()
    df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)
    df[avg_col] = pd.to_numeric(df[avg_col], errors="coerce").fillna(0)

    used_list, prev_list, today_list = [], [], []
    up_streaks, down_streaks = [], []
    trends = []
    drawdowns, runups = [], []
    alerts = []

    for sym in df[sym_col]:
        bundle = fetch_symbol_bundle(sym)
        used = bundle["used_ticker"]
        prev_close = bundle["prev_close"]
        today_price = bundle["today_price"]
        closes = bundle["closes"]

        used_list.append(used)
        prev_list.append(prev_close)
        today_list.append(today_price)

        # Defaults if we can't compute indicators
        up_s = down_s = 0
        trend = "NA"
        dd_pct = 0.0
        ru_pct = 0.0
        alert_msgs = []

        if closes is not None and closes.dropna().shape[0] >= 15:
            # Streaks
            up_s, down_s = compute_streak(closes)

            # EMA trend (5 vs 10)
            ema5 = closes.ewm(span=5, adjust=False).mean()
            ema10 = closes.ewm(span=10, adjust=False).mean()
            trend = "UP" if ema5.iloc[-1] > ema10.iloc[-1] else "DOWN" if ema5.iloc[-1] < ema10.iloc[-1] else "FLAT"

            # Drawdown / run-up vs last N days high/low
            window = closes.iloc[-DRAWDOWN_LOOKBACK:] if len(closes) >= DRAWDOWN_LOOKBACK else closes
            rolling_high = float(window.max())
            rolling_low = float(window.min())
            last_close = float(closes.iloc[-1])

            dd_pct = safe_pct(last_close - rolling_high, rolling_high)   # negative when below high
            ru_pct = safe_pct(last_close - rolling_low, rolling_low)     # positive when above low

            # Alert rules (email only)
            if down_s >= ALERT_STREAK_DAYS:
                alert_msgs.append(f"Down {down_s} days")
            if up_s >= ALERT_STREAK_DAYS:
                alert_msgs.append(f"Up {up_s} days")
            if trend == "DOWN" and dd_pct <= -abs(ALERT_DRAWDOWN_PCT):
                alert_msgs.append(f"Drawdown {dd_pct:.1f}%")
            if trend == "UP" and ru_pct >= abs(ALERT_RUNUP_PCT):
                alert_msgs.append(f"Run-up {ru_pct:.1f}%")

        up_streaks.append(up_s)
        down_streaks.append(down_s)
        trends.append(trend)
        drawdowns.append(dd_pct)
        runups.append(ru_pct)
        alerts.append(" | ".join(alert_msgs))

    df["Yahoo Ticker Used"] = used_list
    df["Previous Closing Price"] = pd.to_numeric(prev_list, errors="coerce")
    df["Today Price"] = pd.to_numeric(today_list, errors="coerce")

    # P&L absolute
    df["Todays Profit/Share"] = df["Today Price"] - df["Previous Closing Price"]
    df["Total Profit/Share"] = df["Today Price"] - df[avg_col]

    df["Todays Profit"] = df["Todays Profit/Share"] * df[qty_col]
    df["Total Profit"] = df["Total Profit/Share"] * df[qty_col]

    # P&L percent
    df["Todays Profit %"] = df.apply(
        lambda r: safe_pct(r["Today Price"] - r["Previous Closing Price"], r["Previous Closing Price"]),
        axis=1,
    )
    df["Total Profit %"] = df.apply(
        lambda r: safe_pct(r["Today Price"] - r[avg_col], r[avg_col]),
        axis=1,
    )

    # Indicators + Alerts (kept in Excel too, but alerts are used only in email)
    df["Up Streak"] = up_streaks
    df["Down Streak"] = down_streaks
    df["Trend"] = trends
    df["Drawdown %"] = pd.to_numeric(drawdowns, errors="coerce").round(2)
    df["Run-up %"] = pd.to_numeric(runups, errors="coerce").round(2)
    df["Alert"] = alerts

    # Rounding for money columns
    money_cols = [
        "Previous Closing Price", "Today Price", avg_col,
        "Todays Profit/Share", "Total Profit/Share", "Todays Profit", "Total Profit"
    ]
    for c in money_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

    for c in ["Todays Profit %", "Total Profit %"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

    now_ist = datetime.now(IST)
    stamp = now_ist.strftime("%Y-%m-%d_%H%M")
    out_file = f"holdings_updated_{stamp}_{RUN_LABEL}.xlsx"
    df.to_excel(out_file, index=False)

    subject, text_body, html_body = build_email(df, now_ist)
    send_email(subject, text_body, html_body, out_file)

    print("Sent:", subject)
    print("Attachment:", out_file)


if __name__ == "__main__":
    main()
