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

# Optional label so you can see if it's "OPEN" run or "CLOSE" run
RUN_LABEL = os.environ.get("RUN_LABEL", "RUN").strip().upper()
# ---------------------------


def fetch_prices(base_symbol: str):
    """
    Returns (used_ticker, prev_close, today_price)

    - Try NSE: SYMBOL.NS, fallback to BSE: SYMBOL.BO if no data.
    - prev_close from last 2 daily closes.
    - today_price from fast_info.last_price if available else last close.
    """
    base_symbol = str(base_symbol).strip().upper()
    candidates = [f"{base_symbol}.NS", f"{base_symbol}.BO"]

    used = None
    closes = None

    for tk in candidates:
        t = yf.Ticker(tk)
        hist = t.history(period="12d", interval="1d")
        if hist is not None and (not hist.empty) and "Close" in hist.columns:
            c = hist["Close"].dropna()
            if len(c) >= 1:
                used = tk
                closes = c
                break

    if used is None:
        return None, None, None

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

    return used, prev_close, today_price


def safe_pct(numerator, denominator):
    # avoids division by zero
    if denominator is None or pd.isna(denominator) or float(denominator) == 0.0:
        return 0.0
    return float(numerator) / float(denominator) * 100.0


def fmt_money(x):
    if pd.isna(x):
        return ""
    return f"{x:,.2f}"


def fmt_pct(x):
    if pd.isna(x):
        return ""
    return f"{x:.2f}%"


def df_to_html_table(d: pd.DataFrame, title: str) -> str:
    # Minimal, email-safe HTML
    headers = [
        "Symbol", "Qty", "Prev Close", "Today Price",
        "Today P&L (₹)", "Today P&L (%)",
        "Total P&L (₹)", "Total P&L (%)"
    ]

    rows_html = []
    for _, r in d.iterrows():
        rows_html.append(
            "<tr>"
            f"<td>{escape(str(r['Symbol']))}</td>"
            f"<td style='text-align:right'>{escape(str(int(r[qty_col]) if pd.notna(r[qty_col]) else 0))}</td>"
            f"<td style='text-align:right'>{fmt_money(r['Previous Closing Price'])}</td>"
            f"<td style='text-align:right'>{fmt_money(r['Today Price'])}</td>"
            f"<td style='text-align:right'>{fmt_money(r['Todays Profit'])}</td>"
            f"<td style='text-align:right'>{fmt_pct(r['Todays Profit %'])}</td>"
            f"<td style='text-align:right'>{fmt_money(r['Total Profit'])}</td>"
            f"<td style='text-align:right'>{fmt_pct(r['Total Profit %'])}</td>"
            "</tr>"
        )

    table = f"""
    <h3 style="margin:16px 0 8px 0;">{escape(title)}</h3>
    <table cellpadding="6" cellspacing="0" border="1" style="border-collapse:collapse; font-family:Arial, sans-serif; font-size:13px; width:100%;">
      <thead>
        <tr style="background:#f2f2f2;">
          {''.join([f"<th style='text-align:left'>{escape(h)}</th>" if h=="Symbol" else f"<th style='text-align:right'>{escape(h)}</th>" for h in headers])}
        </tr>
      </thead>
      <tbody>
        {''.join(rows_html)}
      </tbody>
    </table>
    """
    return table


def build_email(df: pd.DataFrame, now_ist: datetime):
    losers = df.sort_values("Todays Profit", ascending=True).head(TOP_N)
    gainers = df.sort_values("Todays Profit", ascending=False).head(TOP_N)

    # Plain text fallback (kept short)
    text_body = []
    text_body.append(f"Portfolio Update ({RUN_LABEL}) - {now_ist.strftime('%Y-%m-%d %H:%M')} IST\n\n")
    text_body.append(f"Top {TOP_N} losers and gainers are included in HTML view. Attachment has full portfolio.\n")

    html_body = f"""
    <div style="font-family:Arial, sans-serif;">
      <p style="margin:0 0 10px 0;">
        <b>Portfolio Update ({escape(RUN_LABEL)})</b><br/>
        {escape(now_ist.strftime('%Y-%m-%d %H:%M'))} IST
      </p>
      {df_to_html_table(losers, f"Top {TOP_N} LOSERS (sorted by Today's P&L ₹)")}
      {df_to_html_table(gainers, f"Top {TOP_N} GAINERS (sorted by Today's P&L ₹)")}
      <p style="margin-top:12px; color:#666; font-size:12px;">
        Full portfolio is attached as Excel.
      </p>
    </div>
    """

    subject = f"Portfolio Update ({RUN_LABEL}) - {now_ist.strftime('%Y-%m-%d %H:%M')} IST"
    return subject, "\n".join(text_body), html_body


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

    for sym in df[sym_col]:
        used, prev_close, today_price = fetch_prices(sym)
        used_list.append(used)
        prev_list.append(prev_close)
        today_list.append(today_price)

    df["Yahoo Ticker Used"] = used_list
    df["Previous Closing Price"] = pd.to_numeric(prev_list, errors="coerce")
    df["Today Price"] = pd.to_numeric(today_list, errors="coerce")

    # Per-share
    df["Todays Profit/Share"] = df["Today Price"] - df["Previous Closing Price"]
    df["Total Profit/Share"] = df["Today Price"] - df[avg_col]

    # Absolute ₹
    df["Todays Profit"] = df["Todays Profit/Share"] * df[qty_col]
    df["Total Profit"] = df["Total Profit/Share"] * df[qty_col]

    # % P&L
    # - Today's %: based on prev close
    df["Todays Profit %"] = df.apply(
        lambda r: safe_pct(r["Today Price"] - r["Previous Closing Price"], r["Previous Closing Price"]),
        axis=1,
    )
    # - Total %: based on average price
    df["Total Profit %"] = df.apply(
        lambda r: safe_pct(r["Today Price"] - r[avg_col], r[avg_col]),
        axis=1,
    )

    # Rounding
    money_cols = [
        "Previous Closing Price", "Today Price", avg_col,
        "Todays Profit/Share", "Total Profit/Share", "Todays Profit", "Total Profit"
    ]
    for c in money_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

    pct_cols = ["Todays Profit %", "Total Profit %"]
    for c in pct_cols:
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
