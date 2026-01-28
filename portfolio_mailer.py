import os
import pandas as pd
import yfinance as yf
from datetime import datetime
from zoneinfo import ZoneInfo
import smtplib
from email.message import EmailMessage

# ---------- CONFIG ----------
INPUT_FILE = os.environ.get("HOLDINGS_FILE", "holdings.xlsx")

# Your Excel column names (edit if needed)
sym_col = "Symbol"
qty_col = "Quantity Available"
avg_col = "Average Price"

# Output file naming
IST = ZoneInfo("Asia/Kolkata")

# Email settings from env vars
SMTP_HOST = os.environ["SMTP_HOST"]          # e.g. "smtp.gmail.com"
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ["SMTP_USER"]          # your email id
SMTP_PASS = os.environ["SMTP_PASS"]          # app password recommended
MAIL_TO   = os.environ["MAIL_TO"]            # where to send
MAIL_FROM = os.environ.get("MAIL_FROM", SMTP_USER)

TOP_N = int(os.environ.get("TOP_N", "10"))
# ---------------------------


def fetch_prices(base_symbol: str):
    """
    Returns (used_ticker, prev_close, today_price)
    - Try NSE first: SYMBOL.NS
    - fallback to BSE: SYMBOL.BO if no data
    - prev_close from the latest daily candle's "previous close" via last 2 closes
    - today_price prefers fast_info.last_price, fallback to last close
    """
    base_symbol = str(base_symbol).strip().upper()
    candidates = [f"{base_symbol}.NS", f"{base_symbol}.BO"]

    used = None
    closes = None

    for tk in candidates:
        t = yf.Ticker(tk)
        hist = t.history(period="12d", interval="1d")
        if hist is not None and not hist.empty and "Close" in hist.columns:
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


def build_email_body(df: pd.DataFrame) -> str:
    # Sort by "Todays Profit" to get top gainers/losers
    losers = df.sort_values("Todays Profit", ascending=True).head(TOP_N)
    gainers = df.sort_values("Todays Profit", ascending=False).head(TOP_N)

    cols = ["Symbol", "Quantity Available", "Previous Closing Price", "Today Price",
            "Todays Profit", "Total Profit"]

    def to_text_table(d):
        return d[cols].to_string(index=False)

    body = []
    body.append(f"Top {TOP_N} LOSERS (by Today's P&L):\n")
    body.append(to_text_table(losers))
    body.append("\n\n")
    body.append(f"Top {TOP_N} GAINERS (by Today's P&L):\n")
    body.append(to_text_table(gainers))
    body.append("\n")
    return "".join(body)


def send_email(subject: str, body: str, attachment_path: str):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    msg.set_content(body)

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

    # clean types
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
    df["Previous Closing Price"] = prev_list
    df["Today Price"] = today_list

    # Profits
    df["Todays Profit/Share"] = df["Today Price"] - df["Previous Closing Price"]
    df["Total Profit/Share"] = df["Today Price"] - df[avg_col]

    df["Todays Profit"] = df["Todays Profit/Share"] * df[qty_col]
    df["Total Profit"] = df["Total Profit/Share"] * df[qty_col]

    # Rounding
    for c in ["Previous Closing Price", "Today Price", avg_col, "Todays Profit/Share", "Total Profit/Share",
              "Todays Profit", "Total Profit"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

    now_ist = datetime.now(IST)
    stamp = now_ist.strftime("%Y-%m-%d_%H%M")
    out_file = f"holdings_updated_{stamp}.xlsx"
    df.to_excel(out_file, index=False)

    subject = f"Portfolio Update - {now_ist.strftime('%Y-%m-%d %H:%M')} IST"
    body = build_email_body(df)

    send_email(subject, body, out_file)
    print("Sent:", subject, "with attachment:", out_file)


if __name__ == "__main__":
    main()
