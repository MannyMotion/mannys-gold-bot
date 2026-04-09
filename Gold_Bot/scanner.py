import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import time
import requests
from datetime import datetime, timezone, timedelta

load_dotenv()

# --- CREDENTIALS ---
MT5_LOGIN        = int(os.getenv("MT5_LOGIN"))
MT5_PASSWORD     = os.getenv("MT5_PASSWORD")
MT5_SERVER       = os.getenv("MT5_SERVER")
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- SYMBOLS ---
SYMBOLS = ["XAUUSD", "XAGUSD", "US500", "US30", "BTCUSD", "QQQ.NAS", "NZDUSD"]

# --- 3AM PAIRS (use 2AM+6AM UTC bias hours, not 1AM+5AM) ---
THREE_AM_PAIRS = {"XAGUSD", "US500", "US30", "USOIL"}

# --- TIMEFRAMES PER SYMBOL ---
SYMBOL_TIMEFRAMES = {
    "XAUUSD":  [mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
    "XAGUSD":  [mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
    "US500":   [mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
    "US30":    [mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
    "BTCUSD":  [mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
    "QQQ.NAS": [mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
    "NZDUSD":  [mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
}

TIMEFRAME_NAMES = {
    mt5.TIMEFRAME_M5:  "5M",
    mt5.TIMEFRAME_M15: "15M",
    mt5.TIMEFRAME_M30: "30M",
    mt5.TIMEFRAME_H1:  "1H",
    mt5.TIMEFRAME_H4:  "4H",
}

# --- SETTINGS ---
SYMBOL         = "XAUUSD"
TIMEFRAME_4H   = mt5.TIMEFRAME_H4
TIMEFRAME_1H   = mt5.TIMEFRAME_H1
RISK_PERCENT   = 0.01
RR             = 3.0
CHECK_INTERVAL = 60
ADX_PERIOD     = 14
ADX_THRESH     = 25
ATR_PERIOD     = 14
SCALP_MIN_RR   = 1.5   # minimum RR for scalp signals

# --- SESSIONS UTC ---
LONDON_START  = 7
LONDON_END    = 12
OVERLAP_START = 12
OVERLAP_END   = 13
NY_START      = 13
NY_END        = 17
POSTNY_START  = 17
POSTNY_END    = 22

# ═══════════════════════════════════════════
# PER-PAIR BIAS HOURS
# ─────────────────────────────────────────────
# Matches Pine Script exactly:
# Gold (XAUUSD):              1AM UTC (2AM BST) + 5AM UTC (6AM BST)
# Silver/US30/US500/USOIL:    2AM UTC (3AM BST) + 6AM UTC (7AM BST)
# Hardcoded integers — no dependency on runtime DST detection
# ═══════════════════════════════════════════
def get_pair_bias_hours(symbol):
    """Returns (h1_utc, h2_utc) for this symbol."""
    if symbol in THREE_AM_PAIRS:
        return 2, 6   # 3AM pairs: 2AM+6AM UTC
    return 1, 5       # Gold default: 1AM+5AM UTC

def get_bias_hours():
    """Legacy function — returns Gold default bias hours."""
    return 1, 5

def get_dst_str():
    """Returns BST or GMT string for display."""
    now   = datetime.now(timezone.utc)
    year  = now.year
    march_end = datetime(year, 4, 1, tzinfo=timezone.utc)
    bst_start = march_end - timedelta(days=(march_end.weekday() + 1) % 7)
    bst_start = bst_start.replace(hour=1)
    oct_end   = datetime(year, 11, 1, tzinfo=timezone.utc)
    bst_end   = oct_end - timedelta(days=(oct_end.weekday() + 1) % 7)
    bst_end   = bst_end.replace(hour=1)
    return "BST" if bst_start <= now < bst_end else "GMT"

# ═══════════════════════════════════════════
# NEWS BLACKOUT — ±30 min before and after
# Matches Pine Script cpiDates / fomcDates arrays
# ═══════════════════════════════════════════
CPI_DATES = [
    datetime(2024,1,11,13,30,tzinfo=timezone.utc), datetime(2024,2,13,13,30,tzinfo=timezone.utc),
    datetime(2024,3,12,13,30,tzinfo=timezone.utc), datetime(2024,4,10,13,30,tzinfo=timezone.utc),
    datetime(2024,5,15,13,30,tzinfo=timezone.utc), datetime(2024,6,12,13,30,tzinfo=timezone.utc),
    datetime(2024,7,11,13,30,tzinfo=timezone.utc), datetime(2024,8,14,13,30,tzinfo=timezone.utc),
    datetime(2024,9,11,13,30,tzinfo=timezone.utc), datetime(2024,10,10,13,30,tzinfo=timezone.utc),
    datetime(2024,11,13,13,30,tzinfo=timezone.utc),datetime(2024,12,11,13,30,tzinfo=timezone.utc),
    datetime(2025,1,15,13,30,tzinfo=timezone.utc), datetime(2025,2,12,13,30,tzinfo=timezone.utc),
    datetime(2025,3,12,13,30,tzinfo=timezone.utc), datetime(2025,4,9,13,30,tzinfo=timezone.utc),
    datetime(2025,5,13,13,30,tzinfo=timezone.utc), datetime(2025,6,11,13,30,tzinfo=timezone.utc),
    datetime(2025,7,11,13,30,tzinfo=timezone.utc), datetime(2025,8,12,13,30,tzinfo=timezone.utc),
    datetime(2025,9,10,13,30,tzinfo=timezone.utc), datetime(2025,10,9,13,30,tzinfo=timezone.utc),
    datetime(2025,11,12,13,30,tzinfo=timezone.utc),datetime(2025,12,10,13,30,tzinfo=timezone.utc),
    datetime(2026,1,14,13,30,tzinfo=timezone.utc), datetime(2026,2,12,13,30,tzinfo=timezone.utc),
    datetime(2026,3,11,13,30,tzinfo=timezone.utc), datetime(2026,4,10,13,30,tzinfo=timezone.utc),
    datetime(2026,5,13,13,30,tzinfo=timezone.utc), datetime(2026,6,11,13,30,tzinfo=timezone.utc),
    datetime(2026,7,14,13,30,tzinfo=timezone.utc), datetime(2026,8,12,13,30,tzinfo=timezone.utc),
    datetime(2026,9,10,13,30,tzinfo=timezone.utc), datetime(2026,10,8,13,30,tzinfo=timezone.utc),
    datetime(2026,11,12,13,30,tzinfo=timezone.utc),datetime(2026,12,9,13,30,tzinfo=timezone.utc),
]

FOMC_DATES = [
    datetime(2024,1,31,19,0,tzinfo=timezone.utc),  datetime(2024,3,20,19,0,tzinfo=timezone.utc),
    datetime(2024,5,1,19,0,tzinfo=timezone.utc),   datetime(2024,6,12,19,0,tzinfo=timezone.utc),
    datetime(2024,7,31,19,0,tzinfo=timezone.utc),  datetime(2024,9,18,19,0,tzinfo=timezone.utc),
    datetime(2024,11,7,19,0,tzinfo=timezone.utc),  datetime(2024,12,18,19,0,tzinfo=timezone.utc),
    datetime(2025,1,29,19,0,tzinfo=timezone.utc),  datetime(2025,3,19,19,0,tzinfo=timezone.utc),
    datetime(2025,5,7,19,0,tzinfo=timezone.utc),   datetime(2025,6,18,19,0,tzinfo=timezone.utc),
    datetime(2025,7,30,19,0,tzinfo=timezone.utc),  datetime(2025,9,17,19,0,tzinfo=timezone.utc),
    datetime(2025,11,5,19,0,tzinfo=timezone.utc),  datetime(2025,12,17,19,0,tzinfo=timezone.utc),
    datetime(2026,1,28,19,0,tzinfo=timezone.utc),  datetime(2026,3,18,19,0,tzinfo=timezone.utc),
    datetime(2026,5,6,19,0,tzinfo=timezone.utc),   datetime(2026,6,17,19,0,tzinfo=timezone.utc),
    datetime(2026,7,29,19,0,tzinfo=timezone.utc),  datetime(2026,9,16,19,0,tzinfo=timezone.utc),
    datetime(2026,11,4,19,0,tzinfo=timezone.utc),  datetime(2026,12,16,19,0,tzinfo=timezone.utc),
]

NEWS_WINDOW = timedelta(minutes=30)

def is_news_blackout():
    """
    Returns (blackout: bool, reason: str)
    True if within ±30 minutes of NFP, CPI or FOMC.
    Matches Pine Script newsBlackout logic exactly.
    """
    now = datetime.now(timezone.utc)

    # NFP — first Friday of each month at 13:30 UTC
    if now.weekday() == 4 and now.day <= 7:  # Friday, first week
        nfp_today = now.replace(hour=13, minute=30, second=0, microsecond=0)
        if abs(now - nfp_today) <= NEWS_WINDOW:
            return True, "NFP ±30min"

    # CPI
    for dt in CPI_DATES:
        if abs(now - dt) <= NEWS_WINDOW:
            return True, "CPI ±30min"

    # FOMC
    for dt in FOMC_DATES:
        if abs(now - dt) <= NEWS_WINDOW:
            return True, "FOMC ±30min"

    return False, "Clear"

# ═══════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════
def send_telegram(message):
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Telegram error: {e}")

# ═══════════════════════════════════════════
# SESSION
# ═══════════════════════════════════════════
def get_session():
    h = datetime.now(timezone.utc).hour
    if   LONDON_START  <= h < LONDON_END:  return "London"
    elif OVERLAP_START <= h < OVERLAP_END: return "Overlap"
    elif NY_START      <= h < NY_END:      return "New York"
    elif POSTNY_START  <= h < POSTNY_END:  return "Post-NY"
    else:                                  return "Asian"

def is_active_session():
    return get_session() != "Asian"

# ═══════════════════════════════════════════
# MT5 CONNECTION
# ═══════════════════════════════════════════
def connect_mt5():
    if not mt5.initialize(login=MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER):
        print("MT5 connection failed:", mt5.last_error())
        return False
    return True

# ═══════════════════════════════════════════
# DYNAMIC FILLING MODE
# ═══════════════════════════════════════════
def get_filling_mode(symbol):
    info = mt5.symbol_info(symbol)
    if info is None:
        return mt5.ORDER_FILLING_IOC
    filling = info.filling_mode
    if filling & 2:
        return mt5.ORDER_FILLING_IOC
    elif filling & 1:
        return mt5.ORDER_FILLING_FOK
    else:
        return mt5.ORDER_FILLING_RETURN

# ═══════════════════════════════════════════
# SMART LOT SIZING
# ═══════════════════════════════════════════
def calc_lot_size(symbol, risk_amount_account, sl_distance_price):
    info = mt5.symbol_info(symbol)
    if info is None or sl_distance_price <= 0:
        return 0.01
    tick_size  = info.trade_tick_size
    tick_value = info.trade_tick_value
    if tick_size <= 0 or tick_value <= 0:
        return 0.01
    ticks_in_sl   = sl_distance_price / tick_size
    value_per_lot = ticks_in_sl * tick_value
    if value_per_lot <= 0:
        return 0.01
    lot = risk_amount_account / value_per_lot
    lot = max(info.volume_min,
              min(info.volume_max,
                  round(lot / info.volume_step) * info.volume_step))
    return round(lot, 2)

# ═══════════════════════════════════════════
# GET CANDLES
# ═══════════════════════════════════════════
def get_candles(timeframe, count=500, symbol=None):
    sym   = symbol or SYMBOL
    rates = mt5.copy_rates_from_pos(sym, timeframe, 0, count)
    if rates is None or len(rates) == 0:
        return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
    return df

# ═══════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════
def calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calc_rsi(series, period=14):
    delta    = series.diff()
    gain     = delta.where(delta > 0, 0)
    loss     = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(span=period).mean()
    avg_loss = loss.ewm(span=period).mean()
    rs       = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calc_atr(df, period=14):
    hl = df['high'] - df['low']
    hc = abs(df['high'] - df['close'].shift())
    lc = abs(df['low']  - df['close'].shift())
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.ewm(span=period).mean()

def calc_adx(df, period=14):
    plus_dm  = df['high'].diff()
    minus_dm = df['low'].diff().abs()
    plus_dm[plus_dm   < 0] = 0
    minus_dm[minus_dm < 0] = 0
    atr_val  = calc_atr(df, period)
    plus_di  = 100 * (plus_dm.ewm(span=period).mean()  / atr_val)
    minus_di = 100 * (minus_dm.ewm(span=period).mean() / atr_val)
    dx       = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    return dx.ewm(span=period).mean()

def calc_macd(series):
    macd   = series.ewm(span=12).mean() - series.ewm(span=26).mean()
    signal = macd.ewm(span=9).mean()
    return macd, signal

# ═══════════════════════════════════════════
# STRONG BODY CHECK
# ─────────────────────────────────────────────
# Matches Pine Script c4H_strongBody:
# body >= 60% of total candle range (high - low)
# Prevents wick spikes from confirming bias
# ═══════════════════════════════════════════
def is_strong_body(candle_open, candle_close, candle_high, candle_low, threshold=0.60):
    """Returns (is_strong, is_bull_strong, is_bear_strong)."""
    c_range = candle_high - candle_low
    if c_range <= 0:
        return False, False, False
    c_body      = abs(candle_close - candle_open)
    strong      = (c_body / c_range) >= threshold
    bull_strong = strong and candle_close > candle_open
    bear_strong = strong and candle_close < candle_open
    return strong, bull_strong, bear_strong

# ═══════════════════════════════════════════
# WEEKLY BIAS
# ─────────────────────────────────────────────
# KEY FIX: Uses per-pair hardcoded UTC hours matching Pine Script.
# Gold:  1AM+5AM UTC | Indices/Silver: 2AM+6AM UTC
# Confirmation requires strong body 4H close (>=60% body ratio)
# wb_high = MAX(h1_high, h2_high) | wb_low = MIN(h1_low, h2_low)
# ═══════════════════════════════════════════
def get_weekly_bias(df_1h, df_4h, symbol="XAUUSD"):
    bias_h1, bias_h2 = get_pair_bias_hours(symbol)
    df = df_1h.copy()
    df['dow']  = df['time'].dt.dayofweek   # Monday = 0
    df['hour'] = df['time'].dt.hour

    # Get the two Monday bias candles for this week
    mon_c1 = df[(df['dow'] == 0) & (df['hour'] == bias_h1)]
    mon_c2 = df[(df['dow'] == 0) & (df['hour'] == bias_h2)]

    # Filter to current ISO week
    def this_week_only(sub):
        if len(sub) == 0:
            return sub
        latest_week = sub['time'].dt.isocalendar().week.iloc[-1]
        latest_year = sub['time'].dt.isocalendar().year.iloc[-1]
        return sub[
            (sub['time'].dt.isocalendar().week == latest_week) &
            (sub['time'].dt.isocalendar().year == latest_year)
        ]

    mon_c1_week = this_week_only(mon_c1)
    mon_c2_week = this_week_only(mon_c2)

    wb_high = None
    wb_low  = None
    wk_partial   = False
    wk_full      = False

    if len(mon_c1_week) > 0:
        c1 = mon_c1_week.iloc[-1]
        h1_high = c1['high']
        h1_low  = c1['low']
        if len(mon_c2_week) > 0:
            c2 = mon_c2_week.iloc[-1]
            # Final range: MAX of both highs, MIN of both lows
            wb_high    = max(h1_high, c2['high'])
            wb_low     = min(h1_low,  c2['low'])
            wk_full    = True
            wk_partial = False
        else:
            # Only first candle formed — partial state
            wb_high    = h1_high
            wb_low     = h1_low
            wk_partial = True
            wk_full    = False

    if wb_high is None or wb_low is None:
        return None, None, False, False, False, False, False

    # Strong body 4H confirmation
    # Use last fully closed 4H candle (iloc[-2] = last closed bar)
    if len(df_4h) < 2:
        return wb_high, wb_low, False, False, False, wk_partial, wk_full

    c4h = df_4h.iloc[-2]
    _, c4h_bull_strong, c4h_bear_strong = is_strong_body(
        c4h['open'], c4h['close'], c4h['high'], c4h['low']
    )

    c = df['close'].iloc[-1]
    price_in_range = wb_low < c < wb_high

    # Bias confirmed only if:
    # 1. Both candles formed (wk_full)
    # 2. Last closed 4H candle has strong body break
    wk_bull = wk_full and c4h['close'] > wb_high and c4h_bull_strong
    wk_bear = wk_full and c4h['close'] < wb_low  and c4h_bear_strong

    return wb_high, wb_low, wk_bull, wk_bear, price_in_range, wk_partial, wk_full

# ═══════════════════════════════════════════
# DAILY BIAS
# ─────────────────────────────────────────────
# Uses per-pair bias hour (h1 only — first candle of day)
# Confirmation requires strong body 4H close
# ═══════════════════════════════════════════
def get_daily_bias(df_1h, df_4h, symbol="XAUUSD"):
    bias_h1, _ = get_pair_bias_hours(symbol)
    df = df_1h.copy()
    df['hour'] = df['time'].dt.hour
    df['date'] = df['time'].dt.date

    daily_candle = None
    for d in reversed(sorted(df['date'].unique())):
        match = df[(df['date'] == d) & (df['hour'] == bias_h1)]
        if len(match) > 0:
            daily_candle = match.iloc[-1]
            break

    if daily_candle is None:
        return None, None, False, False

    db_high = daily_candle['high']
    db_low  = daily_candle['low']

    # Sanity check — if range too small use recent bars
    price_scale = db_high if db_high > 1 else 1
    if (db_high - db_low) < price_scale * 0.0005:
        recent  = df.tail(8)
        db_high = recent['high'].max()
        db_low  = recent['low'].min()

    # Strong body 4H confirmation
    if len(df_4h) < 2:
        return db_high, db_low, False, False

    c4h = df_4h.iloc[-2]
    _, c4h_bull_strong, c4h_bear_strong = is_strong_body(
        c4h['open'], c4h['close'], c4h['high'], c4h['low']
    )

    day_bull = c4h['close'] > db_high and c4h_bull_strong
    day_bear = c4h['close'] < db_low  and c4h_bear_strong

    return db_high, db_low, day_bull, day_bear

# ═══════════════════════════════════════════
# HTF EMA
# ═══════════════════════════════════════════
def get_htf_ema(df_1h, df_4h):
    ema200_1h = calc_ema(df_1h['close'], 200).iloc[-1]
    ema200_4h = calc_ema(df_4h['close'], 200).iloc[-1]
    c1h = df_1h['close'].iloc[-1]
    c4h = df_4h['close'].iloc[-1]
    htf_bull = (c1h > ema200_1h) or (c4h > ema200_4h)
    htf_bear = (c1h < ema200_1h) or (c4h < ema200_4h)
    return htf_bull, htf_bear

# ═══════════════════════════════════════════
# LEVELS
# ═══════════════════════════════════════════
def get_prev_day_hl(df):
    df = df.copy()
    df['date'] = df['time'].dt.date
    dates = sorted(df['date'].unique())
    if len(dates) < 2:
        return None, None
    prev = df[df['date'] == dates[-2]]
    return prev['high'].max(), prev['low'].min()

def get_prev_week_hl(df):
    df = df.copy()
    df['week'] = df['time'].dt.isocalendar().week
    df['year'] = df['time'].dt.isocalendar().year
    weeks = df[['year','week']].drop_duplicates().values.tolist()
    if len(weeks) < 2:
        return None, None
    py, pw = weeks[-2]
    prev = df[(df['year'] == py) & (df['week'] == pw)]
    return prev['high'].max(), prev['low'].min()

def get_prev_month_hl(df):
    df = df.copy()
    df['month'] = df['time'].dt.month
    df['year']  = df['time'].dt.year
    months = df[['year','month']].drop_duplicates().values.tolist()
    if len(months) < 2:
        return None, None
    py, pm = months[-2]
    prev = df[(df['year'] == py) & (df['month'] == pm)]
    return prev['high'].max(), prev['low'].min()

# ═══════════════════════════════════════════
# BOS
# ═══════════════════════════════════════════
def check_bos(df, atr_series, bos_reset=30):
    close  = df['close']
    open_  = df['open']
    ema200 = calc_ema(close, 200)
    sw_high     = df['high'].rolling(11, center=True).max()
    sw_low      = df['low'].rolling(11,  center=True).min()
    strong_body = abs(close - open_) > atr_series * 0.5
    above_e200  = close > ema200
    below_e200  = close < ema200
    bull_bos = (close > sw_high.shift(1)) & strong_body & above_e200 & (close.shift(1) <= sw_high.shift(1))
    bear_bos = (close < sw_low.shift(1))  & strong_body & below_e200 & (close.shift(1) >= sw_low.shift(1))
    return bull_bos.iloc[-bos_reset:].any(), bear_bos.iloc[-bos_reset:].any()

# ═══════════════════════════════════════════
# SWEEPS
# ═══════════════════════════════════════════
def check_sweeps(df, atr_series, d1h, d1l):
    if d1h is None or d1l is None:
        return False, False, False, False, None, None
    last = df.iloc[-2]
    atr  = atr_series.iloc[-2]
    bull_sweep  = (last['low'] < d1l)  and (last['close'] > d1l)  and ((d1l - last['low'])   >= atr * 0.3)
    bear_sweep  = (last['high'] > d1h) and (last['close'] < d1h)  and ((last['high'] - d1h)  >= atr * 0.3)
    bull_sw_rej = bull_sweep and ((last['close'] - last['low'])   > atr * 0.7)
    bear_sw_rej = bear_sweep and ((last['high']  - last['close']) > atr * 0.7)
    return (bull_sweep, bear_sweep, bull_sw_rej, bear_sw_rej,
            last['low']  if bull_sweep else None,
            last['high'] if bear_sweep else None)

# ═══════════════════════════════════════════
# INTERNAL SWEEP
# ═══════════════════════════════════════════
def check_internal_sweep(df, above_e200, below_e200):
    low3  = df['low'].iloc[-4:-1].min()
    high3 = df['high'].iloc[-4:-1].max()
    last  = df.iloc[-2]
    int_bull = (last['low'] < low3)   and (last['close'] > low3)  and above_e200
    int_bear = (last['high'] > high3) and (last['close'] < high3) and below_e200
    return int_bull, int_bear

# ═══════════════════════════════════════════
# EQUAL HIGHS / LOWS SWEPT
# ═══════════════════════════════════════════
def check_eql_swept(df, atr):
    ph  = df['high'].iloc[-12]
    pph = df['high'].iloc[-23]
    pl  = df['low'].iloc[-12]
    ppl = df['low'].iloc[-23]
    last    = df.iloc[-2]
    is_eqh  = (abs(ph - pph) <= atr * 0.1) if not (pd.isna(ph) or pd.isna(pph)) else False
    is_eql  = (abs(pl - ppl) <= atr * 0.1) if not (pd.isna(pl) or pd.isna(ppl)) else False
    eqh_swept = is_eqh and (last['high'] > ph) and (last['close'] < ph)
    eql_swept = is_eql and (last['low']  < pl) and (last['close'] > pl)
    return eqh_swept, eql_swept

# ═══════════════════════════════════════════
# FVG
# ═══════════════════════════════════════════
def check_fvg(df, atr_series, above_e200, below_e200,
              wk_bull, wk_bear, day_bull, day_bear,
              bias_med_bull, bias_med_bear,
              bias_sca_bull, bias_sca_bear):
    if len(df) < 6:
        return False, False, None, None

    c0  = df.iloc[-1]
    c1  = df.iloc[-2]
    c2  = df.iloc[-3]
    c3  = df.iloc[-4]

    atr   = atr_series.iloc[-3]
    body  = abs(c2['close'] - c2['open'])
    str_cdl = body > atr * 1.0

    b_fvg_sz  = c1['low']  - c3['high']
    br_fvg_sz = c3['low']  - c1['high']

    fvg_min = 0.5
    b_fvg   = (b_fvg_sz  > 0) and str_cdl and (c2['close'] > c2['open']) and above_e200 and (b_fvg_sz  >= atr * fvg_min)
    br_fvg  = (br_fvg_sz > 0) and str_cdl and (c2['close'] < c2['open']) and below_e200 and (br_fvg_sz >= atr * fvg_min)

    disp_cdl = abs(c0['close'] - c0['open']) > atr_series.iloc[-1] * 1.5

    bull_in_fvg = b_fvg  and (c0['low'] <= c1['low'])   and (c0['close'] >= c3['high'])
    bear_in_fvg = br_fvg and (c0['high'] >= c1['high'])  and (c0['close'] <= c3['low'])

    bull_fvg_ez = bull_in_fvg and disp_cdl and above_e200 and (wk_bull or bias_med_bull or bias_sca_bull)
    bear_fvg_ez = bear_in_fvg and disp_cdl and below_e200 and (wk_bear or bias_med_bear or bias_sca_bear)

    bull_fvg_bot = c3['high'] if b_fvg else None
    bear_fvg_top = c3['low']  if br_fvg else None

    return bull_fvg_ez, bear_fvg_ez, bull_fvg_bot, bear_fvg_top

# ═══════════════════════════════════════════
# STATEFUL ORDER BLOCK DETECTION
# ═══════════════════════════════════════════
def check_ob_stateful(df, atr_series, above_e200, below_e200, ob_state, key):
    if len(df) < 8:
        return False, False, None, None

    close  = df['close']
    open_  = df['open']
    atr    = atr_series.iloc[-2]
    n      = len(df)

    bull_imp = (close.iloc[-2] > close.iloc[-3] and
                close.iloc[-3] > close.iloc[-4] and
                close.iloc[-4] > close.iloc[-5])

    bear_imp = (close.iloc[-2] < close.iloc[-3] and
                close.iloc[-3] < close.iloc[-4] and
                close.iloc[-4] < close.iloc[-5])

    if bull_imp and above_e200 and len(df) >= 6:
        ob_candle = df.iloc[-6]
        if (ob_candle['close'] < ob_candle['open'] and
                abs(ob_candle['close'] - ob_candle['open']) > atr * 0.5):
            existing = ob_state[key].get('bull_ob')
            if not existing or not existing.get('active', False):
                ob_state[key]['bull_ob'] = {
                    'top': ob_candle['high'], 'bot': ob_candle['low'],
                    'active': True, 'touched': False,
                    'bar_formed': n, 'formed_at': str(ob_candle['time'])
                }

    if bear_imp and below_e200 and len(df) >= 6:
        ob_candle = df.iloc[-6]
        if (ob_candle['close'] > ob_candle['open'] and
                abs(ob_candle['close'] - ob_candle['open']) > atr * 0.5):
            existing = ob_state[key].get('bear_ob')
            if not existing or not existing.get('active', False):
                ob_state[key]['bear_ob'] = {
                    'top': ob_candle['high'], 'bot': ob_candle['low'],
                    'active': True, 'touched': False,
                    'bar_formed': n, 'formed_at': str(ob_candle['time'])
                }

    bull_ob_rsp = False
    bear_ob_rsp = False
    bull_ob_bot = None
    bear_ob_top = None
    current = df.iloc[-2]

    bob = ob_state[key].get('bull_ob')
    if bob and bob.get('active') and not bob.get('touched'):
        if current['low'] <= bob['top'] and current['close'] >= bob['bot']:
            bull_ob_rsp = True
            bull_ob_bot = bob['bot']
            ob_state[key]['bull_ob']['touched'] = True
        if current['close'] < bob['bot']:
            ob_state[key]['bull_ob']['active'] = False

    bearob = ob_state[key].get('bear_ob')
    if bearob and bearob.get('active') and not bearob.get('touched'):
        if current['high'] >= bearob['bot'] and current['close'] <= bearob['top']:
            bear_ob_rsp = True
            bear_ob_top = bearob['top']
            ob_state[key]['bear_ob']['touched'] = True
        if current['close'] > bearob['top']:
            ob_state[key]['bear_ob']['active'] = False

    if bob and (n - bob.get('bar_formed', 0)) > 50:
        ob_state[key]['bull_ob']['active'] = False
    if bearob and (n - bearob.get('bar_formed', 0)) > 50:
        ob_state[key]['bear_ob']['active'] = False

    return bull_ob_rsp, bear_ob_rsp, bull_ob_bot, bear_ob_top

# ═══════════════════════════════════════════
# RSI DIVERGENCE
# ═══════════════════════════════════════════
def check_rsi_div(df, rsi_series):
    lkb  = 7
    low  = df['low']
    high = df['high']
    p_ll = low.iloc[-lkb:].min()   < low.iloc[-2*lkb:-lkb].min()
    p_hl = low.iloc[-lkb:].min()   > low.iloc[-2*lkb:-lkb].min()
    p_hh = high.iloc[-lkb:].max()  > high.iloc[-2*lkb:-lkb].max()
    p_lh = high.iloc[-lkb:].max()  < high.iloc[-2*lkb:-lkb].max()
    r_hl = rsi_series.iloc[-lkb:].min() > rsi_series.iloc[-2*lkb:-lkb].min()
    r_ll = rsi_series.iloc[-lkb:].min() < rsi_series.iloc[-2*lkb:-lkb].min()
    r_lh = rsi_series.iloc[-lkb:].max() < rsi_series.iloc[-2*lkb:-lkb].max()
    r_hh = rsi_series.iloc[-lkb:].max() > rsi_series.iloc[-2*lkb:-lkb].max()
    return (p_ll and r_hl), (p_hl and r_ll), (p_hh and r_lh), (p_lh and r_hh)

# ═══════════════════════════════════════════
# STRUCTURE-BASED SL / TP
# ─────────────────────────────────────────────
# Matches Pine Script exactly:
# Priority: OB → FVG → 15-bar swing → ATR fallback
# Session buffer: London/NY 0.2×ATR, Post-NY 0.3×ATR
# Scalp TP: opposite weekly level if RR ≥ 1.5
# ═══════════════════════════════════════════
def calc_structure_sl_tp(
    close, atr,
    t1_bull, t2_bull, t1_bear, t2_bear,
    bull_ob_bot, bear_ob_top,
    bull_fvg_bot, bear_fvg_top,
    df, wb_high, wb_low,
    bull_tier, bear_tier,
    rr=3.0
):
    session = get_session()
    if session in ("London", "New York", "Overlap"):
        sess_buf = atr * 0.2
    elif session == "Post-NY":
        sess_buf = atr * 0.3
    else:
        sess_buf = atr * 0.5

    # LONG SL
    local_swing_low = df['low'].iloc[-17:-2].min()
    long_sl_atr     = close - atr * 1.5

    if t2_bull and bull_ob_bot is not None:
        long_sl     = bull_ob_bot - sess_buf
        sl_src_bull = "OB"
    elif t1_bull and bull_fvg_bot is not None:
        long_sl     = bull_fvg_bot - sess_buf
        sl_src_bull = "FVG"
    elif (local_swing_low - sess_buf) > (close - atr * 3):
        long_sl     = local_swing_low - sess_buf
        sl_src_bull = "Swing"
    else:
        long_sl     = long_sl_atr
        sl_src_bull = "ATR"

    long_risk = max(close - long_sl, 0.0001)

    # SHORT SL
    local_swing_high = df['high'].iloc[-17:-2].max()
    short_sl_atr     = close + atr * 1.5

    if t2_bear and bear_ob_top is not None:
        short_sl     = bear_ob_top + sess_buf
        sl_src_bear  = "OB"
    elif t1_bear and bear_fvg_top is not None:
        short_sl     = bear_fvg_top + sess_buf
        sl_src_bear  = "FVG"
    elif (local_swing_high + sess_buf) < (close + atr * 3):
        short_sl     = local_swing_high + sess_buf
        sl_src_bear  = "Swing"
    else:
        short_sl     = short_sl_atr
        sl_src_bear  = "ATR"

    short_risk = max(short_sl - close, 0.0001)

    # TP — base is always 1:3 from structure SL
    long_tp_base  = close + long_risk  * rr
    short_tp_base = close - short_risk * rr

    # STRONG: pure 1:3
    # MEDIUM: target weekly bias level if beyond 1:3
    # SCALP:  target opposite weekly level, only if RR >= 1.5
    if bull_tier == "STRONG":
        long_tp = long_tp_base
    elif bull_tier == "MEDIUM":
        long_tp = wb_high if (wb_high is not None and wb_high >= long_tp_base) else long_tp_base
    elif bull_tier == "SCALP":
        long_tp_sca = wb_high if (wb_high is not None and wb_high >= long_tp_base) else long_tp_base
        long_rr_sca = (long_tp_sca - close) / long_risk if long_risk > 0 else 0
        long_tp     = long_tp_sca if long_rr_sca >= SCALP_MIN_RR else long_tp_base
    else:
        long_tp = long_tp_base

    if bear_tier == "STRONG":
        short_tp = short_tp_base
    elif bear_tier == "MEDIUM":
        short_tp = wb_low if (wb_low is not None and wb_low <= short_tp_base) else short_tp_base
    elif bear_tier == "SCALP":
        short_tp_sca = wb_low if (wb_low is not None and wb_low <= short_tp_base) else short_tp_base
        short_rr_sca = (close - short_tp_sca) / short_risk if short_risk > 0 else 0
        short_tp     = short_tp_sca if short_rr_sca >= SCALP_MIN_RR else short_tp_base
    else:
        short_tp = short_tp_base

    return (round(long_sl, 5),  long_risk,  round(long_tp, 5),  sl_src_bull,
            round(short_sl, 5), short_risk, round(short_tp, 5), sl_src_bear)

# ═══════════════════════════════════════════
# SCALP RR CHECK
# Matches Pine Script bullSig_final logic:
# Block scalp signal if RR < 1.5
# ═══════════════════════════════════════════
def check_scalp_rr_ok(close, tp, sl, tier):
    """Returns True if signal should fire (non-scalp always True)."""
    if tier != "SCALP":
        return True
    risk = abs(close - sl)
    if risk <= 0:
        return False
    rr = abs(tp - close) / risk
    return rr >= SCALP_MIN_RR

# ═══════════════════════════════════════════
# PLACE TRADE
# ═══════════════════════════════════════════
def place_trade(signal, sl_price, tp_price, lot_size, symbol=None, tf_name="30M"):
    sym  = symbol or SYMBOL
    tick = mt5.symbol_info_tick(sym)
    if tick is None:
        send_telegram(f"❌ Failed to get price for {sym}!")
        return

    price      = tick.ask if signal == "BUY" else tick.bid
    order_type = mt5.ORDER_TYPE_BUY if signal == "BUY" else mt5.ORDER_TYPE_SELL
    filling    = get_filling_mode(sym)

    req = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       sym,
        "volume":       lot_size,
        "type":         order_type,
        "price":        price,
        "sl":           round(sl_price, 5),
        "tp":           round(tp_price, 5),
        "deviation":    20,
        "magic":        234000,
        "comment":      f"Mannys V3 {tf_name}",
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": filling,
    }

    result  = mt5.order_send(req)
    account = mt5.account_info()

    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        err = result.comment if result else "No response from MT5"
        send_telegram(f"❌ <b>TRADE FAILED</b>\n{sym} {tf_name} {signal}\nError: {err}\nRetcode: {result.retcode if result else 'N/A'}")
        print(f"❌ Trade failed: {err}")
    else:
        dst_str = get_dst_str()
        send_telegram(
            f"🥇 <b>TRADE PLACED — Manny's V3</b>\n\n"
            f"📊 {signal} | {sym} | {tf_name}\n"
            f"🏦 {get_session()} | {dst_str}\n"
            f"📈 Entry: {result.price}\n"
            f"🛑 SL: {round(sl_price,5)}\n"
            f"🎯 TP: {round(tp_price,5)}\n"
            f"📦 Lots: {lot_size}\n"
            f"💼 Balance: {account.balance} GBP\n"
            f"⏰ {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
        )
        print(f"✅ {sym} {tf_name} {signal} | Entry:{result.price} SL:{round(sl_price,5)} TP:{round(tp_price,5)} Lots:{lot_size}")

# ═══════════════════════════════════════════
# MAIN SIGNAL CHECK
# ═══════════════════════════════════════════
def check_signal(last_signal_bar, last_bull_bar, last_bear_bar,
                 symbol=None, timeframe=None, ob_state=None):

    sym     = symbol or SYMBOL
    tf      = timeframe or mt5.TIMEFRAME_M30
    tf_name = TIMEFRAME_NAMES.get(tf, "30M")
    key     = f"{sym}_{tf_name}"

    if ob_state is None:
        ob_state = {}
    if key not in ob_state:
        ob_state[key] = {'bull_ob': None, 'bear_ob': None}

    # ── NEWS BLACKOUT CHECK ──
    # Must happen before any signal processing — matches Pine Script gate order
    news_black, news_reason = is_news_blackout()
    if news_black:
        print(f"  🚫 {sym} {tf_name} — NEWS BLACKOUT: {news_reason}")
        return last_signal_bar, last_bull_bar, last_bear_bar

    df_main = get_candles(tf, 500, sym)
    df_4h   = get_candles(TIMEFRAME_4H, 500, sym)
    df_1h   = get_candles(TIMEFRAME_1H, 500, sym)

    if df_main is None or df_4h is None or df_1h is None:
        print(f"  ⚠ No data: {sym} {tf_name}")
        return last_signal_bar, last_bull_bar, last_bear_bar

    if len(df_main) < 210:
        print(f"  ⚠ Not enough bars: {sym} {tf_name}")
        return last_signal_bar, last_bull_bar, last_bear_bar

    # --- INDICATORS ---
    df_main['ema200'] = calc_ema(df_main['close'], 200)
    df_main['ema50']  = calc_ema(df_main['close'], 50)
    df_main['rsi']    = calc_rsi(df_main['close'], 14)
    df_main['atr']    = calc_atr(df_main, ATR_PERIOD)
    df_main['adx']    = calc_adx(df_main, ADX_PERIOD)
    df_main['macd'], df_main['macd_sig'] = calc_macd(df_main['close'])

    last        = df_main.iloc[-2]
    current_bar = last['time']
    bar_index   = len(df_main) - 2

    tf_cooldown = {
        mt5.TIMEFRAME_M5:  12,
        mt5.TIMEFRAME_M15: 10,
        mt5.TIMEFRAME_M30: 8,
        mt5.TIMEFRAME_H1:  5,
        mt5.TIMEFRAME_H4:  3,
    }.get(tf, 8)

    bars_since_bull = (bar_index - last_bull_bar) if last_bull_bar is not None else 999
    bars_since_bear = (bar_index - last_bear_bar) if last_bear_bar is not None else 999
    bull_cooled     = bars_since_bull > tf_cooldown
    bear_cooled     = bars_since_bear > tf_cooldown

    close   = last['close']
    ema200  = last['ema200']
    ema50   = last['ema50']
    rsi     = last['rsi']
    atr     = last['atr']
    adx_val = last['adx']
    macd_l  = last['macd']
    macd_s  = last['macd_sig']

    above_e200 = close > ema200
    below_e200 = close < ema200
    pct_below  = (ema200 - close) / ema200 * 100 if ema200 > 0 else 0
    ext_bear   = pct_below > 20
    trending   = adx_val >= ADX_THRESH
    macd_bull  = macd_l > macd_s
    macd_bear  = macd_l < macd_s
    rsi_bull_x = rsi < 40
    rsi_bear_x = rsi > 60

    # ── BIAS — per-pair hardcoded hours + strong body confirmation ──
    wb_high, wb_low, wk_bull, wk_bear, price_in_weekly_range, wk_partial, wk_full = \
        get_weekly_bias(df_1h, df_4h, sym)
    db_high, db_low, day_bull, day_bear = get_daily_bias(df_1h, df_4h, sym)
    htf_bull, htf_bear = get_htf_ema(df_1h, df_4h)

    # ── MIXED BIAS TIERS ──
    bias_med_bull = wk_bear and day_bull
    bias_med_bear = wk_bull and day_bear
    bias_sca_bull = price_in_weekly_range and day_bull
    bias_sca_bear = price_in_weekly_range and day_bear

    # ── GATES — matching Pine Script signal firing exactly ──
    # G1 weekly, G2 daily, G3 HTF, G4 session+no_news, G5 not_ext_bear, G6 macro
    macro_trend_bull = wk_bull and day_bull
    macro_trend_bear = wk_bear and day_bear

    g_str_bull = wk_bull and day_bull and htf_bull and (not ext_bear) and macro_trend_bull
    g_str_bear = wk_bear and day_bear and htf_bear and (not ext_bear) and macro_trend_bear
    g_med_bull = wk_bear and day_bull and htf_bull and (not ext_bear)
    g_med_bear = wk_bull and day_bear and htf_bear and (not ext_bear)
    g_sca_bull = price_in_weekly_range and day_bull and htf_bull and (not ext_bear)
    g_sca_bear = price_in_weekly_range and day_bear and htf_bear and (not ext_bear)

    # --- LEVELS ---
    d1h, d1l      = get_prev_day_hl(df_main)
    w_high, w_low = get_prev_week_hl(df_main)
    m_high, m_low = get_prev_month_hl(df_main)

    w_mid   = (w_low + (w_high - w_low) / 2) if w_high and w_low else None
    in_disc = (close < w_mid) if w_mid else False
    in_prem = (close > w_mid) if w_mid else False
    m_bull  = (close > m_low  and above_e200) if m_low  else False
    m_bear  = (close < m_high and below_e200) if m_high else False

    # --- BOS ---
    rec_bull_bos, rec_bear_bos = check_bos(df_main, df_main['atr'])

    # --- SWEEPS ---
    (bull_sweep, bear_sweep, bull_sw_rej, bear_sw_rej,
     last_bull_sw_low, last_bear_sw_high) = check_sweeps(df_main, df_main['atr'], d1h, d1l)

    # --- INTERNAL SWEEP ---
    int_bull, int_bear = check_internal_sweep(df_main, above_e200, below_e200)

    # --- EQL ---
    eqh_swept, eql_swept = check_eql_swept(df_main, atr)

    # --- TIER 2 STRUCTURE ---
    s2_long  = sum([above_e200, rec_bull_bos, bull_sweep, in_disc, m_bull])
    s2_short = sum([below_e200, rec_bear_bos, bear_sweep, in_prem, m_bear])

    # --- FVG ---
    bull_fvg_ez, bear_fvg_ez, bull_fvg_bot, bear_fvg_top = check_fvg(
        df_main, df_main['atr'],
        above_e200, below_e200,
        wk_bull, wk_bear, day_bull, day_bear,
        bias_med_bull, bias_med_bear, bias_sca_bull, bias_sca_bear
    )

    # --- OB ---
    bull_ob_rsp, bear_ob_rsp, bull_ob_bot, bear_ob_top = check_ob_stateful(
        df_main, df_main['atr'], above_e200, below_e200, ob_state, key
    )

    # --- RSI DIV ---
    s_bull_div, h_bull_div, s_bear_div, h_bear_div = check_rsi_div(df_main, df_main['rsi'])

    # --- TIER 3 TRIGGERS ---
    bull_wick = last['low']  - min(last['open'], last['close'])
    bear_wick = max(last['open'], last['close']) - last['high']
    pin_body  = abs(last['close'] - last['open'])

    t1_bull = bull_fvg_ez
    t1_bear = bear_fvg_ez
    t2_bull = bull_ob_rsp
    t2_bear = bear_ob_rsp
    t3_bull = bull_sw_rej
    t3_bear = bear_sw_rej
    t4_bull = ((bull_wick >= 2.5 * pin_body) and (pin_body >= atr * 0.1) and
               above_e200 and (wk_bull or bias_med_bull or bias_sca_bull) and day_bull)
    t4_bear = ((bear_wick >= 2.5 * pin_body) and (pin_body >= atr * 0.1) and
               below_e200 and (wk_bear or bias_med_bear or bias_sca_bear) and day_bear)
    t5_bull = ((d1h is not None) and (close > d1h) and
               (df_main['close'].iloc[-3] <= d1h) and above_e200 and
               (wk_bull or bias_med_bull or bias_sca_bull) and day_bull)
    t5_bear = ((d1l is not None) and (close < d1l) and
               (df_main['close'].iloc[-3] >= d1l) and below_e200 and
               (wk_bear or bias_med_bear or bias_sca_bear) and day_bear)
    lh1 = (df_main['high'].iloc[-2] < df_main['high'].iloc[-3] and
           df_main['high'].iloc[-3] < df_main['high'].iloc[-4] and
           df_main['high'].iloc[-4] < df_main['high'].iloc[-5])
    hl1 = (df_main['low'].iloc[-2]  > df_main['low'].iloc[-3]  and
           df_main['low'].iloc[-3]  > df_main['low'].iloc[-4]  and
           df_main['low'].iloc[-4]  > df_main['low'].iloc[-5])
    t7_bull = (lh1 and (last['high'] > df_main['high'].iloc[-3]) and
               above_e200 and (wk_bull or bias_med_bull or bias_sca_bull) and day_bull)
    t7_bear = (hl1 and (last['low']  < df_main['low'].iloc[-3])  and
               below_e200 and (wk_bear or bias_med_bear or bias_sca_bear) and day_bear)

    any_trig_bull = t1_bull or t2_bull or t3_bull or t4_bull or t5_bull or t7_bull
    any_trig_bear = t1_bear or t2_bear or t3_bear or t4_bear or t5_bear or t7_bear

    trig_name_bull = ("FVG" if t1_bull else "OB" if t2_bull else "Sweep" if t3_bull else
                      "Pin" if t4_bull else "PDH" if t5_bull else "CHoCH" if t7_bull else "None")
    trig_name_bear = ("FVG" if t1_bear else "OB" if t2_bear else "Sweep" if t3_bear else
                      "Pin" if t4_bear else "PDL" if t5_bear else "CHoCH" if t7_bear else "None")

    # --- TIER 4 BONUS ---
    bon_l  = 0
    bon_l += 2 if s_bull_div else (1 if h_bull_div else 0)
    bon_l += 1 if macd_bull  else 0
    bon_l += 1 if rsi_bull_x else 0
    bon_l += 1 if trending   else 0
    bon_l += 1 if int_bull   else 0
    bon_l += 1 if eql_swept  else 0

    bon_s  = 0
    bon_s += 2 if s_bear_div else (1 if h_bear_div else 0)
    bon_s += 1 if macd_bear  else 0
    bon_s += 1 if rsi_bear_x else 0
    bon_s += 1 if trending   else 0
    bon_s += 1 if int_bear   else 0
    bon_s += 1 if eqh_swept  else 0

    min_bonus = 2
    min_str2  = 2

    # --- RAW SIGNAL FIRING ---
    raw_bull_str = (g_str_bull and bull_cooled and (s2_long  >= min_str2) and any_trig_bull and (bon_l >= min_bonus))
    raw_bear_str = (g_str_bear and bear_cooled and (s2_short >= min_str2) and any_trig_bear and (bon_s >= min_bonus))
    raw_bull_med = (g_med_bull and bull_cooled and (s2_long  >= min_str2) and any_trig_bull and (bon_l >= max(min_bonus-1,0)))
    raw_bear_med = (g_med_bear and bear_cooled and (s2_short >= min_str2) and any_trig_bear and (bon_s >= max(min_bonus-1,0)))
    raw_bull_sca = (g_sca_bull and bull_cooled and (s2_long  >= max(min_str2-1,1)) and any_trig_bull and (bon_l >= max(min_bonus-1,0)))
    raw_bear_sca = (g_sca_bear and bear_cooled and (s2_short >= max(min_str2-1,1)) and any_trig_bear and (bon_s >= max(min_bonus-1,0)))

    raw_bull = (raw_bull_str or
                (raw_bull_med and not raw_bull_str) or
                (raw_bull_sca and not raw_bull_str and not raw_bull_med))
    raw_bear = (raw_bear_str or
                (raw_bear_med and not raw_bear_str) or
                (raw_bear_sca and not raw_bear_str and not raw_bear_med))

    bull_tier = "STRONG" if raw_bull_str else ("MEDIUM" if raw_bull_med else ("SCALP" if raw_bull_sca else ""))
    bear_tier = "STRONG" if raw_bear_str else ("MEDIUM" if raw_bear_med else ("SCALP" if raw_bear_sca else ""))

    # --- SL / TP ---
    (long_sl, long_risk, long_tp, sl_src_bull,
     short_sl, short_risk, short_tp, sl_src_bear) = calc_structure_sl_tp(
        close, atr,
        t1_bull, t2_bull, t1_bear, t2_bear,
        bull_ob_bot, bear_ob_top,
        bull_fvg_bot, bear_fvg_top,
        df_main, wb_high, wb_low,
        bull_tier, bear_tier, rr=RR
    )

    # ── SCALP RR FILTER — matches Pine Script bullSig_final / bearSig_final ──
    # Block scalp signal if RR < 1.5 to the TP target
    bull_sig_final = raw_bull and check_scalp_rr_ok(close, long_tp,  long_sl,  bull_tier)
    bear_sig_final = raw_bear and check_scalp_rr_ok(close, short_tp, short_sl, bear_tier)

    # --- LOT SIZING ---
    account  = mt5.account_info()
    balance  = account.balance
    risk_amt = balance * RISK_PERCENT
    long_sz  = calc_lot_size(sym, risk_amt, long_risk)
    short_sz = calc_lot_size(sym, risk_amt, short_risk)

    # --- DST / STATUS PRINT ---
    dst_str    = get_dst_str()
    bias_h1, bias_h2 = get_pair_bias_hours(sym)
    pair_lbl   = f"{bias_h1}AM+{bias_h2}AM UTC"
    wk_status  = ("BULL" if wk_bull else "BEAR" if wk_bear else
                  f"PARTIAL({bias_h1}AM formed)" if wk_partial else "UNCONFIRMED")
    day_status = "BULL" if day_bull else "BEAR" if day_bear else "UNCONFIRMED"

    print(f"\n{'='*55}")
    print(f"📈 {sym} {tf_name} | {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC | {get_session()} | {dst_str}")
    print(f"⚡ Bias Hours: {pair_lbl} | WkBias:{wk_status} | DayBias:{day_status}")
    print(f"📊 Close:{close:.5f} EMA200:{ema200:.5f} | RSI:{rsi:.1f} ADX:{adx_val:.1f} ATR:{atr:.5f}")
    if wb_high:
        print(f"📦 WkRange:{wb_high:.5f}-{wb_low:.5f} | InRange:{price_in_weekly_range}")
    else:
        print(f"📦 WkRange: Not yet formed (waiting {bias_h1}AM UTC Monday)")
    if db_high:
        print(f"🟢 DayRange:{db_high:.5f}-{db_low:.5f}")
    else:
        print(f"🟢 DayRange: Not yet formed (waiting {bias_h1}AM UTC)")
    print(f"🔒 HTFBull:{htf_bull} HTFBear:{htf_bear} ExtBear:{ext_bear}")
    print(f"🏗 S2L:{s2_long}/5 S2S:{s2_short}/5 | BonL:{bon_l}/7 BonS:{bon_s}/7")
    print(f"🎯 Trig — Bull:{trig_name_bull}({any_trig_bull}) Bear:{trig_name_bear}({any_trig_bear})")
    print(f"⏳ Cooldown — Bull:{bars_since_bull}bars Bear:{bars_since_bear}bars")
    print(f"📰 News: {news_reason}")
    print(f"🚦 Raw — Bull:{raw_bull}({bull_tier}) Bear:{raw_bear}({bear_tier})")
    if bull_tier == "SCALP" and raw_bull:
        long_rr_check = (long_tp - close) / long_risk if long_risk > 0 else 0
        print(f"⚡ Scalp RR: {long_rr_check:.2f} ({'✅ OK' if long_rr_check >= SCALP_MIN_RR else '❌ BLOCKED'})")
    if bear_tier == "SCALP" and raw_bear:
        short_rr_check = (close - short_tp) / short_risk if short_risk > 0 else 0
        print(f"⚡ Scalp RR: {short_rr_check:.2f} ({'✅ OK' if short_rr_check >= SCALP_MIN_RR else '❌ BLOCKED'})")
    print(f"✅ Final — Bull:{bull_sig_final} Bear:{bear_sig_final}")
    print(f"📐 SL Source — Bull:{sl_src_bull} Bear:{sl_src_bear}")

    # --- FIRE SIGNAL ---
    if bull_sig_final:
        emoji = "🔥" if bull_tier == "STRONG" else "💪" if bull_tier == "MEDIUM" else "⚡"
        print(f"\n{emoji} BUY — {sym} {tf_name} — {bull_tier} — Trig:{trig_name_bull} SL:{sl_src_bull}")
        send_telegram(
            f"{emoji} <b>{bull_tier} BUY — {sym} {tf_name}</b>\n\n"
            f"📊 Manny's Strategy V3 Mixed Bias\n"
            f"🎯 Trigger: {trig_name_bull} | SL from: {sl_src_bull}\n"
            f"⚡ Bias: {pair_lbl} | {dst_str}\n"
            f"🧭 Weekly: {wk_status} | Daily: {day_status}\n"
            f"🏦 {get_session()}\n"
            f"📈 Entry: {close:.5f}\n"
            f"🛑 SL: {long_sl}\n"
            f"🎯 TP: {long_tp}\n"
            f"📦 Lots: {long_sz}\n"
            f"🏗 S2:{s2_long}/5 Bonus:{bon_l}/7\n"
            f"⏰ {datetime.now(timezone.utc).strftime('%H:%M')} UTC"
        )
        place_trade("BUY", long_sl, long_tp, long_sz, sym, tf_name)
        return current_bar, bar_index, last_bear_bar

    elif bear_sig_final:
        emoji = "🔥" if bear_tier == "STRONG" else "💪" if bear_tier == "MEDIUM" else "⚡"
        print(f"\n{emoji} SELL — {sym} {tf_name} — {bear_tier} — Trig:{trig_name_bear} SL:{sl_src_bear}")
        send_telegram(
            f"{emoji} <b>{bear_tier} SELL — {sym} {tf_name}</b>\n\n"
            f"📊 Manny's Strategy V3 Mixed Bias\n"
            f"🎯 Trigger: {trig_name_bear} | SL from: {sl_src_bear}\n"
            f"⚡ Bias: {pair_lbl} | {dst_str}\n"
            f"🧭 Weekly: {wk_status} | Daily: {day_status}\n"
            f"🏦 {get_session()}\n"
            f"📈 Entry: {close:.5f}\n"
            f"🛑 SL: {short_sl}\n"
            f"🎯 TP: {short_tp}\n"
            f"📦 Lots: {short_sz}\n"
            f"🏗 S2:{s2_short}/5 Bonus:{bon_s}/7\n"
            f"⏰ {datetime.now(timezone.utc).strftime('%H:%M')} UTC"
        )
        place_trade("SELL", short_sl, short_tp, short_sz, sym, tf_name)
        return current_bar, last_bull_bar, bar_index

    return current_bar, last_bull_bar, last_bear_bar

# ═══════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════
def run():
    print("🚀 Manny's Gold Strategy V3 — Mixed Bias Edition")
    print("✅ UPDATES IN THIS VERSION:")
    print("  ⚡ Per-pair hardcoded UTC bias hours (Gold 1AM+5AM | Indices 2AM+6AM)")
    print("  💪 Strong body confirmation (≥60%) for weekly AND daily bias")
    print("  📰 News blackout ±30min around NFP / CPI / FOMC")
    print("  ⚡ Scalp minimum 1.5R filter — bad RR scalps blocked")
    print("  🟦 Stateful OB memory between scans")
    print("  🎯 Structure SL: OB → FVG → Swing → ATR")
    print("="*55)

    h1, h2  = get_pair_bias_hours("XAUUSD")
    dst     = get_dst_str()
    print(f"🕐 {dst} | Gold bias hours: {h1}AM + {h2}AM UTC")
    print(f"📈 Scanning: {', '.join(SYMBOLS)}")

    send_telegram(
        f"🚀 <b>Manny's Strategy V3 — V3 Final Update</b>\n\n"
        f"✅ Per-pair bias hours (Gold 1AM+5AM | Indices 2AM+6AM)\n"
        f"✅ Strong body confirmation ≥60%\n"
        f"✅ News blackout ±30min NFP/CPI/FOMC\n"
        f"✅ Scalp min 1.5R filter active\n"
        f"📈 Pairs: {', '.join(SYMBOLS)}\n"
        f"⏱ Timeframes: 5M 15M 30M 1H 4H\n"
        f"🕐 {dst} | Gold: {h1}AM+{h2}AM UTC\n"
        f"🏦 London | Overlap | NY | Post-NY"
    )

    if not connect_mt5():
        print("Failed to connect to MT5")
        return

    print("✅ MT5 Connected!")

    ob_state     = {}
    signal_state = {}

    for s in SYMBOLS:
        for tf in SYMBOL_TIMEFRAMES.get(s, [mt5.TIMEFRAME_M30]):
            key = f"{s}_{TIMEFRAME_NAMES[tf]}"
            signal_state[key] = {
                "last_signal_bar": None,
                "last_bull_bar":   None,
                "last_bear_bar":   None
            }
            ob_state[key] = {'bull_ob': None, 'bear_ob': None}

    while True:
        now = datetime.now(timezone.utc)

        # Check news blackout for logging
        news_black, news_reason = is_news_blackout()

        if is_active_session():
            status = f"🚫 NEWS BLACKOUT: {news_reason}" if news_black else get_session()
            print(f"\n[{now.strftime('%H:%M:%S')} UTC] Scanning | {status}")
            for sym in SYMBOLS:
                for tf in SYMBOL_TIMEFRAMES.get(sym, [mt5.TIMEFRAME_M30]):
                    tf_name = TIMEFRAME_NAMES[tf]
                    key     = f"{sym}_{tf_name}"
                    try:
                        state = signal_state[key]
                        new_sig, new_bull, new_bear = check_signal(
                            state["last_signal_bar"],
                            state["last_bull_bar"],
                            state["last_bear_bar"],
                            symbol=sym,
                            timeframe=tf,
                            ob_state=ob_state
                        )
                        signal_state[key]["last_signal_bar"] = new_sig
                        signal_state[key]["last_bull_bar"]   = new_bull
                        signal_state[key]["last_bear_bar"]   = new_bear
                    except Exception as e:
                        print(f"❌ {sym} {tf_name} Error: {e}")
                        import traceback
                        traceback.print_exc()
                        send_telegram(f"⚠️ {sym} {tf_name} error: {e}")
        else:
            h1, _ = get_pair_bias_hours("XAUUSD")
            print(f"[{now.strftime('%H:%M:%S')} UTC] 💤 Asian — Next: London 07:00 UTC | Gold Bias: {h1}AM UTC")

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    run()
