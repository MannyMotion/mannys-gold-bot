import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import time
import requests
from datetime import datetime

load_dotenv()

# --- CREDENTIALS ---
MT5_LOGIN = int(os.getenv("MT5_LOGIN"))
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER = os.getenv("MT5_SERVER")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- SETTINGS ---
SYMBOL = "XAUUSD"
TIMEFRAME = mt5.TIMEFRAME_M30
RISK_PERCENT = 0.01
RR = 3.0
CHECK_INTERVAL = 60

# --- SESSIONS (UTC) ---
LONDON_START = 7
LONDON_END = 12
NY_START = 13
NY_END = 17

# --- TELEGRAM ---
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"Telegram error: {e}")

# --- CHECK SESSION ---
def is_active_session():
    now = datetime.utcnow()
    hour = now.hour
    is_london = LONDON_START <= hour < LONDON_END
    is_ny = NY_START <= hour < NY_END
    return is_london or is_ny

# --- CONNECT MT5 ---
def connect_mt5():
    if not mt5.initialize(
        login=MT5_LOGIN,
        password=MT5_PASSWORD,
        server=MT5_SERVER
    ):
        print("MT5 connection failed:", mt5.last_error())
        return False
    return True

# --- GET CANDLE DATA ---
def get_candles(symbol, timeframe, count=200):
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, count)
    if rates is None:
        return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    return df

# --- CALCULATE EMA ---
def calculate_ema(df, period):
    return df['close'].ewm(span=period, adjust=False).mean()

# --- CALCULATE RSI ---
def calculate_rsi(df, period=14):
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(span=period).mean()
    avg_loss = loss.ewm(span=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# --- CALCULATE ATR ---
def calculate_atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = abs(df['high'] - df['close'].shift())
    low_close = abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.ewm(span=period).mean()

# --- POSITION SIZING ---
def calculate_lot_size(balance, sl_distance):
    risk_amount = balance * RISK_PERCENT
    lot_size = round(risk_amount / (sl_distance * 100), 2)
    return max(0.01, lot_size)

# --- PLACE TRADE ---
def place_trade(signal, sl_distance):
    account = mt5.account_info()
    balance = account.balance
    tick = mt5.symbol_info_tick(SYMBOL)

    if signal == "BUY":
        order_type = mt5.ORDER_TYPE_BUY
        price = tick.ask
        sl = round(price - sl_distance, 2)
        tp = round(price + (sl_distance * RR), 2)
    else:
        order_type = mt5.ORDER_TYPE_SELL
        price = tick.bid
        sl = round(price + sl_distance, 2)
        tp = round(price - (sl_distance * RR), 2)

    lot_size = calculate_lot_size(balance, sl_distance)

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": SYMBOL,
        "volume": lot_size,
        "type": order_type,
        "price": price,
        "sl": sl,
        "tp": tp,
        "deviation": 20,
        "magic": 234000,
        "comment": "Mannys Gold Bot",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request)

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        msg = (
            f"❌ <b>TRADE FAILED</b>\n"
            f"Signal: {signal}\n"
            f"Error: {result.comment}"
        )
        send_telegram(msg)
        print(f"Trade failed: {result.comment}")
    else:
        msg = (
            f"🥇 <b>TRADE PLACED!</b>\n\n"
            f"📊 Signal: {signal}\n"
            f"💰 Symbol: {SYMBOL}\n"
            f"📈 Entry: {result.price}\n"
            f"🛑 Stop Loss: {sl}\n"
            f"🎯 Take Profit: {tp}\n"
            f"📦 Lot Size: {lot_size}\n"
            f"💼 Balance: {balance} GBP\n"
            f"⏰ Time: {datetime.utcnow().strftime('%H:%M:%S')} UTC"
        )
        send_telegram(msg)
        print(f"✅ Trade placed: {signal} at {result.price}")

# --- MAIN STRATEGY CHECK ---
def check_signal(last_signal_bar):
    df = get_candles(SYMBOL, TIMEFRAME, 200)
    if df is None or len(df) < 50:
        print("Not enough data")
        return last_signal_bar

    # Calculate indicators
    df['ema200'] = calculate_ema(df, 200)
    df['ema50'] = calculate_ema(df, 50)
    df['rsi'] = calculate_rsi(df, 14)
    df['atr'] = calculate_atr(df, 14)

    # Get last closed candle
    last = df.iloc[-2]
    current_bar = last['time']

    # Cooldown — don't fire same bar twice
    if last_signal_bar == current_bar:
        print(f"⏳ Cooldown active | Bar: {current_bar}")
        return last_signal_bar

    close = last['close']
    ema200 = last['ema200']
    ema50 = last['ema50']
    rsi = last['rsi']
    atr = last['atr']

    # ATR based SL distance
    sl_distance = round(atr * 1.5, 2)
    sl_distance = max(8.0, sl_distance)

    # Bias
    bull_bias = close > ema200
    bear_bias = close < ema200

    # BUY conditions
    buy_signal = (
        bull_bias and
        rsi < 65 and
        rsi > 35 and
        close > ema50 and
        last['close'] > last['open']
    )

    # SELL conditions
    sell_signal = (
        bear_bias and
        rsi > 35 and
        rsi < 65 and
        close < ema50 and
        last['close'] < last['open']
    )

    print(f"📊 Close: {close:.2f} | EMA200: {ema200:.2f} | EMA50: {ema50:.2f} | RSI: {rsi:.1f} | ATR: {atr:.2f}")
    print(f"🔍 Bull Bias: {bull_bias} | Buy Signal: {buy_signal} | Sell Signal: {sell_signal}")

    if buy_signal:
        print(f"🟢 BUY Signal!")
        place_trade("BUY", sl_distance)
        return current_bar
    elif sell_signal:
        print(f"🔴 SELL Signal!")
        place_trade("SELL", sl_distance)
        return current_bar

    return last_signal_bar

# --- MAIN LOOP ---
def run():
    print("🚀 Mannys Gold Scanner Starting...")
    send_telegram(
        "🚀 <b>Mannys Gold Scanner Started!</b>\n"
        "📊 Scanning XAUUSD 30M every 60 seconds\n"
        "⏰ Active: London 07-12 UTC | NY 13-17 UTC"
    )

    if not connect_mt5():
        print("Failed to connect to MT5")
        return

    print("✅ MT5 Connected!")
    last_signal_bar = None

    while True:
        now = datetime.utcnow()

        if is_active_session():
            print(f"\n[{now.strftime('%H:%M:%S')} UTC] 🔍 Scanning...")
            last_signal_bar = check_signal(last_signal_bar)
        else:
            print(f"[{now.strftime('%H:%M:%S')} UTC] 💤 Outside session. Next: London 07:00 UTC")

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    run()
