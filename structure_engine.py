def calculate_vwap(candles):
    total_pv = 0
    total_vol = 0

    for c in candles:
        high = float(c["high"])
        low = float(c["low"])
        close = float(c["close"])
        volume = float(c["volume"])

        typical_price = (high + low + close) / 3
        total_pv += typical_price * volume
        total_vol += volume

    if total_vol == 0:
        return None

    return total_pv / total_vol


def detect_higher_lows(candles, lookback=8):
    recent = candles[-lookback:]

    lows = [float(c["low"]) for c in recent]

    if len(lows) < 4:
        return False

    higher_count = 0

    for i in range(1, len(lows)):
        if lows[i] > lows[i - 1]:
            higher_count += 1

    return higher_count >= 4


def detect_breakout(candles, lookback=20):
    if len(candles) < lookback + 1:
        return False, None

    previous = candles[-lookback:-1]
    current = candles[-1]

    resistance = max(float(c["high"]) for c in previous)
    current_close = float(current["close"])

    if current_close > resistance:
        return True, resistance

    return False, resistance


def candle_strength(candle):
    open_price = float(candle["open"])
    high = float(candle["high"])
    low = float(candle["low"])
    close = float(candle["close"])

    candle_range = high - low
    body = abs(close - open_price)

    if candle_range == 0:
        return "neutral"

    close_position = (close - low) / candle_range
    upper_wick = high - max(open_price, close)

    if close_position >= 0.75 and body / candle_range >= 0.45:
        return "strong"

    if upper_wick / candle_range >= 0.45:
        return "wicky_trap"

    if close_position <= 0.35:
        return "weak"

    return "neutral"


def analyze_structure(ticker, candles):
    if not candles or len(candles) < 20:
        return {
            "ticker": ticker,
            "structure_score": 0,
            "above_vwap": False,
            "vwap": None,
            "breakout": False,
            "breakout_level": None,
            "higher_lows": False,
            "candle_strength": "not_enough_data",
            "reasons": ["Not enough candle data"],
            "risk_flags": []
        }

    current = candles[-1]
    current_price = float(current["close"])

    vwap = calculate_vwap(candles)
    above_vwap = vwap is not None and current_price > vwap

    breakout, breakout_level = detect_breakout(candles)
    higher_lows = detect_higher_lows(candles)
    strength = candle_strength(current)

    score = 0
    reasons = []
    risk_flags = []

    if above_vwap:
        score += 2
        reasons.append("Price above VWAP")
    else:
        risk_flags.append("Below VWAP")

    if breakout:
        score += 2
        reasons.append(f"Breakout above ${breakout_level:.2f}")

    if higher_lows:
        score += 2
        reasons.append("Higher lows forming")

    if strength == "strong":
        score += 2
        reasons.append("Strong candle close near high")

    if strength == "wicky_trap":
        score -= 2
        risk_flags.append("Big upper wick / possible trap")

    if strength == "weak":
        score -= 1
        risk_flags.append("Weak candle close")

    return {
        "ticker": ticker,
        "structure_score": score,
        "above_vwap": above_vwap,
        "vwap": round(vwap, 4) if vwap else None,
        "breakout": breakout,
        "breakout_level": round(breakout_level, 4) if breakout_level else None,
        "higher_lows": higher_lows,
        "candle_strength": strength,
        "reasons": reasons,
        "risk_flags": risk_flags
    }
