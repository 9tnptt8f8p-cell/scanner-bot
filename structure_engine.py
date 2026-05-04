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
            "trend_builder": False,
            "candle_strength": "not_enough_data",
            "reasons": ["Not enough candle data"],
            "risk_flags": []
        }

    current = candles[-1]
    current_price = float(current["close"])

    vwap = calculate_vwap(candles)
    above_vwap = vwap is not None and current_price > (vwap * 0.995)
    breakout, breakout_level = detect_breakout(candles)
    higher_lows = detect_higher_lows(candles)
    strength = candle_strength(current)

    score = 0
    reasons = []
    risk_flags = []
    trend_builder = False

    if above_vwap:
        score += 2
        reasons.append("Price above VWAP")
    else:
        if vwap and current_price < (vwap * 0.99):
            risk_flags.append("Clear below VWAP")

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

    # --- TREND BUILDER DETECTION ---
    try:
        strong_above_vwap = current_price > (vwap * 0.995) if vwap else False

        highs = [float(c["high"]) for c in candles[-5:]]
        lows = [float(c["low"]) for c in candles[-5:]]

        recent_high = max(highs)
        recent_low = min(lows)
        range_pct = (recent_high - recent_low) / current_price if current_price > 0 else 999

        tight_range = range_pct < 0.03
        higher_lows_ok = higher_lows or "Higher lows forming" in reasons

        if strong_above_vwap and higher_lows_ok and tight_range:
            trend_builder = True
            reasons.append("Trend builder: tight consolidation above VWAP")

    except Exception:
        trend_builder = False

    return {
        "ticker": ticker,
        "structure_score": score,
        "above_vwap": above_vwap,
        "vwap": round(vwap, 4) if vwap else None,
        "breakout": breakout,
        "breakout_level": round(breakout_level, 4) if breakout_level else None,
        "higher_lows": higher_lows,
        "trend_builder": trend_builder,
        "candle_strength": strength,
        "reasons": reasons,
        "risk_flags": risk_flags
    }
