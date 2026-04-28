def build_alert(data):
    emoji = data.get("emoji", "🚨")
    ticker = data.get("ticker")
    score = data.get("score")
    rank = data.get("rank")

    price = data.get("price")
    gain = data.get("gain")

    volume = data.get("volume")
    candle_vol = data.get("candle_vol")

    catalyst = data.get("catalyst", "none")
    reasons = data.get("reasons", [])
    risks = data.get("risks", [])

    session = data.get("session", "UNKNOWN")
    regime = data.get("regime", "UNKNOWN")

    reasons_text = "\n- ".join(reasons)
    risks_text = "\n- ".join(risks)

    msg = f"""
{emoji} ALERT

Rank: #{rank}
{ticker} | Score: {score}/10

Price: ${price}
Gain: +{gain}%

Volume: {volume}
Recent Candle Vol: {candle_vol}

Catalyst: {catalyst}

Reasons:
- {reasons_text}

Risk:
- {risks_text}

🕒 SESSION: {session}
📊 MARKET REGIME: {regime}
"""
    return msg.strip()
