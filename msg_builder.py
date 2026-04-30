def build_alert(data):
    emoji = data.get("emoji", "🚨")
    ticker = data.get("ticker")
    score = data.get("score")
    rank = data.get("rank")
    rank_score = data.get("rank_score", 0)
    price = data.get("price")
    gain = data.get("gain")

    catalyst = data.get("catalyst", "none")
    catalyst_type = data.get("catalyst_type", catalyst)
    news_quality = data.get("news_quality", "UNKNOWN")

    reasons = data.get("reasons", [])
    risks = data.get("risks", [])

    regime = data.get("regime", "UNKNOWN")
    trade_bias = data.get("trade_bias", "🤔 Mixed/unclear")

    session = data.get("session", "").upper()

    # --- TEXT FORMATTING ---
    reasons_text = "\n- ".join(reasons) if reasons else "None"
    risks_text = "\n- ".join(risks) if risks else "None"

    # --- SESSION BLOCK (NEW 🔥) ---
    session_block = ""
    if session == "PREMARKET":
        session_block = """
🕒 PREMARKET

⚠️ DO NOT TRADE THIS YET
→ Build watchlist only
→ Wait for open setup
"""

    # --- MESSAGE BUILD ---
    msg = f"""
{emoji} ALERT
{session_block}

Rank: #{rank}
{ticker} | Score: {score}/10 | Rank: {rank_score}/10

Price: ${price}
Gain: +{gain}%

Catalyst: {catalyst_type}
News Quality: {news_quality}

Reasons:
- {reasons_text}

Risk:
- {risks_text}

📊 MARKET REGIME: {regime}

Bias: {trade_bias}
"""

    return msg.strip()
