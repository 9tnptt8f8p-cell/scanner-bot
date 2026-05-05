def build_alert(data):
    emoji = data.get("emoji", "🚨")
    title = data.get("title", "ALERT")

    ticker = data.get("ticker")
    score = data.get("score", 0)
    price = data.get("price", 0)
    gain = data.get("gain", 0)

    rank = data.get("rank", "?")
    rank_score = data.get("rank_score", "?")

    catalyst = data.get("catalyst", "none")
    catalyst_type = data.get("catalyst_type", catalyst)
    news_quality = data.get("news_quality", "UNKNOWN")

    headline = data.get("headline") or data.get("catalyst_text") or "No headline found"
    headline = headline[:120] + "..." if len(headline) > 120 else headline

    reasons = data.get("reasons", [])
    risks = data.get("risks", [])

    regime = data.get("regime", "UNKNOWN")
    trade_bias = data.get("trade_bias", "🤔 Mixed/unclear")
    session = data.get("session", "").upper()

    reasons_text = "\n- ".join(reasons) if reasons else "None"
    risks_text = "\n- ".join(risks) if risks else "None"

    session_block = ""
    if session == "PREMARKET":
        session_block = """
🕒 PREMARKET

⚠️ DO NOT TRADE THIS YET
→ Build watchlist only
→ Wait for open setup
"""
    setup_tag = ""

    # 🟢 VWAP RECLAIM TAG (ONLY IF VALID)
    if "Price above VWAP" in reasons:
        setup_tag += "🟢 VWAP RECLAIM\n"

    # 🌀 COIL BREAKOUT TAG
    if data.get("alert_type") == "SECOND_LEG" or data.get("coil_breakout"):
        setup_tag += "🌀 SECOND LEG COIL — continuation setup\n"

    msg = f"""
{emoji} {title}
{session_block}

Rank: #{rank}
{ticker} | Score: {score}/10 | Rank: {rank_score}/10
{setup_tag.strip()}
Price: ${price}
Gain: +{gain}%

Catalyst: {catalyst_type}
Headline: {headline}
News Quality: {news_quality}

Reasons:
- {reasons_text}

Risk:
- {risks_text}

📊 MARKET REGIME: {regime}

Bias: {trade_bias}
"""

    return msg.strip()

