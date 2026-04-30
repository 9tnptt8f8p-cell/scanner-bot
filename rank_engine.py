def rank_result(result):
    score = 0

    gain = result.get("gain", 0)
    volume = result.get("volume", 0)
    recent_vol = result.get("recent_volume", 0)
    news_quality = result.get("news_quality", "UNKNOWN")
    reasons = result.get("reasons", [])
    risks = result.get("risks", [])

    reasons_text = " ".join(reasons).lower()
    risks_text = " ".join(risks).lower()

    # Momentum
    if gain >= 100:
        score += 3
    elif gain >= 50:
        score += 2
    elif gain >= 20:
        score += 1

    # Volume
    if volume >= 10_000_000:
        score += 3
    elif volume >= 2_000_000:
        score += 2
    elif volume >= 500_000:
        score += 1

    if recent_vol >= 300_000:
        score += 2
    elif recent_vol >= 100_000:
        score += 1

    # Structure
    if "price above vwap" in reasons_text:
        score += 2

    if "breakout" in reasons_text:
        score += 2

    # News
    if news_quality == "STRONG":
        score += 2
    elif news_quality == "WEAK":
        score -= 1
    elif news_quality == "NEGATIVE":
        score -= 3

    # Risk penalties
    if "dilution" in risks_text or "offering" in risks_text or "warrant" in risks_text:
        score -= 3

    if "below vwap" in risks_text:
        score -= 2

    if "upper wick" in risks_text or "trap" in risks_text:
        score -= 2

    return max(0, score)
