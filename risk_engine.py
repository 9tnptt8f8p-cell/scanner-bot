from datetime import datetime

def detect_dilution_type(text):
    t = (text or "").lower()

    if "at the market" in t or "atm" in t:
        return "ATM"
    if "registered direct" in t:
        return "DIRECT OFFERING"
    if "private placement" in t:
        return "PRIVATE PLACEMENT"
    if "securities purchase agreement" in t:
        return "SPA"
    if "warrant" in t:
        return "WARRANTS"
    if "offering" in t:
        return "OFFERING"
    if "shelf" in t or "s-3" in t:
        return "SHELF"

    return None


def dilution_risk_level(days):
    if days <= 3:
        return "HIGH"
    elif days <= 10:
        return "MEDIUM"
    else:
        return "LOW"


def build_risk(filing_text, filing_date):
    risks = []

    text = (filing_text or "").lower()

    dtype = detect_dilution_type(text)

    # --- TIME-BASED DILUTION ---
    if dtype and filing_date:
        days = (datetime.now() - filing_date).days
        level = dilution_risk_level(days)

        risks.append(f"🚨 {dtype} filed {days}d ago — {level} dilution risk")

    # --- STRONG REAL DILUTION FLAGS ---
    if any(x in text for x in [
        "at-the-market",
        "atm program",
        "registered direct offering",
        "private placement",
        "securities purchase agreement",
        "equity line"
    ]):
        risks.append("💣 ACTIVE DILUTION PROGRAM")

    # --- WARRANT OVERHANG ---
    if "warrant" in text or "exercise price" in text:
        risks.append("⚠️ WARRANT OVERHANG — check strike")

    # --- RESALE / 424B3 ---
    if any(x in text for x in ["424b3", "resale", "selling stockholder"]):
        risks.append("⚠️ RESALE / UNLOCK SHARES")

    return risks
