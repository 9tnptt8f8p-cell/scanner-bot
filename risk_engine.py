from datetime import datetime

def detect_dilution_type(text):
    t = (text or "").lower()

    if "at the market" in t or "atm" in t:
        return "ATM"
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

    dtype = detect_dilution_type(filing_text)

    if dtype and filing_date:
        days = (datetime.now() - filing_date).days
        level = dilution_risk_level(days)

        risks.append(f"{dtype} filed {days}d ago — {level} dilution risk")

    return risks
