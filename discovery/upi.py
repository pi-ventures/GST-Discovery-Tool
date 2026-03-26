"""
UPI Name Verifier
Attempts to verify mobile numbers against UPI to fetch registered name.
Returns blank if not found — many numbers won't be on UPI, and that's fine.
"""
import logging
import re
import asyncio
import config

log = logging.getLogger("gst_discovery")

# Short timeout for UPI — don't waste time if it doesn't resolve quickly
UPI_TIMEOUT = 10000  # 10 seconds


async def verify_upi_name(page, mobile_number):
    """
    Quick UPI name lookup. Returns dict with upi_name or None.
    Fails fast and returns blank — not all numbers are on UPI.
    """
    if not mobile_number:
        return None

    mobile = str(mobile_number).strip()
    if len(mobile) != 10 or not mobile.isdigit():
        return None

    # Try Paytm, then PhonePe — return on first success
    for platform, upi_suffix in [("paytm", "paytm"), ("ybl", "ybl")]:
        name = await _quick_upi_check(page, mobile, upi_suffix, platform)
        if name:
            return {"upi_name": name, "platform": platform, "upi_id": f"{mobile}@{upi_suffix}"}

    # Not found — that's OK, return blank
    log.debug(f"  [UPI] No UPI name for {mobile} — returning blank")
    return None


async def _quick_upi_check(page, mobile, upi_suffix, platform):
    """Quick check with short timeout. Returns name string or None."""
    try:
        upi_id = f"{mobile}@{upi_suffix}"

        if platform == "paytm":
            url = f"https://paytm.com/send-money?recipient={upi_id}"
        else:
            url = f"https://phon.pe/pay?upi={upi_id}"

        await page.goto(url, timeout=UPI_TIMEOUT)
        await page.wait_for_load_state("domcontentloaded", timeout=UPI_TIMEOUT)
        await asyncio.sleep(1)

        body_text = await page.locator("body").inner_text()
        return _extract_upi_name(body_text)

    except Exception:
        return None


def _extract_upi_name(text):
    """Extract a person name from UPI verification text."""
    patterns = [
        r'(?:Sending to|Pay|Paying|Name|Verified)\s*:?\s*([A-Z][A-Za-z\s\.]+)',
        r'(?:Account holder|Beneficiary)\s*:?\s*([A-Z][A-Za-z\s\.]+)',
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            name = match.group(1).strip()
            if len(name.split()) >= 2 and len(name) < 50:
                skip_words = ["send money", "bank account", "upi id", "mobile number"]
                if not any(sw in name.lower() for sw in skip_words):
                    return name

    return None
