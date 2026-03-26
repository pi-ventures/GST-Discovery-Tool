"""
Jamku GST Scraper
Fetches Trade Name, Legal Name, HSN Codes, Business Owners, Address from gst.jamku.app
Uses direct URL navigation (Nuxt.js app serves data at /gstin/{GSTIN}).
"""
import logging
import re
import json
import asyncio
import config

log = logging.getLogger("gst_discovery")


async def get_gst_details_from_jamku(page, gstin):
    """
    Navigate directly to gst.jamku.app/gstin/{GSTIN} and extract data
    from the __NUXT_DATA__ JSON blob embedded in the page.
    Returns: dict with trade_name, legal_name, hsn_codes, owners, address, phone, email.
    """
    try:
        log.info(f"  [Jamku] Looking up {gstin}...")

        # Navigate directly to the GSTIN detail page (Nuxt route)
        url = f"{config.JAMKU_GST_URL}gstin/{gstin}"
        await page.goto(url, timeout=config.BROWSER_TIMEOUT)
        await page.wait_for_load_state("networkidle", timeout=config.BROWSER_TIMEOUT)
        await asyncio.sleep(2)

        result = {
            "gstin": gstin,
            "trade_name": None,
            "legal_name": None,
            "hsn_codes": [],
            "business_owners": [],
            "address": None,
            "phone": None,
            "email": None,
            "source": "jamku",
        }

        # Method 1: Extract from __NUXT_DATA__ script tag (most reliable)
        nuxt_data = await _extract_nuxt_data(page)
        if nuxt_data:
            result["trade_name"] = nuxt_data.get("tradeName") or nuxt_data.get("tradeNam")
            result["legal_name"] = nuxt_data.get("lgnm")
            result["address"] = nuxt_data.get("adr")
            result["phone"] = nuxt_data.get("pn")
            result["email"] = nuxt_data.get("em")

            # HSN/SAC codes from nature of business or activities
            nba = nuxt_data.get("nba", [])
            if isinstance(nba, list):
                result["hsn_codes"] = nba

            # Business owners / proprietor
            pradr = nuxt_data.get("pradr", {})
            if isinstance(pradr, dict) and pradr.get("addr"):
                addr = pradr["addr"]
                if isinstance(addr, dict):
                    addr_parts = [addr.get(k, "") for k in ["bno", "st", "loc", "dst", "stcd", "pncd"]]
                    full_addr = ", ".join(p for p in addr_parts if p)
                    if full_addr:
                        result["address"] = full_addr

        # Method 2: Fallback - extract from rendered page text
        if not result["trade_name"]:
            body_text = await page.locator("body").inner_text()
            result = _extract_from_page_text(body_text, result)

        # Use phone from Jamku as mobile_number if found
        if result.get("phone"):
            result["mobile_number"] = result["phone"]

        log.info(f"  [Jamku] Trade: {result.get('trade_name', '-')}, "
                 f"Legal: {result.get('legal_name', '-')}, "
                 f"Phone: {result.get('phone', '-')}")
        return result

    except Exception as e:
        log.error(f"  [Jamku] Error for {gstin}: {e}")
        return None


async def _extract_nuxt_data(page):
    """Extract structured data from Nuxt.js __NUXT_DATA__ script tag.

    Nuxt 3 payload format: a JSON array where dict values are indices
    pointing to other items in the same array. We need to dereference them.
    """
    try:
        script = page.locator("script#__NUXT_DATA__")
        if await script.count() == 0:
            return None

        raw = await script.inner_text()
        if not raw:
            return None

        data_array = json.loads(raw)
        if not isinstance(data_array, list):
            return None

        def resolve(index):
            """Recursively resolve a Nuxt payload index to its actual value."""
            if not isinstance(index, int) or index < 0 or index >= len(data_array):
                return index  # already a literal value
            val = data_array[index]
            if isinstance(val, dict):
                return {k: resolve(v) for k, v in val.items()}
            if isinstance(val, list):
                return [resolve(item) for item in val]
            return val  # string, number, bool, null

        # Find the schema dict that has GST fields (tradeName, lgnm, pn, etc.)
        target_keys = {"tradeName", "lgnm", "gstin", "pn", "em", "adr", "pincode", "sts", "hsn"}
        for item in data_array:
            if isinstance(item, dict):
                matching = set(item.keys()) & target_keys
                if len(matching) >= 3:
                    # This is the schema dict - resolve all its index references
                    resolved = {k: resolve(v) for k, v in item.items()}
                    return resolved

        return None

    except Exception as e:
        log.debug(f"  [Jamku] Nuxt data parse error: {e}")
        return None


def _extract_from_page_text(body_text, result):
    """Fallback: extract GST details from rendered page text."""
    lines = body_text.split("\n")

    for i, line in enumerate(lines):
        line_clean = line.strip()
        line_lower = line_clean.lower()

        if "trade name" in line_lower and not result["trade_name"]:
            val = _get_next_value(line_clean, lines, i)
            if val:
                result["trade_name"] = val

        elif "legal name" in line_lower and not result["legal_name"]:
            val = _get_next_value(line_clean, lines, i)
            if val:
                result["legal_name"] = val

        elif "address" in line_lower and not result["address"]:
            val = _get_next_value(line_clean, lines, i)
            if val:
                result["address"] = val

        elif any(kw in line_lower for kw in ["proprietor", "partner", "director", "karta"]):
            val = _get_next_value(line_clean, lines, i)
            if val and len(val) > 2:
                result["business_owners"].append(val)

        # HSN/SAC codes
        hsn_matches = re.findall(r'\b\d{4,8}\b', line_clean)
        if hsn_matches and ("hsn" in line_lower or "sac" in line_lower):
            result["hsn_codes"].extend(hsn_matches)

    return result


def _get_next_value(line, lines, index):
    """Get value from 'Label: Value' or the next line."""
    if ":" in line:
        val = line.split(":", 1)[1].strip()
        if val:
            return val
    if index + 1 < len(lines):
        next_line = lines[index + 1].strip()
        if next_line and len(next_line) > 1:
            return next_line
    return None
