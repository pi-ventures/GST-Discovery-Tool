"""
KnowYourGST Scraper
Searches firm name to discover related GSTINs across states.
URL: https://www.knowyourgst.com/gst-number-search/by-name-pan/
"""
import logging
import re
import csv
import os
import time
import config

log = logging.getLogger("gst_discovery")


async def discover_related_gstins(page, firm_name, output_dir=None):
    """
    Search first one or two words of firm name on KnowYourGST
    and collect all matching GSTINs with state locations.
    Returns: list of dicts with gstin, trade_name, state, status.
    """
    try:
        # Use first 1-2 words of firm name for broader search
        firm_name = str(firm_name) if firm_name else ""
        words = firm_name.strip().split()
        search_term = " ".join(words[:2]) if len(words) >= 2 else words[0]

        log.info(f"  [KnowYourGST] Searching: '{search_term}'...")
        await page.goto(config.KNOWYOURGST_URL, timeout=config.BROWSER_TIMEOUT)
        await page.wait_for_load_state("networkidle", timeout=config.BROWSER_TIMEOUT)

        # Find search input
        search_input = page.locator(
            "input[type='text'], input[name*='search'], "
            "input[placeholder*='name'], input[placeholder*='PAN']"
        ).first
        await search_input.fill(search_term)

        # Click search button
        search_btn = page.locator(
            "button[type='submit'], input[type='submit'], "
            "button:text-matches('Search', 'i')"
        ).first
        await search_btn.click()

        await page.wait_for_load_state("networkidle", timeout=config.BROWSER_TIMEOUT)
        time.sleep(3)

        results = []

        # Extract results from table
        rows = await page.locator("table tbody tr, .search-result-row").all()
        for row in rows:
            cells = await row.locator("td").all()
            if len(cells) >= 2:
                texts = [await c.inner_text() for c in cells]
                gstin = None
                trade_name = None
                state = None
                status = None

                for text in texts:
                    text = text.strip()
                    # GSTIN pattern: 2 digits + 10 char PAN + 1 digit + Z + 1 char
                    if re.match(r'^\d{2}[A-Z]{5}\d{4}[A-Z]\d[A-Z\d][A-Z]\d$', text):
                        gstin = text
                    elif len(text) > 3 and not gstin:
                        trade_name = text

                if gstin:
                    # Derive state from first 2 digits of GSTIN
                    state_code = gstin[:2]
                    state = GST_STATE_CODES.get(state_code, state_code)
                    results.append({
                        "gstin": gstin,
                        "trade_name": trade_name or "",
                        "state": state,
                        "status": status or "active",
                    })

        # Try pagination - get more results
        while True:
            next_btn = page.locator(
                "a:text-matches('Next', 'i'), button:text-matches('Next', 'i'), "
                ".pagination a:last-child"
            )
            if await next_btn.count() > 0:
                try:
                    await next_btn.first.click()
                    await page.wait_for_load_state("networkidle", timeout=config.BROWSER_TIMEOUT)
                    time.sleep(2)

                    rows = await page.locator("table tbody tr").all()
                    new_count = 0
                    for row in rows:
                        cells = await row.locator("td").all()
                        if len(cells) >= 2:
                            texts = [await c.inner_text() for c in cells]
                            gstin = None
                            trade_name = None
                            for text in texts:
                                text = text.strip()
                                if re.match(r'^\d{2}[A-Z]{5}\d{4}[A-Z]\d[A-Z\d][A-Z]\d$', text):
                                    gstin = text
                                elif len(text) > 3 and not gstin:
                                    trade_name = text
                            if gstin:
                                state = GST_STATE_CODES.get(gstin[:2], gstin[:2])
                                results.append({
                                    "gstin": gstin,
                                    "trade_name": trade_name or "",
                                    "state": state,
                                })
                                new_count += 1
                    if new_count == 0:
                        break
                except Exception:
                    break
            else:
                break

        log.info(f"  [KnowYourGST] Found {len(results)} related GSTINs")

        # Save to CSV
        if results and output_dir:
            csv_path = os.path.join(
                output_dir,
                f"related_gstins_{search_term.replace(' ', '_')}.csv"
            )
            os.makedirs(output_dir, exist_ok=True)
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["gstin", "trade_name", "state", "status"])
                writer.writeheader()
                writer.writerows(results)
            log.info(f"  [KnowYourGST] Saved to {csv_path}")

        return results

    except Exception as e:
        log.error(f"  [KnowYourGST] Error searching '{firm_name}': {e}")
        return []


# GST State Codes mapping
GST_STATE_CODES = {
    "01": "Jammu & Kashmir", "02": "Himachal Pradesh", "03": "Punjab",
    "04": "Chandigarh", "05": "Uttarakhand", "06": "Haryana",
    "07": "Delhi", "08": "Rajasthan", "09": "Uttar Pradesh",
    "10": "Bihar", "11": "Sikkim", "12": "Arunachal Pradesh",
    "13": "Nagaland", "14": "Manipur", "15": "Mizoram",
    "16": "Tripura", "17": "Meghalaya", "18": "Assam",
    "19": "West Bengal", "20": "Jharkhand", "21": "Odisha",
    "22": "Chhattisgarh", "23": "Madhya Pradesh", "24": "Gujarat",
    "26": "Dadra & Nagar Haveli and Daman & Diu",
    "27": "Maharashtra", "29": "Karnataka", "30": "Goa",
    "31": "Lakshadweep", "32": "Kerala", "33": "Tamil Nadu",
    "34": "Puducherry", "35": "Andaman & Nicobar Islands",
    "36": "Telangana", "37": "Andhra Pradesh",
}
