"""
TGCT Portal Scraper - Get mobile number from GST dealer search.
URL: https://www.tgct.gov.in/tgportal/GST/GST_Dealer_Search.aspx
"""
import logging
import re
import asyncio
import config

log = logging.getLogger("gst_discovery")


async def get_mobile_from_tgct(page, gstin):
    """
    Enter GSTIN on TGCT portal, submit, and extract mobile number.
    Uses exact ASP.NET element IDs from the actual page.
    Returns: dict with mobile_number, dealer_name, address or None on failure.
    """
    try:
        log.info(f"  [TGCT] Looking up {gstin}...")
        await page.goto(config.TGCT_GST_URL, timeout=config.BROWSER_TIMEOUT)
        await page.wait_for_load_state("networkidle", timeout=config.BROWSER_TIMEOUT)

        # Exact ASP.NET element IDs from the page
        gstin_input = page.locator("#ContentPlaceHolder2_txtGSTIN")
        await gstin_input.fill(gstin)

        # Click the Search button
        search_btn = page.locator("#ContentPlaceHolder2_btnSearch")
        await search_btn.click()

        # Wait for postback response
        await page.wait_for_load_state("networkidle", timeout=config.BROWSER_TIMEOUT)
        await asyncio.sleep(2)

        result = {"mobile_number": None, "source": "tgct"}

        # Check for error message first
        error_label = page.locator("#ContentPlaceHolder2_lblError")
        if await error_label.count() > 0:
            error_text = (await error_label.inner_text()).strip()
            if error_text:
                log.warning(f"  [TGCT] Error from portal: {error_text}")
                return result

        # Extract dealer details panel (rendered after postback)
        # Look for the results panel
        panel = page.locator("#ContentPlaceHolder2_PnlDlrDtls")
        if await panel.count() > 0:
            panel_text = await panel.inner_text()
            # Extract mobile from panel
            mobile_match = re.findall(r'(?:Mobile|Phone|Contact)\s*:?\s*([6-9]\d{9})', panel_text, re.IGNORECASE)
            if mobile_match:
                result["mobile_number"] = mobile_match[0]

        # Also check GridView for results
        grid = page.locator("#ContentPlaceHolder2_gridforlt")
        if await grid.count() > 0:
            rows = await grid.locator("tr").all()
            for row in rows:
                text = await row.inner_text()
                text_lower = text.lower()

                if "mobile" in text_lower or "phone" in text_lower or "contact" in text_lower:
                    phones = re.findall(r'[6-9]\d{9}', text)
                    if phones:
                        result["mobile_number"] = phones[0]

                if "trade" in text_lower and "name" in text_lower:
                    parts = text.split("\t")
                    if len(parts) >= 2:
                        result["dealer_name"] = parts[-1].strip()

                if "address" in text_lower:
                    parts = text.split("\t")
                    if len(parts) >= 2:
                        result["address"] = parts[-1].strip()

        # Fallback: scan full page content for mobile, but ONLY within result areas
        if not result["mobile_number"]:
            # Get only the main content area, excluding header/footer
            content_area = page.locator("#ContentPlaceHolder2_PnlDlrDtls, #ContentPlaceHolder2_gridforlt, .panel-body, .result-panel")
            if await content_area.count() > 0:
                content_text = await content_area.first.inner_text()
                phones = re.findall(r'[6-9]\d{9}', content_text)
                if phones:
                    result["mobile_number"] = phones[0]

        if result["mobile_number"]:
            log.info(f"  [TGCT] Found mobile: {result['mobile_number']}")
        else:
            log.warning(f"  [TGCT] No mobile found for {gstin}")

        return result

    except Exception as e:
        log.error(f"  [TGCT] Error for {gstin}: {e}")
        return None
