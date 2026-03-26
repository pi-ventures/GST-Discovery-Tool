"""
Zaubacorp / Director Scraper
Fetches proper company name and current/previous directors for Pvt Ltd / LLP companies.
Sources: zaubacorp.com (via Playwright to bypass 403), tofler.in
"""
import logging
import re
import json
import asyncio
import config

log = logging.getLogger("gst_discovery")


async def get_directors_from_zaubacorp(page, company_name, cin=None):
    """
    Search company on Zaubacorp and extract director details.
    Uses Playwright browser to bypass Cloudflare/403 blocks.
    Returns: dict with proper_name, directors list, cin.
    """
    try:
        log.info(f"  [Zaubacorp] Looking up: {company_name}")

        result = {
            "proper_name": None,
            "cin": cin,
            "directors": [],
            "source": "zaubacorp",
        }

        # Search by CIN if available (more reliable)
        if cin:
            search_query = cin
        else:
            # Clean company name for search
            search_query = re.sub(r'\s+', '+', company_name.strip())

        search_url = f"{config.ZAUBACORP_URL}/companysearchresults/{search_query}"
        await page.goto(search_url, timeout=config.BROWSER_TIMEOUT)
        await page.wait_for_load_state("networkidle", timeout=config.BROWSER_TIMEOUT)
        await asyncio.sleep(2)

        # Check if we landed on a company page directly or search results
        current_url = page.url

        if "/company/" in current_url and "/companysearch" not in current_url:
            # Already on company page
            pass
        else:
            # On search results page - click first result
            # Results are in table#results tbody tr td a
            first_link = page.locator("table#results tbody tr td a").first
            if await first_link.count() > 0:
                await first_link.click()
                await page.wait_for_load_state("networkidle", timeout=config.BROWSER_TIMEOUT)
                await asyncio.sleep(1)
            else:
                # Try alternative selectors
                first_link = page.locator("a[href*='/company/']").first
                if await first_link.count() > 0:
                    await first_link.click()
                    await page.wait_for_load_state("networkidle", timeout=config.BROWSER_TIMEOUT)
                    await asyncio.sleep(1)
                else:
                    log.warning(f"  [Zaubacorp] No results for {company_name}")
                    return result

        # Now on company detail page - extract company name
        h1 = page.locator("h1")
        if await h1.count() > 0:
            result["proper_name"] = (await h1.first.inner_text()).strip()

        # Extract CIN from page content
        content = await page.content()
        cin_match = re.search(r'[UL]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}', content)
        if cin_match:
            result["cin"] = cin_match.group(0)

        # Navigate to directors page
        # URL pattern: /company-directors/{slug}/{CIN}
        company_url = page.url
        directors_url = company_url.replace("/company/", "/company-directors/")
        await page.goto(directors_url, timeout=config.BROWSER_TIMEOUT)
        await page.wait_for_load_state("networkidle", timeout=config.BROWSER_TIMEOUT)
        await asyncio.sleep(1)

        # Extract directors from table
        tables = await page.locator("table.table").all()
        for table in tables:
            rows = await table.locator("tr").all()
            for row in rows:
                cells = await row.locator("td").all()
                if len(cells) >= 2:
                    texts = []
                    for c in cells:
                        t = (await c.inner_text()).strip()
                        texts.append(t)

                    # Also check for links (DIN links)
                    links = await row.locator("a").all()
                    din = ""
                    if links:
                        href = await links[0].get_attribute("href") or ""
                        din_match = re.search(r'\d{7,8}', href)
                        if din_match:
                            din = din_match.group(0)

                    director = {
                        "din": din or (texts[0] if texts[0].isdigit() else ""),
                        "name": texts[1] if len(texts) > 1 else texts[0],
                        "designation": texts[2] if len(texts) > 2 else "",
                        "appointment_date": texts[3] if len(texts) > 3 else "",
                        "status": "current",
                    }

                    # Skip header rows
                    name_lower = director["name"].lower()
                    if name_lower and name_lower not in ("name", "director name", "din", "designation"):
                        result["directors"].append(director)

        # Check if there's a "Previous Directors" or "Ceased" section
        page_text = await page.locator("body").inner_text()
        if "ceased" in page_text.lower() or "previous" in page_text.lower():
            # Mark directors after the ceased heading as previous
            found_ceased = False
            for d in result["directors"]:
                if "ceased" in d.get("designation", "").lower() or \
                   "resigned" in d.get("designation", "").lower():
                    d["status"] = "previous"

        log.info(f"  [Zaubacorp] Found {len(result['directors'])} directors")
        return result

    except Exception as e:
        log.error(f"  [Zaubacorp] Error for {company_name}: {e}")
        return None


async def get_directors_from_tofler(page, company_name):
    """
    Fallback: search on tofler.in for director details.
    """
    try:
        log.info(f"  [Tofler] Looking up: {company_name}")
        search_query = company_name.replace(" ", "+")
        search_url = f"https://www.tofler.in/search?q={search_query}"
        await page.goto(search_url, timeout=config.BROWSER_TIMEOUT)
        await page.wait_for_load_state("networkidle", timeout=config.BROWSER_TIMEOUT)
        await asyncio.sleep(1)

        # Click first result
        first_link = page.locator("a[href*='/company/'], a[href*='tofler.in/']").first
        if await first_link.count() > 0:
            await first_link.click()
            await page.wait_for_load_state("networkidle", timeout=config.BROWSER_TIMEOUT)
            await asyncio.sleep(1)
        else:
            return None

        directors = []

        # Extract directors from page
        dir_section = page.locator("table:has(th:text-matches('Director', 'i')), .director-info, [class*='director']")
        if await dir_section.count() > 0:
            rows = await dir_section.first.locator("tr").all()
            for row in rows:
                cells = await row.locator("td").all()
                if cells:
                    name = (await cells[0].inner_text()).strip()
                    if name and len(name) > 2 and name.lower() != "name":
                        directors.append({
                            "name": name,
                            "designation": (await cells[1].inner_text()).strip() if len(cells) > 1 else "",
                            "source": "tofler",
                        })

        return {"directors": directors, "source": "tofler"} if directors else None

    except Exception as e:
        log.error(f"  [Tofler] Error: {e}")
        return None
