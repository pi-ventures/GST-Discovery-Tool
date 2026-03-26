#!/usr/bin/env python3
"""
GST Discovery Tool - Main Orchestrator
KnowledgeHub.ai

Usage:
    python main.py                     # Run full discovery pipeline
    python main.py --load-only         # Only load data into SQLite
    python main.py --limit 100         # Process only first 100 GSTINs
    python main.py --export            # Export results to CSV
    python main.py --update-mca        # Only update MCA files with tags
    python main.py --type PVT_LTD      # Only process Private Limited companies
    python main.py --type REGULAR      # Only process regular companies
    python main.py --gstin 36AAACR...  # Process a single GSTIN
"""
import asyncio
import argparse
import json
import logging
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from db import get_db, init_db, load_all, get_pending_gstins, mark_gst_updated, \
    find_mca_company, save_discovery_result
from classifier import classify_company, classify_all
from discovery.tgct import get_mobile_from_tgct
from discovery.zaubacorp import get_directors_from_zaubacorp, get_directors_from_tofler
from discovery.jamku import get_gst_details_from_jamku
from discovery.knowyourgst import discover_related_gstins
from discovery.upi import verify_upi_name
from updater import update_ficci_gst_file, update_mca_master_with_tag, export_discovery_csv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(config.BASE_DIR, "gst_discovery.log"),
            encoding="utf-8"
        ),
    ]
)
log = logging.getLogger("gst_discovery")


async def process_pvt_ltd(page, gstin, trade_name, conn):
    """
    Discovery flow for Private Limited / LLP companies.
    2.1.1 - Get directors from Zaubacorp
    2.1.2 - Get mobile from TGCT
    2.1.3 - Verify via UPI
    2.1.4 - Match/add name to directors
    2.1.5 - Mark Gst_Updated
    """
    result = {
        "gstin": gstin,
        "trade_name": trade_name,
        "company_type": "PVT_LTD",
        "source": "pvt_ltd_flow",
    }

    # 2.1.1 - Find in MCA and get directors from Zaubacorp
    mca = find_mca_company(conn, trade_name)
    cin = mca["cin"] if mca else None
    result["cin"] = cin

    directors_data = await get_directors_from_zaubacorp(page, trade_name, cin)
    if directors_data:
        result["legal_name"] = directors_data.get("proper_name")
        result["directors"] = json.dumps(directors_data.get("directors", []))
        if directors_data.get("cin"):
            result["cin"] = directors_data["cin"]
    else:
        # Fallback to Tofler
        tofler_data = await get_directors_from_tofler(page, trade_name)
        if tofler_data:
            result["directors"] = json.dumps(tofler_data.get("directors", []))

    # Get GST details from Jamku (phone, email, address, legal name)
    jamku_data = await get_gst_details_from_jamku(page, gstin)
    if jamku_data:
        if jamku_data.get("phone"):
            result["mobile_number"] = str(jamku_data["phone"])
        if jamku_data.get("address"):
            result["address"] = jamku_data["address"]
        if not result.get("legal_name") and jamku_data.get("legal_name"):
            result["legal_name"] = jamku_data["legal_name"]

    # 2.1.2 - Also get mobile from TGCT (always run both sources)
    tgct_data = await get_mobile_from_tgct(page, gstin)
    if tgct_data:
        tgct_mobile = tgct_data.get("mobile_number")
        if tgct_mobile:
            # If Jamku didn't have a number, use TGCT's
            if not result.get("mobile_number"):
                result["mobile_number"] = tgct_mobile
            # If TGCT found a different number, store it as secondary
            elif tgct_mobile != result.get("mobile_number"):
                result["mobile_number_tgct"] = tgct_mobile
        if tgct_data.get("address") and not result.get("address"):
            result["address"] = tgct_data["address"]

    # UPI verification skipped — can be done later via upi_batch.py

    return result


async def process_regular(page, gstin, trade_name, conn):
    """
    Discovery flow for regular companies (proprietorship, partnership, etc).
    2.2.1 - Get details from Jamku
    2.2.2 - Get mobile from TGCT
    2.2.3 - Verify via UPI
    2.2.4 - Match/add name
    2.2.5 - Mark Gst_Updated
    2.2.6 - Discover related GSTINs via KnowYourGST
    """
    result = {
        "gstin": gstin,
        "trade_name": trade_name,
        "company_type": "REGULAR",
        "source": "regular_flow",
    }

    # 2.2.1 - Get details from Jamku
    jamku_data = await get_gst_details_from_jamku(page, gstin)
    if jamku_data:
        result["trade_name"] = jamku_data.get("trade_name") or trade_name
        result["legal_name"] = jamku_data.get("legal_name")
        result["hsn_codes"] = json.dumps(jamku_data.get("hsn_codes", []))
        result["business_owners"] = json.dumps(jamku_data.get("business_owners", []))
        result["address"] = jamku_data.get("address")
        if jamku_data.get("phone"):
            result["mobile_number"] = str(jamku_data["phone"])

    # 2.2.2 - Also get mobile from TGCT (always run both sources)
    tgct_data = await get_mobile_from_tgct(page, gstin)
    if tgct_data:
        tgct_mobile = tgct_data.get("mobile_number")
        if tgct_mobile:
            if not result.get("mobile_number"):
                result["mobile_number"] = tgct_mobile
            elif tgct_mobile != result.get("mobile_number"):
                result["mobile_number_tgct"] = tgct_mobile
        if tgct_data.get("address") and not result.get("address"):
            result["address"] = tgct_data["address"]

    # UPI verification skipped — can be done later via upi_batch.py

    # 2.2.6 - Discover related GSTINs via KnowYourGST
    firm_name = result.get("legal_name") or result.get("trade_name") or trade_name
    if firm_name:
        related = await discover_related_gstins(page, firm_name, config.OUTPUT_DIR)
        if related:
            result["related_gstins"] = json.dumps(related)

    return result


async def run_discovery(conn, limit=None, company_type=None, single_gstin=None):
    """Main discovery loop."""
    from playwright.async_api import async_playwright

    if single_gstin:
        rows = conn.execute(
            "SELECT gstin, trade_name, company_type FROM gst_numbers WHERE gstin = ?",
            (single_gstin,)
        ).fetchall()
    else:
        sql = "SELECT gstin, trade_name, company_type FROM gst_numbers WHERE gst_updated = 0"
        params = []
        if company_type:
            sql += " AND company_type = ?"
            params.append(company_type)
        if limit:
            sql += f" LIMIT {limit}"
        rows = conn.execute(sql, params).fetchall()

    if not rows:
        log.info("No pending GSTINs to process.")
        return

    log.info(f"Processing {len(rows)} GSTINs...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=config.HEADLESS)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        processed = 0
        failed = 0

        for row in rows:
            gstin = row["gstin"]
            trade_name = row["trade_name"]
            ctype = row["company_type"] or classify_company(trade_name)

            log.info(f"\n{'='*60}")
            log.info(f"[{processed+1}/{len(rows)}] {gstin} | {trade_name} | {ctype}")
            log.info(f"{'='*60}")

            try:
                if ctype in ("PVT_LTD", "LLP"):
                    result = await process_pvt_ltd(page, gstin, trade_name, conn)
                else:
                    result = await process_regular(page, gstin, trade_name, conn)

                # Save result
                save_discovery_result(conn, result)
                mark_gst_updated(conn, gstin)
                processed += 1

                log.info(f"  -> Mobile: {result.get('mobile_number', '-')}, "
                         f"UPI Name: {result.get('upi_name', '-')}")

            except Exception as e:
                log.error(f"  FAILED: {e}")
                failed += 1

            # Rate limiting
            import time
            time.sleep(config.REQUEST_DELAY)

        await browser.close()

    log.info(f"\nDiscovery complete: {processed} processed, {failed} failed")


def main():
    parser = argparse.ArgumentParser(
        description="GST Discovery Tool - KnowledgeHub.ai",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--load-only", action="store_true",
                        help="Only load data into SQLite, don't run discovery")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit number of GSTINs to process")
    parser.add_argument("--type", choices=["PVT_LTD", "LLP", "REGULAR"],
                        help="Only process this company type")
    parser.add_argument("--gstin", type=str, default=None,
                        help="Process a single GSTIN")
    parser.add_argument("--export", action="store_true",
                        help="Export discovery results to CSV")
    parser.add_argument("--update-mca", action="store_true",
                        help="Update MCA files with Gst_Updated tags")
    parser.add_argument("--headless", action="store_true", default=True,
                        help="Run browser in headless mode (default)")
    parser.add_argument("--no-headless", action="store_true",
                        help="Run browser with visible UI")

    args = parser.parse_args()

    if args.no_headless:
        config.HEADLESS = False

    # Step 1: Load data
    log.info("=" * 60)
    log.info("GST Discovery Tool - KnowledgeHub.ai")
    log.info("=" * 60)

    conn = load_all()

    # Step 2: Classify companies
    log.info("\nClassifying companies...")
    stats = classify_all(conn)
    for ctype, count in sorted(stats.items()):
        log.info(f"  {ctype}: {count}")

    if args.load_only:
        log.info("\nData loaded. Exiting (--load-only).")
        return

    if args.export:
        export_discovery_csv(conn)
        return

    if args.update_mca:
        update_ficci_gst_file(conn)
        update_mca_master_with_tag(conn)
        return

    # Step 3: Run discovery
    asyncio.run(run_discovery(
        conn,
        limit=args.limit,
        company_type=args.type,
        single_gstin=args.gstin,
    ))

    # Step 4: Update files
    log.info("\nUpdating output files...")
    update_ficci_gst_file(conn)
    update_mca_master_with_tag(conn)
    export_discovery_csv(conn)

    log.info("\nAll done!")


if __name__ == "__main__":
    main()
