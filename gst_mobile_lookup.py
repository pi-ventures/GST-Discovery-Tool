#!/usr/bin/env python3
"""
GST Mobile Number Lookup
KnowledgeHub.ai

Simple tool: Input GST number → Get mobile number via Jamku

Usage:
    python gst_mobile_lookup.py 36AAACR5055K1Z8
    python gst_mobile_lookup.py --file gst_list.txt
    python gst_mobile_lookup.py --file gst_list.txt --output results.csv
    python gst_mobile_lookup.py --pan AAACR5055K
"""
import sys
import io
import os
import csv
import json
import asyncio
import argparse
import re

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from playwright.async_api import async_playwright

JAMKU_URL = "https://gst.jamku.app/gstin/"

# All Indian state codes for PAN-based discovery
STATE_CODES = [
    "01","02","03","04","05","06","07","08","09","10",
    "11","12","13","14","15","16","17","18","19","20",
    "21","22","23","24","25","26","27","29","30","31",
    "32","33","34","35","36","37",
]
STATE_NAMES = {
    "01":"Jammu & Kashmir","02":"Himachal Pradesh","03":"Punjab",
    "04":"Chandigarh","05":"Uttarakhand","06":"Haryana",
    "07":"Delhi","08":"Rajasthan","09":"Uttar Pradesh",
    "10":"Bihar","11":"Sikkim","12":"Arunachal Pradesh",
    "13":"Nagaland","14":"Manipur","15":"Mizoram",
    "16":"Tripura","17":"Meghalaya","18":"Assam",
    "19":"West Bengal","20":"Jharkhand","21":"Odisha",
    "22":"Chhattisgarh","23":"Madhya Pradesh","24":"Gujarat",
    "26":"Dadra & Nagar Haveli","27":"Maharashtra","29":"Karnataka",
    "30":"Goa","31":"Lakshadweep","32":"Kerala",
    "33":"Tamil Nadu","34":"Puducherry","35":"Andaman & Nicobar",
    "36":"Telangana","37":"Andhra Pradesh",
}


async def lookup_gstin(page, gstin):
    """Fetch mobile number for a single GSTIN from Jamku."""
    try:
        url = JAMKU_URL + gstin
        await page.goto(url, timeout=30000)
        await page.wait_for_load_state("networkidle", timeout=30000)
        await asyncio.sleep(2)

        script = page.locator("script#__NUXT_DATA__")
        if await script.count() == 0:
            return None

        raw = await script.inner_text()
        data = json.loads(raw)

        # Find the schema dict with GST fields
        target_keys = {"tradeName", "lgnm", "pn", "sts", "gstin"}
        for item in data:
            if isinstance(item, dict):
                if len(set(item.keys()) & target_keys) >= 3:
                    def resolve(idx):
                        if isinstance(idx, int) and 0 <= idx < len(data):
                            val = data[idx]
                            if isinstance(val, list):
                                return [resolve(i) for i in val]
                            return val
                        return idx

                    return {
                        "gstin": gstin,
                        "trade_name": resolve(item.get("tradeName", "")),
                        "legal_name": resolve(item.get("lgnm", "")),
                        "phone": resolve(item.get("pn", "")),
                        "email": resolve(item.get("em", "")),
                        "status": resolve(item.get("sts", "")),
                        "address": resolve(item.get("adr", "")),
                        "pincode": resolve(item.get("pincode", "")),
                        "state": STATE_NAMES.get(gstin[:2], gstin[:2]),
                    }
        return None
    except Exception:
        return None


async def discover_by_pan(page, pan):
    """Try all state codes for a PAN to find all GSTINs nationwide."""
    print(f"\nDiscovering all GSTINs for PAN: {pan}")
    print(f"{'GSTIN':<20} {'State':<20} {'Phone':<14} {'Status':<12} {'Trade Name'}")
    print("-" * 110)

    results = []
    for sc in STATE_CODES:
        # Try common suffixes: 1Z0-1Z9, 2Z0-2Z9, etc.
        for digit in "123456789":
            for last in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ":
                gstin = f"{sc}{pan}{digit}Z{last}"
                data = await lookup_gstin(page, gstin)
                if data and data.get("legal_name"):
                    phone = str(data.get("phone") or "-")
                    status = str(data.get("status") or "-")
                    trade = str(data.get("trade_name") or "-")
                    print(f"{gstin:<20} {data['state']:<20} {phone:<14} {status:<12} {trade}")
                    results.append(data)
                    # Try next digit suffix for same state
                else:
                    break  # no more suffixes for this digit in this state
    return results


async def run(gstins, output_file=None, pan=None):
    """Main runner."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        results = []

        if pan:
            results = await discover_by_pan(page, pan)
        else:
            print(f"{'GSTIN':<20} {'State':<20} {'Phone':<14} {'Status':<12} {'Trade Name':<45} {'Legal Name'}")
            print("-" * 150)

            for gstin in gstins:
                data = await lookup_gstin(page, gstin)
                if data:
                    phone = str(data.get("phone") or "-")
                    status = str(data.get("status") or "-")
                    trade = str(data.get("trade_name") or "-")[:45]
                    legal = str(data.get("legal_name") or "-")
                    print(f"{gstin:<20} {data['state']:<20} {phone:<14} {status:<12} {trade:<45} {legal}")
                    results.append(data)
                else:
                    print(f"{gstin:<20} {'?':<20} {'-':<14} {'NOT FOUND':<12}")

        await browser.close()

        # Save to CSV
        if output_file and results:
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                fields = ["gstin", "state", "phone", "trade_name", "legal_name",
                          "email", "status", "address", "pincode"]
                writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(results)
            print(f"\nSaved {len(results)} results to {output_file}")

        print(f"\nTotal: {len(results)} GSTINs found with data")
        return results


def main():
    parser = argparse.ArgumentParser(
        description="GST Mobile Number Lookup - KnowledgeHub.ai"
    )
    parser.add_argument("gstin", nargs="*", help="One or more GSTIN numbers")
    parser.add_argument("--file", "-f", help="File with one GSTIN per line")
    parser.add_argument("--output", "-o", help="Output CSV file path")
    parser.add_argument("--pan", "-p", help="PAN number - discover all GSTINs nationwide")
    args = parser.parse_args()

    gstins = []

    if args.pan:
        pan = args.pan.upper().strip()
        if len(pan) != 10:
            print("PAN must be 10 characters")
            sys.exit(1)
        output = args.output or f"gst_pan_{pan}.csv"
        asyncio.run(run([], output_file=output, pan=pan))
        return

    if args.gstin:
        gstins = [g.strip().upper() for g in args.gstin if len(g.strip()) == 15]

    if args.file:
        with open(args.file, "r") as f:
            for line in f:
                g = line.strip().upper()
                if len(g) == 15 and re.match(r"^\d{2}[A-Z0-9]+$", g):
                    gstins.append(g)

    if not gstins:
        parser.print_help()
        print("\nExamples:")
        print("  python gst_mobile_lookup.py 36AAACR5055K1Z8")
        print("  python gst_mobile_lookup.py 36AAACR5055K1Z8 27AAACR5055K1Z7 06AAACR5055K1ZB")
        print("  python gst_mobile_lookup.py --file gst_list.txt --output results.csv")
        print("  python gst_mobile_lookup.py --pan AAACR5055K --output reliance_all.csv")
        sys.exit(1)

    output = args.output or "gst_mobile_results.csv"
    asyncio.run(run(gstins, output_file=output))


if __name__ == "__main__":
    main()
