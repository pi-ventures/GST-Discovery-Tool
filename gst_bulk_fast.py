#!/usr/bin/env python3
"""
GST Bulk Fast Lookup - High-speed GSTIN enrichment via HTTP
KnowledgeHub.ai

Uses plain HTTP requests (no browser) + async concurrency for speed.
~0.5-1s per request, 20 concurrent = ~500-1000 GSTINs/minute.

Usage:
    python gst_bulk_fast.py                      # Run all pending from DB
    python gst_bulk_fast.py --limit 1000         # First 1000
    python gst_bulk_fast.py --workers 30         # 30 concurrent workers
    python gst_bulk_fast.py --resume             # Resume from last checkpoint
"""
import sys
import io
import os
import json
import csv
import re
import time
import asyncio
import argparse
import sqlite3
import logging

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

try:
    import aiohttp
except ImportError:
    print("Installing aiohttp...")
    os.system(f"{sys.executable} -m pip install aiohttp")
    import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "gst_bulk.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger("gst_bulk")

JAMKU_PAGE_URL = "https://gst.jamku.app/gstin/"
JAMKU_API_URL = "https://gst.jamku.app/api/gstin/"
STATE_NAMES = {
    "01": "Jammu & Kashmir", "02": "Himachal Pradesh", "03": "Punjab",
    "04": "Chandigarh", "05": "Uttarakhand", "06": "Haryana",
    "07": "Delhi", "08": "Rajasthan", "09": "Uttar Pradesh",
    "10": "Bihar", "11": "Sikkim", "12": "Arunachal Pradesh",
    "13": "Nagaland", "14": "Manipur", "15": "Mizoram",
    "16": "Tripura", "17": "Meghalaya", "18": "Assam",
    "19": "West Bengal", "20": "Jharkhand", "21": "Odisha",
    "22": "Chhattisgarh", "23": "Madhya Pradesh", "24": "Gujarat",
    "26": "Dadra & Nagar Haveli", "27": "Maharashtra", "29": "Karnataka",
    "30": "Goa", "31": "Lakshadweep", "32": "Kerala",
    "33": "Tamil Nadu", "34": "Puducherry", "35": "Andaman & Nicobar",
    "36": "Telangana", "37": "Andhra Pradesh",
}


def parse_nuxt_data(html):
    """Extract GST data from Jamku's __NUXT_DATA__ in raw HTML."""
    match = re.search(r'id="__NUXT_DATA__">\s*(\[.*?\])\s*</script>', html, re.DOTALL)
    if not match:
        return None

    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None

    target_keys = {"tradeName", "lgnm", "pn", "hsn", "sts", "gstin"}
    for item in data:
        if isinstance(item, dict) and len(set(item.keys()) & target_keys) >= 3:
            def resolve(idx):
                if isinstance(idx, int) and 0 <= idx < len(data):
                    val = data[idx]
                    if isinstance(val, list):
                        return [resolve(i) for i in val]
                    return val
                return idx

            hsn_raw = resolve(item.get("hsn", []))
            hsn_codes = hsn_raw if isinstance(hsn_raw, list) else []

            return {
                "trade_name": resolve(item.get("tradeName", "")) or "",
                "legal_name": resolve(item.get("lgnm", "")) or "",
                "phone": str(resolve(item.get("pn", "")) or ""),
                "email": str(resolve(item.get("em", "")) or ""),
                "status": resolve(item.get("sts", "")) or "",
                "address": resolve(item.get("adr", "")) or "",
                "pincode": str(resolve(item.get("pincode", "")) or ""),
                "hsn_codes": ", ".join(str(h) for h in hsn_codes if h),
                "dealer_type": resolve(item.get("dty", "")) or "",
            }
    return None


async def fetch_gstin(session, gstin, semaphore):
    """Fetch a single GSTIN — try JSON API first (fast), fall back to HTML."""
    async with semaphore:
        # Method 1: JSON API (much faster, no HTML parsing)
        try:
            async with session.get(
                JAMKU_API_URL + gstin,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    gst_data = data.get("data", {})
                    # Check if it has real data (not just a stub)
                    if gst_data.get("lgnm") or gst_data.get("tradeName") or gst_data.get("pn"):
                        hsn_raw = gst_data.get("hsn", [])
                        hsn_codes = hsn_raw if isinstance(hsn_raw, list) else []
                        result = {
                            "gstin": gstin,
                            "state": STATE_NAMES.get(gstin[:2], gstin[:2]),
                            "trade_name": gst_data.get("tradeName", "") or "",
                            "legal_name": gst_data.get("lgnm", "") or "",
                            "phone": str(gst_data.get("pn", "") or ""),
                            "email": str(gst_data.get("em", "") or ""),
                            "status": gst_data.get("sts", "") or "",
                            "address": gst_data.get("adr", "") or "",
                            "pincode": str(gst_data.get("pincode", "") or ""),
                            "hsn_codes": ", ".join(str(h) for h in hsn_codes if h),
                            "dealer_type": gst_data.get("dty", "") or "",
                        }
                        return gstin, result
        except Exception:
            pass

        # Method 2: HTML page (fallback, slower but gets NUXT data)
        try:
            async with session.get(
                JAMKU_PAGE_URL + gstin,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    return gstin, None
                html = await resp.text()
                result = parse_nuxt_data(html)
                if result:
                    result["gstin"] = gstin
                    result["state"] = STATE_NAMES.get(gstin[:2], gstin[:2])
                return gstin, result
        except Exception:
            return gstin, None


async def process_batch(gstins, workers=20):
    """Process a batch of GSTINs concurrently."""
    semaphore = asyncio.Semaphore(workers)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    connector = aiohttp.TCPConnector(limit=workers, limit_per_host=workers)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        tasks = [fetch_gstin(session, g, semaphore) for g in gstins]
        results = await asyncio.gather(*tasks)

    return results


def main():
    parser = argparse.ArgumentParser(description="GST Bulk Fast Lookup")
    parser.add_argument("--limit", type=int, default=None, help="Limit GSTINs to process")
    parser.add_argument("--workers", type=int, default=20, help="Concurrent workers (default: 20)")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size for DB commits (default: 500)")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    parser.add_argument("--output", type=str, default=None, help="Output CSV file")
    args = parser.parse_args()

    db_path = config.WORK_DB
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Ensure results table exists with all columns
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS bulk_results (
            gstin TEXT PRIMARY KEY,
            state TEXT,
            trade_name TEXT,
            legal_name TEXT,
            phone TEXT,
            email TEXT,
            status TEXT,
            address TEXT,
            pincode TEXT,
            hsn_codes TEXT,
            dealer_type TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()

    # Get pending GSTINs
    if args.resume:
        sql = """
            SELECT g.gstin FROM gst_numbers g
            LEFT JOIN bulk_results b ON g.gstin = b.gstin
            WHERE b.gstin IS NULL
        """
    else:
        sql = "SELECT gstin FROM gst_numbers WHERE gst_updated = 0"

    if args.limit:
        sql += f" LIMIT {args.limit}"

    rows = conn.execute(sql).fetchall()
    total = len(rows)
    gstins = [r["gstin"] for r in rows]

    if not gstins:
        log.info("No pending GSTINs to process.")
        return

    log.info(f"Processing {total} GSTINs with {args.workers} concurrent workers")
    log.info(f"Estimated time: ~{total / (args.workers * 60):.0f} minutes")

    # Process in batches
    batch_size = args.batch_size
    processed = 0
    found = 0
    start_time = time.time()

    # CSV output
    csv_path = args.output or os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "output", "gst_bulk_results.csv"
    )
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    csv_fields = [
        "gstin", "state", "phone", "trade_name", "legal_name",
        "hsn_codes", "dealer_type", "email", "status", "address", "pincode",
    ]
    csv_file = open(csv_path, "w", newline="", encoding="utf-8")
    csv_writer = csv.DictWriter(csv_file, fieldnames=csv_fields, extrasaction="ignore")
    csv_writer.writeheader()

    for i in range(0, total, batch_size):
        batch = gstins[i : i + batch_size]

        results = asyncio.run(process_batch(batch, workers=args.workers))

        # Save to DB and CSV
        db_batch = []
        for gstin, data in results:
            processed += 1
            if data:
                found += 1
                db_batch.append((
                    data["gstin"], data["state"], data.get("trade_name", ""),
                    data.get("legal_name", ""), data.get("phone", ""),
                    data.get("email", ""), data.get("status", ""),
                    data.get("address", ""), data.get("pincode", ""),
                    data.get("hsn_codes", ""), data.get("dealer_type", ""),
                ))
                csv_writer.writerow(data)

        if db_batch:
            conn.executemany("""
                INSERT OR REPLACE INTO bulk_results
                (gstin, state, trade_name, legal_name, phone, email,
                 status, address, pincode, hsn_codes, dealer_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, db_batch)
            conn.commit()

        # Mark as updated
        conn.executemany(
            "UPDATE gst_numbers SET gst_updated = 1 WHERE gstin = ?",
            [(g,) for g, _ in results if _ is not None]
        )
        conn.commit()

        # Progress
        elapsed = time.time() - start_time
        rate = processed / elapsed if elapsed > 0 else 0
        eta = (total - processed) / rate if rate > 0 else 0
        log.info(
            f"  [{processed}/{total}] "
            f"Found: {found} | "
            f"Rate: {rate:.0f}/s | "
            f"ETA: {eta/60:.0f}m"
        )

    csv_file.close()
    elapsed = time.time() - start_time

    log.info(f"\n{'='*60}")
    log.info(f"DONE! Processed: {processed}, Found: {found}")
    log.info(f"Time: {elapsed/60:.1f} minutes ({processed/elapsed:.0f} GSTINs/sec)")
    log.info(f"Results: {csv_path}")
    log.info(f"Database: {db_path}")


if __name__ == "__main__":
    main()
