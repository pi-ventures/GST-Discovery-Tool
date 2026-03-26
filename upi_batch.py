#!/usr/bin/env python3
"""
UPI Batch Export/Import Tool
KnowledgeHub.ai - GST Discovery Tool

Step 1: Export phone numbers to CSV for manual UPI verification in Paytm app
Step 2: Import the verified names back from the filled CSV

Usage:
    python upi_batch.py --export              # Export phones to CSV
    python upi_batch.py --export --limit 100  # Export first 100
    python upi_batch.py --import-file upi_names_filled.csv  # Import names back
"""
import argparse
import csv
import json
import os
import sys
import sqlite3
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
from db import get_db

log = logging.getLogger("gst_discovery")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def export_phones(limit=None):
    """Export discovered phone numbers to CSV for manual UPI check."""
    conn = get_db()
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    sql = """
        SELECT gstin, trade_name, legal_name, company_type,
               mobile_number, mobile_number_tgct, upi_name
        FROM discovery_results
        WHERE mobile_number IS NOT NULL AND mobile_number != ''
    """
    if limit:
        sql += f" LIMIT {limit}"

    rows = conn.execute(sql).fetchall()

    if not rows:
        log.info("No phone numbers to export. Run discovery first.")
        return

    output_path = os.path.join(config.OUTPUT_DIR, "upi_verify_batch.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "GSTIN", "Trade Name", "Legal Name", "Company Type",
            "Mobile (Jamku)", "Mobile (TGCT)",
            "UPI_ID (@paytm)", "UPI_Name (FILL THIS)",
        ])
        for r in rows:
            mobile = r["mobile_number"]
            writer.writerow([
                r["gstin"],
                r["trade_name"],
                r["legal_name"],
                r["company_type"],
                mobile,
                r["mobile_number_tgct"] or "",
                f"{mobile}@paytm",
                r["upi_name"] or "",  # pre-fill if already known
            ])

    log.info(f"Exported {len(rows)} phone numbers to: {output_path}")
    log.info("")
    log.info("INSTRUCTIONS:")
    log.info("1. Open the CSV file")
    log.info("2. For each row, search the Mobile number in Paytm app (Send Money)")
    log.info("3. Enter the name shown by Paytm in the 'UPI_Name (FILL THIS)' column")
    log.info("4. Save the CSV")
    log.info(f"5. Run: python upi_batch.py --import-file \"{output_path}\"")


def import_names(csv_path):
    """Import UPI verified names back from the filled CSV."""
    if not os.path.exists(csv_path):
        log.error(f"File not found: {csv_path}")
        return

    conn = get_db()
    updated = 0
    new_cxo = 0

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gstin = row.get("GSTIN", "").strip()
            upi_name = row.get("UPI_Name (FILL THIS)", "").strip()

            if not gstin or not upi_name:
                continue

            # Update discovery_results with UPI name
            conn.execute(
                "UPDATE discovery_results SET upi_name = ? WHERE gstin = ?",
                (upi_name, gstin)
            )

            # Check if this name matches existing directors/owners
            result = conn.execute(
                "SELECT directors, business_owners, company_type FROM discovery_results WHERE gstin = ?",
                (gstin,)
            ).fetchone()

            if result:
                upi_upper = upi_name.upper()

                if result["company_type"] in ("PVT_LTD", "LLP"):
                    # Check against directors
                    directors = json.loads(result["directors"] or "[]")
                    matched = any(
                        d.get("name", "").upper() in upi_upper or upi_upper in d.get("name", "").upper()
                        for d in directors
                    )
                    if not matched:
                        directors.append({
                            "name": upi_name,
                            "designation": "CXO - CS/CA",
                            "status": "current",
                            "upi_verified": True,
                            "source": "upi_manual",
                        })
                        conn.execute(
                            "UPDATE discovery_results SET directors = ? WHERE gstin = ?",
                            (json.dumps(directors), gstin)
                        )
                        new_cxo += 1
                else:
                    # Check against business owners
                    owners = json.loads(result["business_owners"] or "[]")
                    matched = any(
                        upi_upper in str(o).upper() or str(o).upper() in upi_upper
                        for o in owners
                    )
                    if not matched:
                        owners.append(f"{upi_name} [CXO - CS/CA]")
                        conn.execute(
                            "UPDATE discovery_results SET business_owners = ? WHERE gstin = ?",
                            (json.dumps(owners), gstin)
                        )
                        new_cxo += 1

            updated += 1

    conn.commit()
    log.info(f"Imported {updated} UPI names, {new_cxo} new CXO entries added")
    log.info("Run 'python main.py --update-mca' to write changes back to MCA files")


def main():
    parser = argparse.ArgumentParser(description="UPI Batch Export/Import Tool")
    parser.add_argument("--export", action="store_true", help="Export phone numbers to CSV")
    parser.add_argument("--import-file", type=str, help="Import UPI names from filled CSV")
    parser.add_argument("--limit", type=int, help="Limit export rows")
    args = parser.parse_args()

    if args.export:
        export_phones(args.limit)
    elif args.import_file:
        import_names(args.import_file)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
