"""
GST Discovery Tool - Company Classifier
Determines if a company is Private Limited, LLP, or Regular.
"""
import config


def classify_company(trade_name):
    """
    Classify company type from trade name.
    Returns: 'PVT_LTD', 'LLP', or 'REGULAR'
    """
    if not trade_name:
        return "REGULAR"

    name_upper = trade_name.upper().strip()

    # Check LLP first (before PVT LTD, since some LLPs have "Limited" in name)
    for kw in config.LLP_KEYWORDS:
        if kw in name_upper:
            return "LLP"

    # Check Private Limited
    for kw in config.PVT_LTD_KEYWORDS:
        if kw in name_upper:
            return "PVT_LTD"

    # Check Public Limited (has "LIMITED" but not "PRIVATE LIMITED")
    for kw in config.PUBLIC_LTD_KEYWORDS:
        if kw in name_upper and "PRIVATE" not in name_upper:
            return "PVT_LTD"  # treat public ltd same as pvt for director lookup

    return "REGULAR"


def classify_all(conn):
    """Classify all GST numbers in the database."""
    cursor = conn.cursor()
    rows = cursor.execute(
        "SELECT gstin, trade_name FROM gst_numbers WHERE company_type IS NULL"
    ).fetchall()

    for row in rows:
        ctype = classify_company(row["trade_name"])
        cursor.execute(
            "UPDATE gst_numbers SET company_type = ? WHERE gstin = ?",
            (ctype, row["gstin"])
        )

    conn.commit()

    # Summary
    stats = cursor.execute("""
        SELECT company_type, COUNT(*) as cnt
        FROM gst_numbers
        GROUP BY company_type
    """).fetchall()

    return {row["company_type"]: row["cnt"] for row in stats}
