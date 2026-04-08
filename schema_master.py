#!/usr/bin/env python3
"""
KnowledgeHub.ai — Master Company Schema
Relational design: Company → GST → HSN → Locations → Directors → Contacts

Run: python schema_master.py
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gst_discovery.db")


def create_master_schema(conn):
    """Create the KnowledgeHub master company tables."""
    conn.executescript("""

    -- ══════════════════════════════════════════════════════════
    -- CORE: Company Master (like LinkedIn company page)
    -- ══════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS kh_company (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cin TEXT UNIQUE,                    -- MCA Corporate Identity Number
        pan TEXT,                           -- PAN (derived from CIN or GSTIN)
        company_name TEXT NOT NULL,
        trade_name TEXT,
        company_type TEXT,                  -- PVT_LTD, LLP, PUBLIC, OPC, PARTNERSHIP, PROPRIETORSHIP
        class TEXT,                         -- Private, Public, OPC
        category TEXT,                      -- Company limited by shares, etc.
        sub_category TEXT,                  -- Indian Non-Government, etc.
        date_of_incorporation TEXT,
        state TEXT,                         -- Registered state
        roc TEXT,                           -- Registrar of Companies
        status TEXT DEFAULT 'ACTIVE',       -- ACTIVE, INACTIVE, STRUCK_OFF, DISSOLVED
        authorized_capital REAL,
        paid_capital REAL,
        asset_cr REAL,                      -- Assets in Crores
        nic_code TEXT,                      -- National Industrial Classification
        activity_description TEXT,
        sector TEXT,                        -- Agriculture, IT, Manufacturing, etc.
        website TEXT,
        source TEXT,                        -- MCA, FICCI, GSTSERVER, etc.
        tags TEXT,                          -- JSON: ["Gst_Updated", "Director_Verified"]
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- ══════════════════════════════════════════════════════════
    -- GST Registrations (1 company → many GST numbers across states)
    -- ══════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS kh_gst (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER,
        gstin TEXT UNIQUE NOT NULL,
        state_code TEXT,                    -- 36 = Telangana
        state_name TEXT,
        trade_name TEXT,
        legal_name TEXT,
        gst_status TEXT,                    -- Active, Inactive, Cancelled, Provisional
        dealer_type TEXT,                   -- Regular, Composition, etc.
        registration_date TEXT,
        phone TEXT,
        email TEXT,
        pincode TEXT,
        address TEXT,
        source TEXT,                        -- TGST, GSTSERVER, JAMKU
        gst_updated INTEGER DEFAULT 0,
        fetched_at TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES kh_company(id)
    );

    -- ══════════════════════════════════════════════════════════
    -- HSN Codes per GST (1 GST → many HSN codes = business verticals)
    -- ══════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS kh_hsn (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gst_id INTEGER,
        company_id INTEGER,
        hsn_code TEXT NOT NULL,
        description TEXT,                   -- HSN description (from master)
        chapter TEXT,                       -- HSN chapter (first 2 digits)
        vertical TEXT,                      -- Mapped business vertical
        FOREIGN KEY (gst_id) REFERENCES kh_gst(id),
        FOREIGN KEY (company_id) REFERENCES kh_company(id)
    );

    -- ══════════════════════════════════════════════════════════
    -- Locations (1 company → many locations: HQ, branches, factories)
    -- ══════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS kh_location (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER,
        gst_id INTEGER,                     -- linked GST registration for this location
        location_type TEXT,                 -- REGISTERED_OFFICE, BRANCH, FACTORY, WAREHOUSE, RETAIL
        address TEXT,
        city TEXT,
        district TEXT,
        state TEXT,
        pincode TEXT,
        latitude REAL,
        longitude REAL,
        phone TEXT,
        email TEXT,
        source TEXT,
        FOREIGN KEY (company_id) REFERENCES kh_company(id),
        FOREIGN KEY (gst_id) REFERENCES kh_gst(id)
    );

    -- ══════════════════════════════════════════════════════════
    -- Directors / CXOs (1 company → many directors, current + previous)
    -- ══════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS kh_director (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER,
        din TEXT,                            -- Director Identification Number
        name TEXT NOT NULL,
        designation TEXT,                   -- Managing Director, Director, CS, CA, CFO
        cxo_category TEXT,                  -- Board, CXO - CS/CA, Promoter, Nominee
        appointment_date TEXT,
        cessation_date TEXT,
        status TEXT DEFAULT 'CURRENT',      -- CURRENT, PREVIOUS, RESIGNED
        total_directorships INTEGER,
        pan TEXT,
        aadhar TEXT,
        phone TEXT,
        email TEXT,
        upi_verified INTEGER DEFAULT 0,
        upi_name TEXT,
        source TEXT,                        -- ZAUBACORP, TOFLER, FICCI, UPI_MANUAL
        FOREIGN KEY (company_id) REFERENCES kh_company(id)
    );

    -- ══════════════════════════════════════════════════════════
    -- Financial Details (borrowings, charges — from FICCI/MCA)
    -- ══════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS kh_financial (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER,
        financial_type TEXT,                -- BORROWING, CHARGE, CAPITAL
        charge_holder TEXT,                 -- Bank/NBFC name
        amount_lakhs REAL,
        status TEXT,                        -- OPEN, CLOSED
        start_date TEXT,
        closure_date TEXT,
        assets_secured TEXT,
        source TEXT,
        FOREIGN KEY (company_id) REFERENCES kh_company(id)
    );

    -- ══════════════════════════════════════════════════════════
    -- Contacts (unified contact book across all sources)
    -- ══════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS kh_contact (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER,
        director_id INTEGER,
        contact_type TEXT,                  -- PHONE, EMAIL, WEBSITE, UPI
        value TEXT NOT NULL,
        label TEXT,                         -- OFFICE, PERSONAL, GST_REGISTERED, UPI_PAYTM
        verified INTEGER DEFAULT 0,
        source TEXT,
        FOREIGN KEY (company_id) REFERENCES kh_company(id),
        FOREIGN KEY (director_id) REFERENCES kh_director(id)
    );

    -- ══════════════════════════════════════════════════════════
    -- INDEXES
    -- ══════════════════════════════════════════════════════════
    CREATE INDEX IF NOT EXISTS idx_company_pan ON kh_company(pan);
    CREATE INDEX IF NOT EXISTS idx_company_name ON kh_company(company_name);
    CREATE INDEX IF NOT EXISTS idx_company_type ON kh_company(company_type);
    CREATE INDEX IF NOT EXISTS idx_company_state ON kh_company(status);

    CREATE INDEX IF NOT EXISTS idx_gst_company ON kh_gst(company_id);
    CREATE INDEX IF NOT EXISTS idx_gst_state ON kh_gst(state_code);
    CREATE INDEX IF NOT EXISTS idx_gst_pan ON kh_gst(gstin);

    CREATE INDEX IF NOT EXISTS idx_hsn_company ON kh_hsn(company_id);
    CREATE INDEX IF NOT EXISTS idx_hsn_code ON kh_hsn(hsn_code);
    CREATE INDEX IF NOT EXISTS idx_hsn_vertical ON kh_hsn(vertical);

    CREATE INDEX IF NOT EXISTS idx_loc_company ON kh_location(company_id);
    CREATE INDEX IF NOT EXISTS idx_loc_state ON kh_location(state);
    CREATE INDEX IF NOT EXISTS idx_loc_pincode ON kh_location(pincode);

    CREATE INDEX IF NOT EXISTS idx_dir_company ON kh_director(company_id);
    CREATE INDEX IF NOT EXISTS idx_dir_din ON kh_director(din);
    CREATE INDEX IF NOT EXISTS idx_dir_name ON kh_director(name);

    CREATE INDEX IF NOT EXISTS idx_contact_company ON kh_contact(company_id);
    CREATE INDEX IF NOT EXISTS idx_contact_value ON kh_contact(value);
    """)
    conn.commit()
    print("KnowledgeHub master schema created.")


def merge_existing_data(conn):
    """Merge all existing data into master schema. Upsert only — never delete."""
    cur = conn.cursor()

    # ── 1. Upsert MCA companies (tagged as MCA) ──
    print("\n1. Merging MCA companies...")
    cur.execute("""
        INSERT INTO kh_company
        (cin, company_name, date_of_incorporation, state, roc, category, sub_category,
         class, authorized_capital, paid_capital, activity_description,
         registered_address, source, tags)
        SELECT cin, company_name, date_of_incorporation, state, roc, category, sub_category,
               class, CAST(authorized_capital AS REAL), CAST(paid_capital AS REAL),
               activity_description, registered_address, 'MCA',
               '["MCA"]'
        FROM mca_companies
        WHERE cin IS NOT NULL AND cin != ''
        ON CONFLICT(cin) DO UPDATE SET
            company_name = COALESCE(NULLIF(excluded.company_name, ''), kh_company.company_name),
            state = COALESCE(NULLIF(excluded.state, ''), kh_company.state),
            roc = COALESCE(NULLIF(excluded.roc, ''), kh_company.roc),
            category = COALESCE(NULLIF(excluded.category, ''), kh_company.category),
            sub_category = COALESCE(NULLIF(excluded.sub_category, ''), kh_company.sub_category),
            class = COALESCE(NULLIF(excluded.class, ''), kh_company.class),
            authorized_capital = COALESCE(excluded.authorized_capital, kh_company.authorized_capital),
            paid_capital = COALESCE(excluded.paid_capital, kh_company.paid_capital),
            activity_description = COALESCE(NULLIF(excluded.activity_description, ''), kh_company.activity_description),
            registered_address = COALESCE(NULLIF(excluded.registered_address, ''), kh_company.registered_address),
            updated_at = CURRENT_TIMESTAMP
    """)
    mca_count = cur.rowcount
    conn.commit()
    print(f"   MCA companies merged: {mca_count:,}")

    # ── 2. Upsert GST numbers ──
    print("\n2. Merging GST numbers...")
    # Batch insert to handle large volume
    rows = cur.execute("""
        SELECT gstin, trade_name,
               CASE WHEN type = 'GSTSERVER' THEN 'GSTSERVER' ELSE 'TGST' END as src
        FROM gst_numbers
    """).fetchall()
    gst_count = 0
    batch = []
    for r in rows:
        batch.append((r[0], r[0][:2], r[1] or '', r[2]))
        if len(batch) >= 10000:
            cur.executemany("""
                INSERT OR IGNORE INTO kh_gst (gstin, state_code, trade_name, source)
                VALUES (?, ?, ?, ?)
            """, batch)
            gst_count += len(batch)
            batch = []
    if batch:
        cur.executemany("""
            INSERT OR IGNORE INTO kh_gst (gstin, state_code, trade_name, source)
            VALUES (?, ?, ?, ?)
        """, batch)
        gst_count += len(batch)
    conn.commit()
    print(f"   GST numbers merged: {gst_count:,}")

    # ── 3. Merge Jamku enrichment ──
    print("\n3. Merging Jamku enrichment...")
    rows = cur.execute("SELECT * FROM bulk_results").fetchall()
    cols = [d[0] for d in cur.description]
    enriched = 0
    for row in rows:
        r = dict(zip(cols, row))
        gstin = r["gstin"]
        cur.execute("""
            UPDATE kh_gst SET
                trade_name = COALESCE(NULLIF(?, ''), trade_name),
                legal_name = COALESCE(NULLIF(?, ''), legal_name),
                phone = COALESCE(NULLIF(?, ''), phone),
                email = COALESCE(NULLIF(?, ''), email),
                gst_status = COALESCE(NULLIF(?, ''), gst_status),
                address = COALESCE(NULLIF(?, ''), address),
                pincode = COALESCE(NULLIF(?, ''), pincode),
                dealer_type = COALESCE(NULLIF(?, ''), dealer_type),
                state_name = COALESCE(NULLIF(?, ''), state_name),
                gst_updated = 1,
                fetched_at = CURRENT_TIMESTAMP,
                source = COALESCE(source, '') || ',JAMKU'
            WHERE gstin = ?
        """, (
            r.get("trade_name", ""), r.get("legal_name", ""),
            r.get("phone", ""), r.get("email", ""),
            r.get("status", ""), r.get("address", ""),
            r.get("pincode", ""), r.get("dealer_type", ""),
            r.get("state", ""), gstin
        ))

        # Upsert HSN codes
        hsn_str = r.get("hsn_codes", "")
        if hsn_str:
            for code in hsn_str.split(", "):
                code = code.strip()
                if code:
                    gst_id = cur.execute("SELECT id FROM kh_gst WHERE gstin = ?", (gstin,)).fetchone()
                    if gst_id:
                        cur.execute("""
                            INSERT OR IGNORE INTO kh_hsn (gst_id, hsn_code, chapter)
                            VALUES (?, ?, ?)
                        """, (gst_id[0], code, code[:2]))
        enriched += 1

    conn.commit()
    print(f"   Jamku enrichments merged: {enriched:,}")

    # ── 4. Link GST → Companies by name match ──
    print("\n4. Linking GST to Companies...")
    cur.execute("""
        UPDATE kh_gst SET company_id = (
            SELECT c.id FROM kh_company c
            WHERE UPPER(c.company_name) = UPPER(kh_gst.legal_name)
               OR UPPER(c.company_name) = UPPER(kh_gst.trade_name)
            LIMIT 1
        )
        WHERE company_id IS NULL
        AND (legal_name IS NOT NULL AND legal_name != ''
             OR trade_name IS NOT NULL AND trade_name != '')
    """)
    linked = cur.rowcount
    conn.commit()
    print(f"   GST-Company links: {linked:,}")

    # ── 5. Set PAN on companies from linked GSTINs ──
    print("\n5. Setting PAN from GSTINs...")
    cur.execute("""
        UPDATE kh_company SET pan = (
            SELECT SUBSTR(g.gstin, 3, 10) FROM kh_gst g
            WHERE g.company_id = kh_company.id LIMIT 1
        )
        WHERE pan IS NULL AND id IN (SELECT company_id FROM kh_gst WHERE company_id IS NOT NULL)
    """)
    conn.commit()
    print(f"   PAN set: {cur.rowcount:,}")

    # ── 6. Tag MCA companies with industry from HSN ──
    print("\n6. Mapping industries from HSN codes...")
    cur.execute("""
        UPDATE kh_company SET sector = (
            SELECT h.chapter FROM kh_hsn h
            JOIN kh_gst g ON h.gst_id = g.id
            WHERE g.company_id = kh_company.id
            GROUP BY h.chapter ORDER BY COUNT(*) DESC LIMIT 1
        )
        WHERE sector IS NULL AND id IN (
            SELECT g.company_id FROM kh_gst g
            JOIN kh_hsn h ON h.gst_id = g.id
            WHERE g.company_id IS NOT NULL
        )
    """)
    conn.commit()
    print(f"   Industries mapped: {cur.rowcount:,}")

    # ── Summary ──
    print("\n" + "=" * 60)
    print("MERGE SUMMARY")
    print("=" * 60)
    for table in ["kh_company", "kh_gst", "kh_hsn", "kh_location", "kh_director", "kh_contact", "kh_financial"]:
        count = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table:<20} {count:>12,}")

    # Extra stats
    with_pan = cur.execute("SELECT COUNT(*) FROM kh_company WHERE pan IS NOT NULL").fetchone()[0]
    linked_gst = cur.execute("SELECT COUNT(*) FROM kh_gst WHERE company_id IS NOT NULL").fetchone()[0]
    with_phone = cur.execute("SELECT COUNT(*) FROM kh_gst WHERE phone IS NOT NULL AND phone != ''").fetchone()[0]
    with_hsn = cur.execute("SELECT COUNT(DISTINCT gst_id) FROM kh_hsn").fetchone()[0]
    print(f"\n  Companies with PAN:  {with_pan:,}")
    print(f"  GSTINs linked:       {linked_gst:,}")
    print(f"  GSTINs with phone:   {with_phone:,}")
    print(f"  GSTINs with HSN:     {with_hsn:,}")


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    print("=" * 60)
    print("KnowledgeHub.ai — Master Company Database")
    print("=" * 60)

    create_master_schema(conn)
    merge_existing_data(conn)

    conn.close()
    print(f"\nDatabase: {DB_PATH}")
    print("Done!")


if __name__ == "__main__":
    main()
