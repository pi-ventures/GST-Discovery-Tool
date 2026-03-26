"""
GST Discovery Tool - Configuration
KnowledgeHub.ai
"""
import os

# ── Paths ──────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# GST source file
GST_FILE = r"C:\Users\CHIST\Desktop\TG GST New.xlsx"
GST_SHEET_PRIMARY = "GST"        # 416K rows
GST_SHEET_SECONDARY = "gst 2"   # 150K rows

# FICCI & GST file (has TS GST sheet with Mobile/Address/Name columns)
FICCI_GST_FILE = r"E:\Backup Data\Abhishek Backup 2024\- 2024 D Drive\HD DataAI\- MCA [1947 - 2021]\FICCI & GST.xlsx"
FICCI_GST_SHEET = "TS GST"

# MCA master data directory (monthly eir files 2016-2021)
MCA_BASE_DIR = r"E:\Backup Data\Abhishek Backup 2024\- 2024 D Drive\HD DataAI\- MCA [1947 - 2021]"
MCA_YEAR_FOLDERS = ["16", "17", "18", "19", "20", "21"]
MCA_STATES_DIR = os.path.join(MCA_BASE_DIR, "States")

# MCA Metros file (pre-2015 companies)
MCA_METROS_FILE = os.path.join(MCA_BASE_DIR, "MCA Metros 2015.xlsx")

# Working database
WORK_DB = os.path.join(BASE_DIR, "gst_discovery.db")

# Output
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# ── Discovery URLs ─────────────────────────────────────────────────────
TGCT_GST_URL = "https://www.tgct.gov.in/tgportal/GST/GST_Dealer_Search.aspx"
JAMKU_GST_URL = "https://gst.jamku.app/"
KNOWYOURGST_URL = "https://www.knowyourgst.com/gst-number-search/by-name-pan/"
ZAUBACORP_URL = "https://www.zaubacorp.com"

# ── Company Type Keywords ──────────────────────────────────────────────
PVT_LTD_KEYWORDS = [
    "PRIVATE LIMITED", "PVT LTD", "PVT. LTD", "PVT.LTD",
    "(OPC)", "ONE PERSON COMPANY",
]
LLP_KEYWORDS = [
    "LLP", "LIMITED LIABILITY PARTNERSHIP",
]
PUBLIC_LTD_KEYWORDS = [
    "PUBLIC LIMITED", "LIMITED",  # but NOT "PRIVATE LIMITED"
]

# ── Scraping Settings ──────────────────────────────────────────────────
REQUEST_DELAY = 1.0          # seconds between requests
MAX_RETRIES = 3
BROWSER_TIMEOUT = 30000      # ms
HEADLESS = True              # run browser headless
