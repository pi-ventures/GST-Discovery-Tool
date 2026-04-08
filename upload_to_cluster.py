"""Upload GST Discovery Tool to Ray cluster (PI2 head node)."""
import paramiko
import os
import sys

PI2_HOST = "10.10.4.150"
PI2_USER = "pi2"
PI2_PASS = "pi2"

SRC = os.path.dirname(os.path.abspath(__file__))
DST = "/home/pi2/KnowledgeHub.ai/GST-Discovery-Tool"

CODE_FILES = [
    "config.py", "db.py", "classifier.py", "main.py", "updater.py",
    "upi_batch.py", "gst_mobile_lookup.py", "gst_bulk_fast.py",
    "schema_master.py", "requirements.txt",
    "discovery/__init__.py", "discovery/tgct.py", "discovery/zaubacorp.py",
    "discovery/jamku.py", "discovery/knowyourgst.py", "discovery/upi.py",
]

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(PI2_HOST, username=PI2_USER, password=PI2_PASS, timeout=10)
    sftp = ssh.open_sftp()

    # Transfer code files
    print("Uploading code files...")
    for f in CODE_FILES:
        local = os.path.join(SRC, f)
        remote = DST + "/" + f
        sftp.put(local, remote)
        print(f"  {f}")

    # Transfer database (1.5GB - will take a few minutes)
    db_file = "gst_discovery.db"
    local_db = os.path.join(SRC, db_file)
    remote_db = DST + "/" + db_file
    size_mb = os.path.getsize(local_db) / (1024 * 1024)
    print(f"\nUploading database ({size_mb:.0f} MB)...")

    # Use callback to show progress
    transferred = [0]
    total = os.path.getsize(local_db)
    def progress(sent, total_size):
        transferred[0] = sent
        pct = (sent / total_size) * 100
        sys.stdout.write(f"\r  {sent/(1024*1024):.0f}/{total_size/(1024*1024):.0f} MB ({pct:.1f}%)")
        sys.stdout.flush()

    sftp.put(local_db, remote_db, callback=progress)
    print("\n  Database uploaded!")

    sftp.close()

    # Update config.py paths for Linux
    print("\nUpdating config paths for Linux...")
    ssh.exec_command(f"""
        cd {DST} && sed -i 's|C:\\\\.*TG GST New.xlsx|/home/pi2/data/TG_GST_New.xlsx|g' config.py
        sed -i 's|E:\\\\.*MCA.*|/home/pi2/data/MCA|g' config.py
        sed -i "s|WORK_DB = .*|WORK_DB = '{DST}/gst_discovery.db'|" config.py
    """)

    # Install dependencies
    print("Installing dependencies on PI2...")
    stdin, stdout, stderr = ssh.exec_command(f"cd {DST} && pip3 install -r requirements.txt 2>&1 | tail -5")
    print(stdout.read().decode())

    ssh.close()
    print(f"\nDone! Files at PI2:{DST}")
    print(f"SSH: ssh pi2@{PI2_HOST}")
    print(f"Run: cd {DST} && python3 gst_bulk_fast.py --resume --workers 50")

if __name__ == "__main__":
    main()
