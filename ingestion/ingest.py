import os
import requests
import zipfile
import io
import logging
from datetime import datetime

# ─────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../logs/ingestion.log",encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
DATASET_URL = "https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip"
RAW_DATA_DIR = "../data/raw/"
FILENAME = "household_power_consumption.txt"

# ─────────────────────────────────────────
# STEP 1 — Create folders if not exist
# ─────────────────────────────────────────
def setup_dirs():
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs("../logs", exist_ok=True)
    log.info("Directories verified.")

# ─────────────────────────────────────────
# STEP 2 — Download the zip file
# ─────────────────────────────────────────
def download_dataset():
    target_path = os.path.join(RAW_DATA_DIR, FILENAME)

    # Skip download if file already exists
    if os.path.exists(target_path):
        log.info(f"Dataset already exists at {target_path}. Skipping download.")
        return target_path

    log.info("Starting download from UCI repository...")
    response = requests.get(DATASET_URL, stream=True, timeout=60)

    if response.status_code != 200:
        log.error(f"Download failed. HTTP Status: {response.status_code}")
        raise Exception("Download failed.")

    # ─────────────────────────────────────────
    # STEP 3 — Unzip directly from memory
    # ─────────────────────────────────────────
    log.info("Download complete. Extracting zip...")
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        z.extractall(RAW_DATA_DIR)
    log.info(f"Extracted to {RAW_DATA_DIR}")

    return target_path

# ─────────────────────────────────────────
# STEP 4 — Validate the file
# ─────────────────────────────────────────
def validate_file(path):
    if not os.path.exists(path):
        log.error("File not found after extraction!")
        raise FileNotFoundError(f"{path} does not exist.")

    size_mb = os.path.getsize(path) / (1024 * 1024)
    log.info(f"File size: {size_mb:.2f} MB")

    # Check row count
    with open(path, "r") as f:
        row_count = sum(1 for _ in f)
    log.info(f"Total rows (including header): {row_count}")

    if row_count < 2000000:
        log.warning(f"Row count seems low: {row_count}. Expected ~2 million.")
    else:
        log.info("✅ Validation passed. Dataset looks complete.")

    return row_count

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    log.info("=" * 50)
    log.info("STAGE 1 — INGESTION STARTED")
    log.info(f"Timestamp: {datetime.now()}")
    log.info("=" * 50)

    setup_dirs()
    path = download_dataset()
    validate_file(path)

    log.info("STAGE 1 — INGESTION COMPLETE ✅")

if __name__ == "__main__":
    main()