import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../logs/cleaning.log", encoding="utf-8"),
        logging.StreamHandler()
    ])
log = logging.getLogger(__name__)

RAW_PATH      = "../data/raw/household_power_consumption.txt"
PROCESSED_DIR = "../data/processed/"
OUTPUT_PATH   = PROCESSED_DIR + "clean_energy_data.csv"

def main():
    log.info("STAGE 2 - CLEANING (Jenkins lightweight mode)")
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    log.info("Reading raw data with pandas...")
    df = pd.read_csv(RAW_PATH, sep=";", low_memory=False)
    log.info(f"Raw rows: {len(df)}")

    # Replace ? with NaN
    df.replace("?", pd.NA, inplace=True)

    # Merge Date + Time into Timestamp
    df["Timestamp"] = pd.to_datetime(
        df["Date"] + " " + df["Time"],
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce"
    )
    df.drop(columns=["Date", "Time"], inplace=True)

    # Cast numeric columns
    numeric_cols = [
        "Global_active_power", "Global_reactive_power",
        "Voltage", "Global_intensity",
        "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"
    ]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Drop nulls
    before = len(df)
    df.dropna(subset=["Timestamp", "Global_active_power", "Voltage"], inplace=True)
    log.info(f"Dropped {before - len(df)} null rows. Remaining: {len(df)}")

    # Flag anomalies
    df["anomaly_flag"] = "normal"
    df.loc[df["Voltage"] > 250, "anomaly_flag"]              = "high_voltage"
    df.loc[df["Global_active_power"] > 6, "anomaly_flag"]    = "high_power"
    df.loc[df["Voltage"] < 220, "anomaly_flag"]              = "low_voltage"
    df.loc[df["Global_active_power"] == 0, "anomaly_flag"]   = "zero_power"

    # Log summary
    log.info(f"Anomaly counts:\n{df['anomaly_flag'].value_counts()}")

    # Save in chunks — memory efficient
    log.info(f"Saving to {OUTPUT_PATH}...")
    df.to_csv(OUTPUT_PATH, index=False, chunksize=50000)
    log.info(f"Saved. Size: {os.path.getsize(OUTPUT_PATH)/1024/1024:.2f} MB")
    log.info("STAGE 2 - COMPLETE")

if __name__ == "__main__":
    main()