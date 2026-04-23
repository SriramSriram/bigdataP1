import os
import json
import logging
import pandas as pd
from datetime import datetime
from collections import deque
from kafka import KafkaConsumer

# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../logs/spark_streaming.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
KAFKA_BROKER    = "localhost:9092"
TOPIC_INPUT     = "kafka-energy-raw"
OUTPUT_PATH     = "E:/SRM_Projects/bigdataP1/data/processed/stream_output"
WINDOW_SIZE     = 300   # rolling average over last 300 rows (5 min simulation)
SAVE_EVERY      = 1000  # save results to disk every 1000 rows

# ─────────────────────────────────────────
# SETUP OUTPUT FOLDERS
# ─────────────────────────────────────────
def setup_folders():
    os.makedirs(f"{OUTPUT_PATH}/anomalies", exist_ok=True)
    os.makedirs(f"{OUTPUT_PATH}/rolling_avg", exist_ok=True)
    log.info("Output folders ready.")

# ─────────────────────────────────────────
# CONNECT TO KAFKA
# ─────────────────────────────────────────
def create_consumer():
    log.info(f"Connecting to Kafka topic: {TOPIC_INPUT}")
    consumer = KafkaConsumer(
        TOPIC_INPUT,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="energy-streaming-group"
    )
    log.info("Kafka consumer connected.")
    return consumer

# ─────────────────────────────────────────
# ANOMALY DETECTION
# ─────────────────────────────────────────
def detect_anomaly(row):
    try:
        voltage = float(row.get("Voltage", 0) or 0)
        power   = float(row.get("Global_active_power", 0) or 0)
        flag    = row.get("anomaly_flag", "normal")

        if flag != "normal":
            return {
                "Timestamp"           : row.get("Timestamp"),
                "anomaly_flag"        : flag,
                "Voltage"             : voltage,
                "Global_active_power" : power,
                "detected_at"         : datetime.now().isoformat()
            }
    except Exception:
        pass
    return None

# ─────────────────────────────────────────
# ROLLING AVERAGE
# ─────────────────────────────────────────
def compute_rolling_avg(window_buffer):
    if not window_buffer:
        return None
    try:
        powers    = [float(r.get("Global_active_power") or 0) for r in window_buffer]
        voltages  = [float(r.get("Voltage") or 0)             for r in window_buffer]
        intensity = [float(r.get("Global_intensity") or 0)    for r in window_buffer]
        return {
            "window_end"     : datetime.now().isoformat(),
            "avg_power"      : round(sum(powers)    / len(powers),    3),
            "avg_voltage"    : round(sum(voltages)   / len(voltages),   3),
            "avg_intensity"  : round(sum(intensity)  / len(intensity),  3),
            "window_size"    : len(window_buffer)
        }
    except Exception:
        return None

# ─────────────────────────────────────────
# SAVE RESULTS TO CSV
# ─────────────────────────────────────────
def save_results(anomalies_batch, rolling_batch):
    if anomalies_batch:
        path = f"{OUTPUT_PATH}/anomalies/anomalies.csv"
        df   = pd.DataFrame(anomalies_batch)
        df.to_csv(path, mode="a",
                  header=not os.path.exists(path),
                  index=False)
        log.info(f"Saved {len(anomalies_batch)} anomalies to CSV.")

    if rolling_batch:
        path = f"{OUTPUT_PATH}/rolling_avg/rolling_avg.csv"
        df   = pd.DataFrame(rolling_batch)
        df.to_csv(path, mode="a",
                  header=not os.path.exists(path),
                  index=False)
        log.info(f"Saved {len(rolling_batch)} rolling avg records to CSV.")

# ─────────────────────────────────────────
# MAIN STREAMING LOOP
# ─────────────────────────────────────────
def stream():
    consumer        = create_consumer()
    window_buffer   = deque(maxlen=WINDOW_SIZE)
    anomalies_batch = []
    rolling_batch   = []
    total           = 0
    alert_count     = 0

    log.info("Streaming started — waiting for messages from Kafka...")
    log.info("Press Ctrl+C to stop.\n")

    try:
        for message in consumer:
            row = message.value
            total += 1

            # Add to rolling window
            window_buffer.append(row)

            # Check for anomaly
            anomaly = detect_anomaly(row)
            if anomaly:
                anomalies_batch.append(anomaly)
                alert_count += 1
                log.info(f"ANOMALY [{anomaly['anomaly_flag']}] "
                         f"Voltage: {anomaly['Voltage']} "
                         f"Power: {anomaly['Global_active_power']} "
                         f"@ {anomaly['Timestamp']}")

            # Compute rolling average every WINDOW_SIZE rows
            if total % WINDOW_SIZE == 0:
                avg = compute_rolling_avg(list(window_buffer))
                if avg:
                    rolling_batch.append(avg)
                    log.info(f"Rolling avg → Power: {avg['avg_power']}kW "
                             f"Voltage: {avg['avg_voltage']}V "
                             f"Intensity: {avg['avg_intensity']}A")

            # Save to disk every SAVE_EVERY rows
            if total % SAVE_EVERY == 0:
                save_results(anomalies_batch, rolling_batch)
                anomalies_batch = []
                rolling_batch   = []
                log.info(f"Progress: {total:,} rows | "
                         f"Alerts: {alert_count} | "
                         f"Buffer: {len(window_buffer)} rows")

    except KeyboardInterrupt:
        log.info("Stream stopped by user.")
        # Save whatever is remaining
        save_results(anomalies_batch, rolling_batch)

    finally:
        consumer.close()
        log.info(f"STAGE 4 COMPLETE — Total: {total:,} rows | Alerts: {alert_count}")

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    log.info("=" * 50)
    log.info("STAGE 4 - STREAMING ANALYTICS STARTED")
    log.info(f"Timestamp: {datetime.now()}")
    log.info("=" * 50)
    setup_folders()
    stream()

if __name__ == "__main__":
    main()