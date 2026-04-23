import os
import csv
import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer

# ─────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../logs/producer.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
KAFKA_BROKER     = "localhost:9092"
TOPIC_RAW        = "kafka-energy-raw"
TOPIC_ALERTS     = "kafka-energy-alerts"
CLEAN_DATA_PATH  = "../data/processed/clean_energy_data.csv"
DELAY_SECONDS    = 0.01   # 10ms between rows = ~100 rows/sec streaming speed

# ─────────────────────────────────────────
# STEP 1 — Connect to Kafka
# ─────────────────────────────────────────
def create_producer():
    log.info(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None
    )
    log.info("Kafka producer connected successfully.")
    return producer

# ─────────────────────────────────────────
# STEP 2 — Stream rows into Kafka
# ─────────────────────────────────────────
def stream_data(producer):
    log.info(f"Reading from {CLEAN_DATA_PATH}...")
    log.info("Starting stream — press Ctrl+C to stop.\n")

    total_sent    = 0
    alert_sent    = 0
    normal_sent   = 0

    with open(CLEAN_DATA_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Every row goes to kafka-energy-raw
            producer.send(
                TOPIC_RAW,
                key=row.get("Timestamp", "unknown"),
                value=row
            )
            normal_sent += 1

            # Anomaly rows ALSO go to kafka-energy-alerts
            if row.get("anomaly_flag") != "normal":
                producer.send(
                    TOPIC_ALERTS,
                    key=row.get("Timestamp", "unknown"),
                    value=row
                )
                alert_sent += 1
                log.info(f"ALERT sent → [{row['anomaly_flag']}] "
                         f"Voltage: {row.get('Voltage')} "
                         f"Power: {row.get('Global_active_power')} "
                         f"@ {row.get('Timestamp')}")

            total_sent += 1

            # Progress log every 10,000 rows
            if total_sent % 10000 == 0:
                log.info(f"Progress: {total_sent:,} rows sent "
                         f"| Alerts: {alert_sent} "
                         f"| Speed: ~{int(1/DELAY_SECONDS)} rows/sec")

            # Flush every 1000 rows to avoid buffer buildup
            if total_sent % 1000 == 0:
                producer.flush()

            time.sleep(DELAY_SECONDS)

    # Final flush
    producer.flush()
    return total_sent, alert_sent

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    log.info("=" * 50)
    log.info("STAGE 3 - KAFKA PRODUCER STARTED")
    log.info(f"Timestamp: {datetime.now()}")
    log.info("=" * 50)

    producer = create_producer()

    try:
        total, alerts = stream_data(producer)
        log.info("=" * 50)
        log.info(f"STAGE 3 COMPLETE")
        log.info(f"Total rows streamed : {total:,}")
        log.info(f"Alert rows sent     : {alerts:,}")
        log.info("=" * 50)

    except KeyboardInterrupt:
        log.info("Stream stopped manually by user.")
        producer.flush()

    finally:
        producer.close()
        log.info("Kafka producer closed.")

if __name__ == "__main__":
    main()