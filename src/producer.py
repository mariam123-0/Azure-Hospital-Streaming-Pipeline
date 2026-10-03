"""
Hospital patient-admission event generator.

Sends one synthetic patient event every 10-30 seconds to Azure Event Hubs
(through its Kafka-compatible endpoint). Some events are intentionally
"dirty" so the Silver layer has real cleaning work to do.

Usage:
    cp .env.example .env        # then fill in your Event Hub *Send* connection string
    pip install -r requirements.txt
    python src/producer.py
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

try:  # optional: load variables from a local .env file
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ----------------------------------------------------------------------------
# Config  (secrets come from environment variables - never hard-code them)
# ----------------------------------------------------------------------------
EVENTHUBS_NAMESPACE = os.getenv("EVENTHUBS_NAMESPACE", "depi-eventhub.servicebus.windows.net")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "hospitalstopic")
CONNECTION_STRING = os.environ["EVENTHUB_CONNECTION_STRING"]  # fails fast if missing

SEND_INTERVAL_SECONDS = (10, 30)  # random wait between two events

DEPARTMENTS = ["Emergency", "Surgery", "ICU", "Pediatrics", "Maternity", "Oncology", "Cardiology"]
DEPARTMENT_WEIGHTS = [30, 15, 20, 10, 10, 5, 10]
GENDERS = ["Male", "Female"]


def utc_now() -> datetime:
    """Naive UTC datetime (same as the old datetime.utcnow(), without the deprecation warning)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def inject_dirty_data(record: dict) -> dict:
    """Randomly corrupt a record on purpose (to be fixed in the Silver layer)."""
    if random.random() < 0.05:   # impossible age
        record["age"] = random.randint(101, 150)

    if random.random() < 0.05:   # admission time in the future
        record["admission_time"] = (utc_now() + timedelta(hours=random.randint(1, 72))).isoformat()

    if random.random() < 0.20:   # missing discharge time
        record["discharge_time"] = None

    return record


def generate_patient_event() -> dict:
    age = random.choices(
        population=list(range(0, 101)),
        weights=[1] * 5 + [5] * 20 + [10] * 51 + [5] * 20 + [1] * 5,
        k=1,
    )[0]

    department = random.choices(DEPARTMENTS, weights=DEPARTMENT_WEIGHTS, k=1)[0]

    admission_time = utc_now() - timedelta(hours=random.randint(0, 72))
    discharge_time = admission_time + timedelta(hours=random.randint(24, 120))

    event = {
        "patient_id": str(uuid.uuid4()),
        "gender": random.choice(GENDERS),
        "age": age,
        "department": department,
        "admission_time": admission_time.isoformat(),
        "discharge_time": discharge_time.isoformat(),
        "bed_id": random.randint(1, 500),
        "hospital_id": random.randint(1, 7),
    }
    return inject_dirty_data(event)


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=[f"{EVENTHUBS_NAMESPACE}:9093"],
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="$ConnectionString",
        sasl_plain_password=CONNECTION_STRING,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Producing to {EVENTHUBS_NAMESPACE}/{EVENT_HUB_NAME} - press Ctrl+C to stop")
    try:
        while True:
            event = generate_patient_event()
            producer.send(EVENT_HUB_NAME, event)
            print(f"Sent to Event Hub: {json.dumps(event)}")
            time.sleep(random.randint(*SEND_INTERVAL_SECONDS))
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
