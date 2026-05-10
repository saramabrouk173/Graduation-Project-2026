import pandas as pd
import numpy as np
import random
import uuid
import math
import json
import time
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer


def main():
    # ==============================================================
    # 1. Kafka settings
    # ==============================================================
    KAFKA_TOPIC = "water_readings"
    KAFKA_SERVER = "kafka-broker-1:9092"

    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            request_timeout_ms=30000,
            linger_ms=100,
            batch_size=32768
        )
        print(f"✅ Connected to Kafka broker: {KAFKA_SERVER}")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        raise SystemExit(1)

    # ==============================================================
    # 2. Load metadata
    # ==============================================================
    STATIONS_FILE = "/opt/shared/stations_metadata.csv"
    SENSORS_FILE = "/opt/shared/sensors_metadata.csv"

    try:
        stations_df = pd.read_csv(STATIONS_FILE)
        sensors_df = pd.read_csv(SENSORS_FILE)

        stations_df = stations_df[stations_df["operational_status"].isin(["Active", "Under Maintenance"])].copy()
        final_sensors = sensors_df[sensors_df["sensor_stage"] == "final_outlet"][["station_id", "sensor_id"]].copy()

        stations_df = stations_df.merge(final_sensors, on="station_id", how="left")

        print(f"📊 Loaded {len(stations_df)} active/maintenance stations for streaming.")
    except Exception as e:
        print(f"❌ Could not load metadata files: {e}")
        raise SystemExit(1)

    # ==============================================================
    # 3. Source-specific baselines
    # ==============================================================
    SOURCE_BASELINES = {
        "Nile": {
            "raw_turbidity_ntu": (18.0, 8.0),
            "raw_ph": (7.5, 0.18),
            "raw_conductivity_us_cm": (420.0, 120.0),
            "raw_temperature_c": (24.0, 3.0),
            "raw_ammonia_mg_l": (0.10, 0.08),
            "raw_alkalinity_mg_l": (105.0, 20.0),
        },
        "Canal": {
            "raw_turbidity_ntu": (28.0, 14.0),
            "raw_ph": (7.4, 0.22),
            "raw_conductivity_us_cm": (600.0, 180.0),
            "raw_temperature_c": (25.0, 3.2),
            "raw_ammonia_mg_l": (0.16, 0.10),
            "raw_alkalinity_mg_l": (120.0, 24.0),
        },
        "Groundwater": {
            "raw_turbidity_ntu": (4.0, 2.0),
            "raw_ph": (7.7, 0.15),
            "raw_conductivity_us_cm": (1100.0, 250.0),
            "raw_temperature_c": (23.0, 2.0),
            "raw_ammonia_mg_l": (0.05, 0.04),
            "raw_alkalinity_mg_l": (160.0, 25.0),
        },
        "Desalinated Blend": {
            "raw_turbidity_ntu": (3.0, 1.5),
            "raw_ph": (7.8, 0.12),
            "raw_conductivity_us_cm": (850.0, 200.0),
            "raw_temperature_c": (26.0, 2.5),
            "raw_ammonia_mg_l": (0.03, 0.02),
            "raw_alkalinity_mg_l": (80.0, 18.0),
        },
        "Mixed": {
            "raw_turbidity_ntu": (15.0, 6.0),
            "raw_ph": (7.5, 0.18),
            "raw_conductivity_us_cm": (700.0, 160.0),
            "raw_temperature_c": (24.0, 2.7),
            "raw_ammonia_mg_l": (0.08, 0.05),
            "raw_alkalinity_mg_l": (110.0, 20.0),
        }
    }

    # ==============================================================
    # 4. Event realism rates
    # ==============================================================
    SPIKE_RATE = 0.020
    OUTLIER_RATE = 0.007
    MISSING_RATE = 0.020
    DUPLICATE_RATE = 0.004
    LATE_EVENT_RATE = 0.010
    PROCESS_DEGRADATION_RATE = 0.030
    SENSOR_DRIFT_RATE = 0.012

    # ==============================================================
    # 5. Helpers
    # ==============================================================
    def iso(ts: datetime):
        return ts.isoformat().replace("+00:00", "Z")

    def diurnal_factor(ts: datetime):
        seconds = (ts - ts.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
        phase = 2 * math.pi * (seconds / 86400.0)
        return math.sin(phase)

    def bounded_normal(mean, std, lower=None, upper=None):
        value = np.random.normal(mean, std)
        if lower is not None:
            value = max(value, lower)
        if upper is not None:
            value = min(value, upper)
        return value

    def maybe_round(x, nd=3):
        if x is None:
            return None
        return round(float(x), nd)

    # ==============================================================
    # 6. Generate one realistic process event
    # ==============================================================
    def generate_station_event(station_row, t):
        source = station_row["water_source_type"]
        source_base = SOURCE_BASELINES.get(source, SOURCE_BASELINES["Mixed"])
        d = diurnal_factor(t)

        event_time = t
        if random.random() < LATE_EVENT_RATE:
            delay_seconds = random.randint(20, 240)
            event_time = t - timedelta(seconds=delay_seconds)

        # ---------- RAW INTAKE ----------
        raw_turbidity = bounded_normal(
            source_base["raw_turbidity_ntu"][0] + 5.0 * max(d, 0),
            source_base["raw_turbidity_ntu"][1],
            lower=0.1, upper=500.0
        )

        raw_ph = bounded_normal(
            source_base["raw_ph"][0] + 0.04 * d,
            source_base["raw_ph"][1],
            lower=5.0, upper=9.5
        )

        raw_conductivity = bounded_normal(
            source_base["raw_conductivity_us_cm"][0],
            source_base["raw_conductivity_us_cm"][1],
            lower=50.0, upper=5000.0
        )

        raw_temperature = bounded_normal(
            source_base["raw_temperature_c"][0] + 2.8 * d,
            source_base["raw_temperature_c"][1],
            lower=5.0, upper=45.0
        )

        raw_ammonia = bounded_normal(
            source_base["raw_ammonia_mg_l"][0] + 0.02 * max(d, 0),
            source_base["raw_ammonia_mg_l"][1],
            lower=0.0, upper=5.0
        )

        raw_alkalinity = bounded_normal(
            source_base["raw_alkalinity_mg_l"][0],
            source_base["raw_alkalinity_mg_l"][1],
            lower=10.0, upper=400.0
        )

      # ---------- PROCESS CONDITION ----------
        process_degraded = random.random() < PROCESS_DEGRADATION_RATE
        sensor_drift = random.random() < SENSOR_DRIFT_RATE

        # ✅ efficiencies أعلى شوية علشان الطبيعي يبقى green
        coag_efficiency = bounded_normal(88.0, 5.0, lower=55.0, upper=99.0)
        filter_efficiency = bounded_normal(97.0, 2.0, lower=88.0, upper=99.7)

        if process_degraded:
            coag_efficiency -= random.uniform(8.0, 18.0)
            filter_efficiency -= random.uniform(4.0, 10.0)

        coag_efficiency = max(35.0, min(coag_efficiency, 99.0))
        filter_efficiency = max(75.0, min(filter_efficiency, 99.7))

        # ---------- SEDIMENTATION ----------
        settled_turbidity = raw_turbidity * (1 - coag_efficiency / 100.0)
        settled_turbidity = max(0.08, settled_turbidity)

        sludge_blanket = bounded_normal(0.8, 0.22, lower=0.2, upper=3.5)
        if process_degraded:
            sludge_blanket += random.uniform(0.3, 1.0)

        estimated_settling_efficiency = ((raw_turbidity - settled_turbidity) / max(raw_turbidity, 0.1)) * 100.0
        estimated_settling_efficiency = max(0.0, min(estimated_settling_efficiency, 100.0))

        # ---------- FILTRATION ----------
        filtered_turbidity = settled_turbidity * (1 - filter_efficiency / 100.0)
        filtered_turbidity = max(0.03, filtered_turbidity)

        head_loss = bounded_normal(1.0, 0.30, lower=0.2, upper=4.0)
        filtration_rate = bounded_normal(6.5, 0.8, lower=3.0, upper=12.0)

        if process_degraded:
            head_loss += random.uniform(0.4, 1.0)
            filtration_rate += random.uniform(0.5, 1.5)
            filtered_turbidity += random.uniform(0.05, 0.35)

        # ---------- FINAL DISINFECTION ----------
        final_residual_chlorine = bounded_normal(0.65, 0.10, lower=0.15, upper=1.4)
        final_ph = bounded_normal(7.35, 0.12, lower=6.7, upper=8.3)

        # ✅ الطبيعي: final أقل شوية من filtered أو قريب جدًا
        final_turbidity = max(0.02, filtered_turbidity * random.uniform(0.80, 0.95))

        if process_degraded:
            final_residual_chlorine -= random.uniform(0.05, 0.18)
            final_turbidity += random.uniform(0.05, 0.40)
            final_ph += random.uniform(-0.20, 0.20)

        # sensor drift effect
        if sensor_drift:
            drift_field = random.choice([
                "raw_turbidity", "raw_ph", "raw_conductivity", "raw_temperature",
                "settled_turbidity", "filtered_turbidity", "head_loss",
                "final_residual_chlorine", "final_ph", "final_turbidity"
            ])
            drift_factor = random.uniform(1.08, 1.25)

            if drift_field == "raw_turbidity":
                raw_turbidity *= drift_factor
            elif drift_field == "raw_ph":
                raw_ph += random.uniform(0.25, 0.60)
            elif drift_field == "raw_conductivity":
                raw_conductivity *= drift_factor
            elif drift_field == "raw_temperature":
                raw_temperature += random.uniform(0.8, 2.0)
            elif drift_field == "settled_turbidity":
                settled_turbidity *= drift_factor
            elif drift_field == "filtered_turbidity":
                filtered_turbidity *= drift_factor
            elif drift_field == "head_loss":
                head_loss *= drift_factor
            elif drift_field == "final_residual_chlorine":
                final_residual_chlorine *= random.uniform(0.70, 0.90)
            elif drift_field == "final_ph":
                final_ph += random.uniform(0.20, 0.50)
            elif drift_field == "final_turbidity":
                final_turbidity *= drift_factor

        # spikes and outliers
        if random.random() < SPIKE_RATE:
            spike_field = random.choice([
                "raw_turbidity", "raw_ammonia", "settled_turbidity",
                "filtered_turbidity", "head_loss", "final_turbidity", "final_residual_chlorine"
            ])
            if spike_field == "final_residual_chlorine":
                final_residual_chlorine *= random.uniform(1.6, 2.5)
            else:
                locals()[spike_field] = locals()[spike_field] * random.uniform(1.8, 4.0)

        if random.random() < OUTLIER_RATE:
            outlier_type = random.choice(["ph", "chlorine_low", "chlorine_high", "turbidity"])
            if outlier_type == "ph":
                final_ph = random.choice([random.uniform(5.3, 6.0), random.uniform(8.9, 9.6)])
            elif outlier_type == "chlorine_low":
                final_residual_chlorine = random.uniform(0.0, 0.08)
            elif outlier_type == "chlorine_high":
                final_residual_chlorine = random.uniform(1.6, 2.5)
            elif outlier_type == "turbidity":
                final_turbidity = random.uniform(1.2, 4.5)

        # recompute a few bounded values after anomalies
        raw_turbidity = min(max(raw_turbidity, 0.05), 500.0)
        raw_ph = min(max(raw_ph, 0.0), 14.0)
        raw_conductivity = min(max(raw_conductivity, 20.0), 6000.0)
        raw_temperature = min(max(raw_temperature, 1.0), 50.0)
        raw_ammonia = min(max(raw_ammonia, 0.0), 10.0)
        raw_alkalinity = min(max(raw_alkalinity, 5.0), 500.0)
        settled_turbidity = min(max(settled_turbidity, 0.05), 300.0)
        sludge_blanket = min(max(sludge_blanket, 0.05), 6.0)
        estimated_settling_efficiency = min(max(estimated_settling_efficiency, 0.0), 100.0)
        filtered_turbidity = min(max(filtered_turbidity, 0.02), 50.0)
        head_loss = min(max(head_loss, 0.1), 6.0)
        filtration_rate = min(max(filtration_rate, 1.0), 20.0)
        final_residual_chlorine = min(max(final_residual_chlorine, 0.0), 3.0)
        final_ph = min(max(final_ph, 0.0), 14.0)
        final_turbidity = min(max(final_turbidity, 0.02), 10.0)

        # overall derived before Spark recalculates them too
        turbidity_removal_pct = ((raw_turbidity - final_turbidity) / max(raw_turbidity, 0.1)) * 100.0
        turbidity_removal_pct = max(0.0, min(turbidity_removal_pct, 100.0))

        # occasional missing values
        event = {
            "reading_id": str(uuid.uuid4()),
            "station_id": station_row["station_id"],
            "sensor_id": station_row["sensor_id"] if pd.notna(station_row["sensor_id"]) else f"{station_row['station_id']}-FINAL_OUTLET",
            "timestamp": iso(event_time),
            "water_source_type": station_row["water_source_type"],
            "station_category": station_row["station_category"],
            "raw_turbidity_ntu": maybe_round(raw_turbidity),
            "raw_ph": maybe_round(raw_ph),
            "raw_conductivity_us_cm": maybe_round(raw_conductivity),
            "raw_temperature_c": maybe_round(raw_temperature),
            "raw_ammonia_mg_l": maybe_round(raw_ammonia),
            "raw_alkalinity_mg_l": maybe_round(raw_alkalinity),
            "settled_turbidity_ntu": maybe_round(settled_turbidity),
            "sludge_blanket_level_m": maybe_round(sludge_blanket),
            "estimated_settling_efficiency_pct": maybe_round(estimated_settling_efficiency),
            "filtered_turbidity_ntu": maybe_round(filtered_turbidity),
            "head_loss_m": maybe_round(head_loss),
            "filtration_rate_m_h": maybe_round(filtration_rate),
            "final_residual_chlorine_mg_l": maybe_round(final_residual_chlorine),
            "final_ph": maybe_round(final_ph),
            "final_turbidity_ntu": maybe_round(final_turbidity),
            "data_source": "stream"
        }

        if random.random() < MISSING_RATE:
            nullable_fields = [
                "raw_ammonia_mg_l", "raw_alkalinity_mg_l", "sludge_blanket_level_m",
                "estimated_settling_efficiency_pct", "head_loss_m", "filtration_rate_m_h"
            ]
            miss_col = random.choice(nullable_fields)
            event[miss_col] = None

        return event

    # ==============================================================
    # 7. Main stream loop
    # ==============================================================
    print("📡 Starting continuous EWIS drinking water treatment stream...")

    try:
        while True:
            current_time = datetime.now(timezone.utc)

            for _, station in stations_df.iterrows():
                event = generate_station_event(station, current_time)
                producer.send(KAFKA_TOPIC, value=event)

                if random.random() < DUPLICATE_RATE:
                    dup = event.copy()
                    dup["reading_id"] = str(uuid.uuid4())
                    producer.send(KAFKA_TOPIC, value=dup)

            producer.flush()
            print(f"⏰ Sent process-aware sensor batch at {current_time.strftime('%H:%M:%S')} UTC")
            time.sleep(5)

    except KeyboardInterrupt:
        print("🛑 Sensor stream stopped manually.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()