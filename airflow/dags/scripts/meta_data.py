import os
import json
import random
import pandas as pd
from datetime import date
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

# ==============================================================
# 1. Config
# ==============================================================
FLAG_FILE = "/opt/airflow/dags/.metadata_sent"
STATIONS_CSV_PATH = "/opt/shared/stations_metadata.csv"
SENSORS_CSV_PATH = "/opt/shared/sensors_metadata.csv"

# ==============================================================
# 2. Egypt 27 governorates
#    Coordinates are representative governorate-center anchors
# ==============================================================
GOVERNORATES = {
    "Cairo": {"city": "Cairo", "lat": 30.0444, "lon": 31.2357, "source": "Nile"},
    "Giza": {"city": "Giza", "lat": 30.0131, "lon": 31.2089, "source": "Nile"},
    "Alexandria": {"city": "Alexandria", "lat": 31.2001, "lon": 29.9187, "source": "Canal"},
    "Dakahlia": {"city": "Mansoura", "lat": 31.0409, "lon": 31.3785, "source": "Nile"},
    "Red Sea": {"city": "Hurghada", "lat": 27.2579, "lon": 33.8116, "source": "Desalinated Blend"},
    "Beheira": {"city": "Damanhur", "lat": 31.0341, "lon": 30.4682, "source": "Canal"},
    "Fayoum": {"city": "Fayoum", "lat": 29.3084, "lon": 30.8428, "source": "Canal"},
    "Gharbia": {"city": "Tanta", "lat": 30.7865, "lon": 31.0004, "source": "Canal"},
    "Ismailia": {"city": "Ismailia", "lat": 30.5965, "lon": 32.2715, "source": "Canal"},
    "Menofia": {"city": "Shibin El Kom", "lat": 30.5549, "lon": 31.0124, "source": "Nile"},
    "Minya": {"city": "Minya", "lat": 28.1099, "lon": 30.7503, "source": "Nile"},
    "Qalyubia": {"city": "Banha", "lat": 30.4660, "lon": 31.1848, "source": "Nile"},
    "New Valley": {"city": "Kharga", "lat": 25.4514, "lon": 30.5464, "source": "Groundwater"},
    "Suez": {"city": "Suez", "lat": 29.9668, "lon": 32.5498, "source": "Canal"},
    "Aswan": {"city": "Aswan", "lat": 24.0889, "lon": 32.8998, "source": "Nile"},
    "Asyut": {"city": "Asyut", "lat": 27.1809, "lon": 31.1837, "source": "Nile"},
    "Beni Suef": {"city": "Beni Suef", "lat": 29.0661, "lon": 31.0994, "source": "Nile"},
    "Port Said": {"city": "Port Said", "lat": 31.2653, "lon": 32.3019, "source": "Canal"},
    "Damietta": {"city": "Damietta", "lat": 31.4165, "lon": 31.8133, "source": "Nile"},
    "Sharkia": {"city": "Zagazig", "lat": 30.5877, "lon": 31.5020, "source": "Canal"},
    "South Sinai": {"city": "El Tor", "lat": 28.2417, "lon": 33.6222, "source": "Desalinated Blend"},
    "Kafr El Sheikh": {"city": "Kafr El Sheikh", "lat": 31.1117, "lon": 30.9399, "source": "Canal"},
    "Matrouh": {"city": "Mersa Matruh", "lat": 31.3543, "lon": 27.2373, "source": "Groundwater"},
    "Luxor": {"city": "Luxor", "lat": 25.6872, "lon": 32.6396, "source": "Nile"},
    "Qena": {"city": "Qena", "lat": 26.1551, "lon": 32.7160, "source": "Nile"},
    "North Sinai": {"city": "Arish", "lat": 31.1313, "lon": 33.7984, "source": "Groundwater"},
    "Sohag": {"city": "Sohag", "lat": 26.5560, "lon": 31.6948, "source": "Nile"},
}

# ==============================================================
# 3. Tuning station counts
#    2 stations per governorate = 54 stations total
#    Good balance for demo realism and machine performance
# ==============================================================
STATIONS_PER_GOVERNORATE = 2

SENSOR_STAGE_CONFIG = [
    {"stage": "raw_intake", "sensor_type": "multi-parameter", "unit_group": "mixed"},
    {"stage": "sedimentation_outlet", "sensor_type": "clarifier-monitor", "unit_group": "mixed"},
    {"stage": "filtration_outlet", "sensor_type": "filter-monitor", "unit_group": "mixed"},
    {"stage": "final_outlet", "sensor_type": "final-quality-monitor", "unit_group": "mixed"},
]

MANUFACTURERS = ["Endress+Hauser", "Hach", "ABB", "Yokogawa", "Siemens"]
MODELS = {
    "multi-parameter": ["MP-100", "MP-200", "AquaSense-X"],
    "clarifier-monitor": ["CL-210", "SettloTrack-5", "Clarifier-Pro"],
    "filter-monitor": ["FLT-330", "BackwashEye", "FilterSense-9"],
    "final-quality-monitor": ["FQ-500", "ChloroSafe", "OutletGuard-X"],
}

STATUS_WEIGHTS = [0.88, 0.08, 0.04]  # Active, Under Maintenance, Inactive
SENSOR_STATUS_WEIGHTS = [0.90, 0.07, 0.03]  # Active, Maintenance, Faulty

# ==============================================================
# 4. Helpers
# ==============================================================
def random_offset(value, delta=0.08):
    return round(value + random.uniform(-delta, delta), 6)

def random_capacity(governorate_name, source_type):
    # realistic-ish ranges for drinking treatment plants
    if governorate_name in ["Cairo", "Giza", "Alexandria"]:
        return random.randint(250000, 900000)
    elif source_type == "Desalinated Blend":
        return random.randint(30000, 180000)
    elif source_type == "Groundwater":
        return random.randint(40000, 150000)
    else:
        return random.randint(80000, 400000)

def random_commission_date():
    return date(
        random.randint(2005, 2024),
        random.randint(1, 12),
        random.randint(1, 28)
    )

def random_recent_date():
    return date(
        random.randint(2023, 2026),
        random.randint(1, 12),
        random.randint(1, 28)
    )

def main():
    if os.path.exists(FLAG_FILE):
        print("⏭️ Metadata already generated and loaded. Skipping...")
        raise SystemExit(0)

    # ==============================================================
    # 5. Generate station metadata
    # ==============================================================
    stations = []
    station_counter = 1

    for governorate, info in GOVERNORATES.items():
        for local_idx in range(1, STATIONS_PER_GOVERNORATE + 1):
            station_id = f"EG-DW-{str(station_counter).zfill(4)}"
            station_name = f"{governorate} Drinking Water Plant {local_idx}"

            stations.append({
                "station_id": station_id,
                "station_name": station_name,
                "governorate": governorate,
                "city": info["city"],
                "latitude": random_offset(info["lat"]),
                "longitude": random_offset(info["lon"]),
                "water_source_type": info["source"],
                "station_category": "Drinking",
                "plant_capacity_m3_day": random_capacity(governorate, info["source"]),
                "operational_status": random.choices(
                    ["Active", "Under Maintenance", "Inactive"],
                    weights=STATUS_WEIGHTS,
                    k=1
                )[0],
                "commission_date": random_commission_date(),
                "data_source": "Simulation Metadata Seeder"
            })

            station_counter += 1

    df_stations = pd.DataFrame(stations)

    # Keep only primarily usable plants for streaming realism
    # But keep some maintenance/inactive stations in metadata to make the dimension richer
    print(f"📍 Generated {len(df_stations)} drinking water stations across 27 governorates.")

    # ==============================================================
    # 6. Generate sensor metadata
    #    4 logical sensor nodes per station
    # ==============================================================
    sensors = []

    for _, row in df_stations.iterrows():
        station_id = row["station_id"]

        for stage_cfg in SENSOR_STAGE_CONFIG:
            stage = stage_cfg["stage"]
            sensor_type = stage_cfg["sensor_type"]
            manufacturer = random.choice(MANUFACTURERS)
            model = random.choice(MODELS[sensor_type])

            sensor_id = f"{station_id}-{stage.upper()}"
            sensor_name = f"{row['station_name']} {stage.replace('_', ' ').title()} Sensor"

            sensors.append({
                "sensor_id": sensor_id,
                "station_id": station_id,
                "sensor_name": sensor_name,
                "sensor_stage": stage,
                "sensor_type": sensor_type,
                "manufacturer": manufacturer,
                "model": model,
                "unit_group": stage_cfg["unit_group"],
                "installation_date": row["commission_date"],
                "calibration_date": random_recent_date(),
                "sensor_status": random.choices(
                    ["Active", "Maintenance", "Faulty"],
                    weights=SENSOR_STATUS_WEIGHTS,
                    k=1
                )[0]
            })

    df_sensors = pd.DataFrame(sensors)
    print(f"🎛️ Generated {len(df_sensors)} logical sensors.")

    # ==============================================================
    # 7. Save CSVs to shared volume
    # ==============================================================
    try:
        df_stations.to_csv(STATIONS_CSV_PATH, index=False, encoding="utf-8-sig")
        print(f"✅ stations_metadata.csv saved to {STATIONS_CSV_PATH}")
    except Exception as e:
        print(f"❌ Failed to save stations CSV: {e}")
        raise

    try:
        df_sensors.to_csv(SENSORS_CSV_PATH, index=False, encoding="utf-8-sig")
        print(f"✅ sensors_metadata.csv saved to {SENSORS_CSV_PATH}")
    except Exception as e:
        print(f"❌ Failed to save sensors CSV: {e}")
        raise

    # ==============================================================
    # 8. Load into SQL Server
    # ==============================================================
    try:
        password = quote_plus("YourStrong@Password123")
        connection_str = f"mssql+pytds://sa:{password}@ewis_sql_server:1433/EWIS_Warehouse"
        engine = create_engine(connection_str)

        with engine.begin() as conn:
            # Truncate/reload dimensions
            conn.execute(text("DELETE FROM dbo.dim_sensor"))
            conn.execute(text("DELETE FROM dbo.dim_station"))

            df_stations_sql = df_stations.copy()
            df_stations_sql.to_sql(
                "dim_station",
                con=conn,
                schema="dbo",
                if_exists="append",
                index=False
            )

            df_sensors_sql = df_sensors.copy()
            df_sensors_sql.to_sql(
                "dim_sensor",
                con=conn,
                schema="dbo",
                if_exists="append",
                index=False
            )

        print("✅ dim_station and dim_sensor loaded successfully into SQL Server.")

    except Exception as e:
        print(f"❌ Failed loading metadata into SQL Server: {e}")
        raise

    # ==============================================================
    # 9. Create flag file
    # ==============================================================
    with open(FLAG_FILE, "w") as f:
        f.write("sent")

    print("✅ Metadata pipeline completed successfully.")

if __name__ == "__main__":
    main()