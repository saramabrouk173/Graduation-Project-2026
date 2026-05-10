from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, lit, when, expr, current_timestamp,
    year, month, dayofmonth, hour, minute, second, round as spark_round,
    greatest, least
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

import pandas as pd
import numpy as np
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

# ==============================================================
# 1. Spark Session
# ==============================================================
spark = SparkSession.builder \
    .appName("EWIS_Enterprise_Intelligence_Engine") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config(
        "spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ==============================================================
# 2. Static reference data
# ==============================================================
stations_path = "/opt/shared/stations_metadata.csv"
sensors_path = "/opt/shared/sensors_metadata.csv"
rules_path = "/opt/shared/reference_threshold_rules.csv"

try:
    stations_static = spark.read.csv(stations_path, header=True, inferSchema=True)
    sensors_static = spark.read.csv(sensors_path, header=True, inferSchema=True)
    rules_static = spark.read.csv(rules_path, header=True, inferSchema=True)

    stations_static = stations_static.select(
        "station_id", "station_name", "governorate", "city",
        "latitude", "longitude", "water_source_type",
        "station_category", "plant_capacity_m3_day", "operational_status"
    )

    sensors_static = sensors_static.select(
        "sensor_id", "station_id", "sensor_stage", "sensor_type", "sensor_status"
    )

    print("✅ Static metadata and rules loaded successfully.")
except Exception as e:
    print(f"❌ Failed to load static reference files: {e}")
    raise

# ==============================================================
# 3. Kafka event schema
# ==============================================================
event_schema = StructType([
    StructField("reading_id", StringType(), True),
    StructField("station_id", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("water_source_type", StringType(), True),
    StructField("station_category", StringType(), True),

    StructField("raw_turbidity_ntu", DoubleType(), True),
    StructField("raw_ph", DoubleType(), True),
    StructField("raw_conductivity_us_cm", DoubleType(), True),
    StructField("raw_temperature_c", DoubleType(), True),
    StructField("raw_ammonia_mg_l", DoubleType(), True),
    StructField("raw_alkalinity_mg_l", DoubleType(), True),

    StructField("settled_turbidity_ntu", DoubleType(), True),
    StructField("sludge_blanket_level_m", DoubleType(), True),
    StructField("estimated_settling_efficiency_pct", DoubleType(), True),

    StructField("filtered_turbidity_ntu", DoubleType(), True),
    StructField("head_loss_m", DoubleType(), True),
    StructField("filtration_rate_m_h", DoubleType(), True),

    StructField("final_residual_chlorine_mg_l", DoubleType(), True),
    StructField("final_ph", DoubleType(), True),
    StructField("final_turbidity_ntu", DoubleType(), True),

    StructField("data_source", StringType(), True)
])

# ==============================================================
# 4. Read stream from Kafka
# ==============================================================
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker-1:9092") \
    .option("subscribe", "water_readings") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), event_schema).alias("data")) \
    .select("data.*")

# ==============================================================
# 5. Stream cleaning and enrichment
# ==============================================================
base_stream = parsed_stream \
    .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
    .dropna(subset=["reading_id", "station_id", "sensor_id", "timestamp"]) \
    .withWatermark("event_timestamp", "10 minutes") \
    .dropDuplicates(["station_id", "sensor_id", "event_timestamp"]) \
    .join(stations_static, on="station_id", how="left") \
    .join(
        sensors_static.select("sensor_id", "sensor_stage", "sensor_type", "sensor_status"),
        on="sensor_id",
        how="left"
    )

# ==============================================================
# 6. Basic validations and derived measures
# ==============================================================
validated_stream = base_stream \
    .withColumn(
        "quality_flag",
        when(col("event_timestamp").isNull(), lit("invalid"))
        .when(
            col("raw_turbidity_ntu").isNull() |
            col("settled_turbidity_ntu").isNull() |
            col("filtered_turbidity_ntu").isNull() |
            col("final_turbidity_ntu").isNull() |
            col("final_residual_chlorine_mg_l").isNull() |
            col("final_ph").isNull(),
            lit("partial")
        )
        .when(~col("raw_ph").between(0, 14), lit("invalid"))
        .when(~col("final_ph").between(0, 14), lit("invalid"))
        .when(~col("raw_turbidity_ntu").between(0, 1000), lit("invalid"))
        .when(~col("settled_turbidity_ntu").between(0, 300), lit("invalid"))
        .when(~col("filtered_turbidity_ntu").between(0, 100), lit("invalid"))
        .when(~col("final_turbidity_ntu").between(0, 20), lit("invalid"))
        .when(~col("final_residual_chlorine_mg_l").between(0, 5), lit("invalid"))
        .when(~col("estimated_settling_efficiency_pct").between(0, 100), lit("invalid"))
        .when(col("settled_turbidity_ntu") > col("raw_turbidity_ntu") * 1.10, lit("suspicious"))
        .when(col("filtered_turbidity_ntu") > col("settled_turbidity_ntu") * 1.20, lit("suspicious"))
        .when(col("final_turbidity_ntu") > col("filtered_turbidity_ntu") * 1.20, lit("suspicious"))
        .when(expr("ABS(final_ph - raw_ph) > 1.5"), lit("suspicious"))
        .when((col("head_loss_m").isNotNull()) & (col("head_loss_m") < 0.2), lit("suspicious"))
        .otherwise(lit("valid"))
    ) \
    .withColumn(
        "anomaly_flag",
        when(
            (col("raw_turbidity_ntu") > 200) |
            (col("final_turbidity_ntu") > 2.0) |
            (col("final_residual_chlorine_mg_l") < 0.10) |
            (col("head_loss_m") > 2.8) |
            (col("sludge_blanket_level_m") > 2.5),
            lit(1)
        ).otherwise(lit(0))
    ) \
    .withColumn(
        "turbidity_removal_pct",
        when(
            col("raw_turbidity_ntu").isNull() | col("final_turbidity_ntu").isNull(),
            None
        ).otherwise(
            spark_round(
                ((col("raw_turbidity_ntu") - col("final_turbidity_ntu")) /
                 greatest(col("raw_turbidity_ntu"), lit(0.1))) * 100,
                2
            )
        )
    ) \
    .withColumn(
        "chlorine_compliance_flag",
        when(col("final_residual_chlorine_mg_l").between(0.20, 1.00), lit(1)).otherwise(lit(0))
    ) \
    .withColumn(
        "filter_backwash_needed_flag",
        when((col("head_loss_m") > 2.8) | (col("filtered_turbidity_ntu") > 0.8), lit(1)).otherwise(lit(0))
    )

# ==============================================================
# 7. Scientific / business scoring
# ==============================================================
scored_stream = validated_stream \
    .withColumn(
        "contamination_risk_score",
        least(
            lit(100.0),
            spark_round(
                (
                    when(col("raw_ammonia_mg_l") > 0.30, (col("raw_ammonia_mg_l") - 0.30) * 40).otherwise(lit(0.0)) +
                    when(col("raw_turbidity_ntu") > 50, (col("raw_turbidity_ntu") - 50) * 0.15).otherwise(lit(0.0)) +
                    when(col("settled_turbidity_ntu") > 10, (col("settled_turbidity_ntu") - 10) * 1.2).otherwise(lit(0.0)) +
                    when(col("filtered_turbidity_ntu") > 0.3, (col("filtered_turbidity_ntu") - 0.3) * 18).otherwise(lit(0.0)) +
                    when(col("final_turbidity_ntu") > 0.3, (col("final_turbidity_ntu") - 0.3) * 22).otherwise(lit(0.0)) +
                    when((col("final_ph") < 6.5) | (col("final_ph") > 8.5), lit(15.0)).otherwise(lit(0.0))
                ),
                2
            )
        )
    ) \
    .withColumn(
        "bacterial_regrowth_risk_score",
        least(
            lit(100.0),
            spark_round(
                (
                    when(col("final_residual_chlorine_mg_l") < 0.20, (0.20 - col("final_residual_chlorine_mg_l")) * 150).otherwise(lit(0.0)) +
                    when(col("final_turbidity_ntu") > 0.30, (col("final_turbidity_ntu") - 0.30) * 25).otherwise(lit(0.0)) +
                    when(col("raw_temperature_c") > 28, (col("raw_temperature_c") - 28) * 3.5).otherwise(lit(0.0)) +
                    when(col("raw_ammonia_mg_l") > 0.20, (col("raw_ammonia_mg_l") - 0.20) * 35).otherwise(lit(0.0)) +
                    when((col("final_ph") > 8.5) | (col("final_ph") < 6.5), lit(12.0)).otherwise(lit(0.0))
                ),
                2
            )
        )
    ) \
    .withColumn(
        "process_performance_score",
        least(
            lit(100.0),
            greatest(
                lit(0.0),
                spark_round(
                    lit(100.0) -
                    (
                        when(col("settled_turbidity_ntu") > 10, (col("settled_turbidity_ntu") - 10) * 1.5).otherwise(lit(0.0)) +
                        when(col("filtered_turbidity_ntu") > 0.3, (col("filtered_turbidity_ntu") - 0.3) * 15).otherwise(lit(0.0)) +
                        when(col("head_loss_m") > 2.0, (col("head_loss_m") - 2.0) * 18).otherwise(lit(0.0)) +
                        when(col("filtration_rate_m_h") > 9.0, (col("filtration_rate_m_h") - 9.0) * 6).otherwise(lit(0.0)) +
                        when(col("final_turbidity_ntu") > 0.3, (col("final_turbidity_ntu") - 0.3) * 20).otherwise(lit(0.0))
                    ),
                    2
                )
            )
        )
    ) \
    .withColumn(
        "wqi_score",
        spark_round(
            lit(100.0) -
            (
                when(col("final_turbidity_ntu") > 0.3, least((col("final_turbidity_ntu") - 0.3) * 20, lit(35.0))).otherwise(lit(0.0)) +
                when(col("final_residual_chlorine_mg_l") < 0.20, least((0.20 - col("final_residual_chlorine_mg_l")) * 120, lit(30.0))).otherwise(lit(0.0)) +
                when((col("final_ph") < 6.5) | (col("final_ph") > 8.5), lit(15.0)).otherwise(lit(0.0)) +
                when(col("raw_ammonia_mg_l") > 0.30, least((col("raw_ammonia_mg_l") - 0.30) * 35, lit(20.0))).otherwise(lit(0.0))
            ),
            2
        )
    ) \
    .withColumn("wqi_score", least(lit(100.0), greatest(lit(0.0), col("wqi_score")))) \
    .withColumn(
        "wqi_class",
        when(col("wqi_score") >= 90, lit("Excellent"))
        .when(col("wqi_score") >= 75, lit("Good"))
        .when(col("wqi_score") >= 60, lit("Watch"))
        .otherwise(lit("Unsafe"))
    ) \
    .withColumn(
        "breach_flag",
        when(
            (col("final_residual_chlorine_mg_l") < 0.20) |
            (col("final_turbidity_ntu") > 0.30) |
            (col("final_ph") < 6.5) | (col("final_ph") > 8.5) |
            (col("filtered_turbidity_ntu") > 0.30) |
            (col("head_loss_m") > 2.8) |
            (col("raw_ammonia_mg_l") > 0.30),
            lit(1)
        ).otherwise(lit(0))
    ) \
    .withColumn(
        "severity_level",
        when(
            (col("final_residual_chlorine_mg_l") < 0.10) |
            (col("final_turbidity_ntu") > 1.0) |
            (col("head_loss_m") > 2.8) |
            (col("bacterial_regrowth_risk_score") >= 60) |
            (col("contamination_risk_score") >= 60),
            lit("critical")
        ).when(
            (col("breach_flag") == 1) | (col("anomaly_flag") == 1),
            lit("warning")
        ).otherwise(lit("normal"))
    ) \
    .withColumn(
        "event_date_key",
        (year(col("event_timestamp")) * 10000 + month(col("event_timestamp")) * 100 + dayofmonth(col("event_timestamp")))
    ) \
    .withColumn(
        "event_time_key",
        (hour(col("event_timestamp")) * 10000 + minute(col("event_timestamp")) * 100 + second(col("event_timestamp")))
    ) \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("ingestion_time", current_timestamp())

warehouse_clean_stream = scored_stream.filter(
    col("event_timestamp").isNotNull() &
    col("station_id").isNotNull() &
    col("sensor_id").isNotNull()
)

# ==============================================================
# 8. SQL connection
# ==============================================================
password = quote_plus("YourStrong@Password123")
connection_str = f"mssql+pytds://sa:{password}@ewis_sql_server:1433/EWIS_Warehouse"
engine = create_engine(connection_str)

# ==============================================================
# 9. Helper: refresh marts using SQL
# ==============================================================
def refresh_marts_sql(conn):
    # ----------------------------------------------------------
    # mart_station_latest_status (calibrated colors + fixed priority)
    # ----------------------------------------------------------
    conn.execute(text("""
        DELETE FROM dbo.mart_station_latest_status;

        WITH latest_wqi AS (
            SELECT
                station_id,
                event_timestamp,
                wqi_score,
                wqi_class,
                contamination_risk_score,
                bacterial_regrowth_risk_score,
                ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY event_timestamp DESC) AS rn
            FROM dbo.fact_water_quality_index
        ),
        latest_reading AS (
            SELECT
                station_id,
                MAX(event_timestamp) AS latest_reading_timestamp,
                MAX(CASE WHEN quality_flag = 'valid' THEN event_timestamp END) AS latest_valid_reading_timestamp
            FROM dbo.fact_sensor_reading
            GROUP BY station_id
        ),
        recent_quality AS (
            SELECT
                station_id,
                SUM(CASE WHEN quality_flag = 'invalid' THEN 1 ELSE 0 END) AS invalid_count_24h,
                SUM(CASE WHEN quality_flag = 'partial' THEN 1 ELSE 0 END) AS partial_count_24h,
                SUM(CASE WHEN quality_flag = 'suspicious' THEN 1 ELSE 0 END) AS suspicious_count_24h,
                SUM(CASE WHEN quality_flag = 'valid' THEN 1 ELSE 0 END) AS valid_count_24h,
                SUM(CASE WHEN anomaly_flag = 1 THEN 1 ELSE 0 END) AS anomaly_count_24h
            FROM dbo.fact_sensor_reading
            WHERE event_timestamp >= DATEADD(HOUR, -24, SYSUTCDATETIME())
            GROUP BY station_id
        ),
        recent_health AS (
            SELECT
                station_id,
                AVG(COALESCE(uptime_percentage, 0)) AS avg_uptime_24h,
                SUM(CASE WHEN health_status = 'critical' THEN 1 ELSE 0 END) AS critical_health_samples_24h,
                SUM(CASE WHEN health_status = 'degraded' THEN 1 ELSE 0 END) AS degraded_health_samples_24h,
                SUM(CASE WHEN drift_risk_flag = 1 THEN 1 ELSE 0 END) AS drift_risk_count_24h,
                SUM(CASE WHEN stale_signal_flag = 1 THEN 1 ELSE 0 END) AS stale_signal_count_24h,
                SUM(anomaly_count) AS anomaly_events_24h
            FROM dbo.fact_sensor_health
            WHERE event_timestamp >= DATEADD(HOUR, -24, SYSUTCDATETIME())
            GROUP BY station_id
        ),
        active_alerts AS (
            SELECT
                station_id,
                COUNT(DISTINCT CONCAT(alert_type, '|', ISNULL(stage_name, ''))) AS active_alert_count,
                SUM(CASE WHEN severity_level = 'critical' THEN 1 ELSE 0 END) AS critical_open_alert_count,
                SUM(CASE WHEN severity_level = 'warning' THEN 1 ELSE 0 END) AS warning_open_alert_count,
                SUM(CASE WHEN alert_type IN (
                        'HIGH_FINAL_TURBIDITY',
                        'LOW_FINAL_CHLORINE',
                        'FINAL_PH_OUT_OF_RANGE',
                        'BACTERIAL_REGROWTH_RISK',
                        'CONTAMINATION_RISK_HIGH'
                    ) THEN 1 ELSE 0 END) AS active_water_alert_count,
                SUM(CASE WHEN alert_type IN (
                        'HIGH_FINAL_TURBIDITY',
                        'LOW_FINAL_CHLORINE',
                        'FINAL_PH_OUT_OF_RANGE',
                        'BACTERIAL_REGROWTH_RISK',
                        'CONTAMINATION_RISK_HIGH'
                    ) AND severity_level = 'critical' THEN 1 ELSE 0 END) AS critical_water_alert_count,
                SUM(CASE WHEN alert_type IN (
                        'HIGH_FINAL_TURBIDITY',
                        'LOW_FINAL_CHLORINE',
                        'FINAL_PH_OUT_OF_RANGE',
                        'BACTERIAL_REGROWTH_RISK',
                        'CONTAMINATION_RISK_HIGH'
                    ) AND severity_level = 'warning' THEN 1 ELSE 0 END) AS warning_water_alert_count,
                SUM(CASE WHEN alert_type IN (
                        'FILTER_BACKWASH_NEEDED',
                        'FILTER_EFFLUENT_TURBIDITY_HIGH',
                        'SLUDGE_BLANKET_HIGH',
                        'RAW_AMMONIA_HIGH'
                    ) THEN 1 ELSE 0 END) AS active_process_alert_count,
                SUM(CASE WHEN alert_type IN (
                        'FILTER_BACKWASH_NEEDED',
                        'FILTER_EFFLUENT_TURBIDITY_HIGH',
                        'SLUDGE_BLANKET_HIGH',
                        'RAW_AMMONIA_HIGH'
                    ) AND severity_level = 'critical' THEN 1 ELSE 0 END) AS critical_process_alert_count,
                SUM(CASE WHEN alert_type IN (
                        'FILTER_BACKWASH_NEEDED',
                        'FILTER_EFFLUENT_TURBIDITY_HIGH',
                        'SLUDGE_BLANKET_HIGH',
                        'RAW_AMMONIA_HIGH'
                    ) AND severity_level = 'warning' THEN 1 ELSE 0 END) AS warning_process_alert_count
            FROM dbo.fact_alert
            WHERE status = 'open'
            GROUP BY station_id
        ),
        top_open_alert AS (
            SELECT
                station_id,
                alert_type,
                severity_level,
                parameter_code,
                stage_name,
                measured_value,
                alert_message,
                ROW_NUMBER() OVER (
                    PARTITION BY station_id
                    ORDER BY CASE WHEN severity_level = 'critical' THEN 1 ELSE 0 END DESC,
                             event_timestamp DESC,
                             alert_type
                ) AS rn
            FROM dbo.fact_alert
            WHERE status = 'open'
        ),
        status_base AS (
            SELECT
                ds.station_id,
                ds.station_name,
                ds.governorate,
                ds.latitude,
                ds.longitude,
                ds.water_source_type,
                ds.plant_capacity_m3_day,
                ds.operational_status,
                lr.latest_reading_timestamp AS last_update_time,
                lw.wqi_score AS latest_wqi_score,
                lw.wqi_class AS latest_wqi_class,
                lw.contamination_risk_score AS latest_contamination_risk_score,
                lw.bacterial_regrowth_risk_score AS latest_bacterial_regrowth_risk_score,
                ISNULL(aa.active_alert_count, 0) AS active_alert_count,
                ISNULL(aa.active_water_alert_count, 0) AS active_water_alert_count,
                ISNULL(aa.active_process_alert_count, 0) AS active_process_alert_count,
                toa.parameter_code AS dominant_risk_parameter,
                toa.alert_type AS latest_open_alert_type,
                toa.severity_level AS latest_open_alert_severity,
                CASE
                    WHEN lr.latest_reading_timestamp IS NULL THEN NULL
                    ELSE DATEDIFF(MINUTE, lr.latest_reading_timestamp, SYSUTCDATETIME())
                END AS data_freshness_minutes,
                CASE
                    WHEN ds.operational_status <> 'Active' THEN 'gray'
                    WHEN lr.latest_reading_timestamp IS NULL THEN 'gray'
                    WHEN lr.latest_reading_timestamp < DATEADD(MINUTE, -20, SYSUTCDATETIME()) THEN 'gray'
                    WHEN lw.station_id IS NULL THEN 'gray'

                    -- 🔴 حالات حرجة فعلًا فقط
                    WHEN lw.wqi_class = 'Unsafe'
                         OR ISNULL(aa.critical_water_alert_count, 0) >= 2 THEN 'red'

                    -- 🟠 تحتاج انتباه حقيقي
                    WHEN lw.wqi_class = 'Watch'
                         AND ISNULL(aa.warning_water_alert_count, 0) >= 3 THEN 'orange'

                    -- 🟡 إنذار خفيف / متابعة
                    WHEN ISNULL(aa.warning_water_alert_count, 0) BETWEEN 1 AND 2 THEN 'yellow'

                    -- ✅ الطبيعي
                    ELSE 'green'
                END AS water_quality_status_color,

                CASE
                    WHEN ds.operational_status <> 'Active' THEN 'gray'
                    WHEN lr.latest_reading_timestamp IS NULL THEN 'gray'
                    WHEN lr.latest_reading_timestamp < DATEADD(MINUTE, -20, SYSUTCDATETIME()) THEN 'gray'

                    WHEN ISNULL(aa.critical_process_alert_count, 0) >= 2
                         OR ISNULL(rh.critical_health_samples_24h, 0) >= 8
                         OR ISNULL(rq.invalid_count_24h, 0) >= 40 THEN 'red'

                    WHEN ISNULL(aa.critical_process_alert_count, 0) = 1
                         OR ISNULL(aa.warning_process_alert_count, 0) >= 5
                         OR ISNULL(rh.drift_risk_count_24h, 0) >= 20
                         OR ISNULL(rq.suspicious_count_24h, 0) >= 40
                         OR ISNULL(rh.degraded_health_samples_24h, 0) >= 20 THEN 'orange'

                    WHEN ISNULL(aa.warning_process_alert_count, 0) BETWEEN 2 AND 4
                         OR ISNULL(rq.suspicious_count_24h, 0) BETWEEN 10 AND 39
                         OR ISNULL(rh.degraded_health_samples_24h, 0) BETWEEN 5 AND 19
                         OR ISNULL(rh.anomaly_events_24h, 0) >= 5 THEN 'yellow'

                    ELSE 'green'
                END AS operational_status_color,
                CASE
                    WHEN ds.operational_status <> 'Active' THEN 'Station not active / under maintenance'
                    WHEN lr.latest_reading_timestamp IS NULL THEN 'No sensor data received yet'
                    WHEN lr.latest_reading_timestamp < DATEADD(MINUTE, -20, SYSUTCDATETIME()) THEN 'No recent data in the freshness window'
                    WHEN lw.station_id IS NULL THEN 'No valid water-quality readings available for WQI'
                    WHEN ISNULL(aa.critical_water_alert_count, 0) >= 1 AND toa.alert_type IS NOT NULL THEN CONCAT('Critical water-safety alert: ', toa.alert_type)
                    WHEN lw.wqi_class = 'Unsafe' THEN 'Water Quality Index is Unsafe'
                    WHEN lw.wqi_class = 'Watch' THEN 'Water Quality Index is in Watch range'
                    WHEN ISNULL(aa.warning_water_alert_count, 0) >= 2 THEN 'Multiple water-quality warnings are open'
                    WHEN ISNULL(aa.warning_water_alert_count, 0) = 1 THEN 'One water-quality warning is open'
                    ELSE 'Water quality is stable and compliant'
                END AS water_quality_status_reason,
               CASE
                    WHEN ds.operational_status <> 'Active' THEN 'Station not active / under maintenance'
                    WHEN lr.latest_reading_timestamp IS NULL THEN 'No sensor stream available'
                    WHEN lr.latest_reading_timestamp < DATEADD(MINUTE, -20, SYSUTCDATETIME()) THEN 'Telemetry feed is stale'

                    WHEN ISNULL(aa.critical_process_alert_count, 0) >= 2 THEN 'Multiple critical operational alerts are open'
                    WHEN ISNULL(rh.critical_health_samples_24h, 0) >= 8 THEN 'Repeated critical sensor-health evidence detected'
                    WHEN ISNULL(rq.invalid_count_24h, 0) >= 40 THEN 'High invalid-reading load in the last 24 hours'

                    WHEN ISNULL(aa.critical_process_alert_count, 0) = 1 THEN 'One critical operational alert requires intervention'
                    WHEN ISNULL(aa.warning_process_alert_count, 0) >= 5 THEN 'Multiple operational warnings are open'
                    WHEN ISNULL(rh.drift_risk_count_24h, 0) >= 20 THEN 'Persistent sensor drift risk detected'
                    WHEN ISNULL(rq.suspicious_count_24h, 0) >= 40 THEN 'High suspicious-reading load in the last 24 hours'
                    WHEN ISNULL(rh.degraded_health_samples_24h, 0) >= 20 THEN 'Sensor health shows repeated degradation'

                    WHEN ISNULL(aa.warning_process_alert_count, 0) BETWEEN 2 AND 4 THEN 'Operational warnings require follow-up'
                    WHEN ISNULL(rq.suspicious_count_24h, 0) BETWEEN 10 AND 39 THEN 'Moderate suspicious-reading volume needs review'
                    WHEN ISNULL(rh.degraded_health_samples_24h, 0) BETWEEN 5 AND 19 THEN 'Sensor health shows repeated degradation'
                    WHEN ISNULL(rh.anomaly_events_24h, 0) >= 5 THEN 'Multiple operational anomalies observed in the last 24 hours'

                    ELSE 'Operations are stable'
                END AS operational_status_reason

            FROM dbo.dim_station ds
            LEFT JOIN latest_wqi lw
                ON ds.station_id = lw.station_id AND lw.rn = 1
            LEFT JOIN latest_reading lr
                ON ds.station_id = lr.station_id
            LEFT JOIN recent_quality rq
                ON ds.station_id = rq.station_id
            LEFT JOIN recent_health rh
                ON ds.station_id = rh.station_id
            LEFT JOIN active_alerts aa
                ON ds.station_id = aa.station_id
            LEFT JOIN top_open_alert toa
                ON ds.station_id = toa.station_id AND toa.rn = 1
        ),
        status_scored AS (
            SELECT
                sb.*,
                CASE
                    WHEN sb.water_quality_status_color = 'gray' OR sb.operational_status_color = 'gray' THEN 'gray'
                    WHEN sb.water_quality_status_color = 'red' OR sb.operational_status_color = 'red' THEN 'red'
                    WHEN sb.water_quality_status_color = 'orange' OR sb.operational_status_color = 'orange' THEN 'orange'
                    WHEN sb.water_quality_status_color = 'yellow' OR sb.operational_status_color = 'yellow' THEN 'yellow'
                    ELSE 'green'
                END AS overall_status_color,
                CASE
                    WHEN sb.water_quality_status_color = 'gray' OR sb.operational_status_color = 'gray' THEN
                        CASE
                            WHEN sb.operational_status <> 'Active' THEN 'Station not active / under maintenance'
                            WHEN sb.data_freshness_minutes IS NULL THEN 'No incoming telemetry yet'
                            ELSE 'Recent valid data is unavailable or stale'
                        END
                    WHEN sb.water_quality_status_color = 'red' THEN sb.water_quality_status_reason
                    WHEN sb.operational_status_color = 'red' THEN sb.operational_status_reason
                    WHEN sb.water_quality_status_color = 'orange' THEN sb.water_quality_status_reason
                    WHEN sb.operational_status_color = 'orange' THEN sb.operational_status_reason
                    WHEN sb.water_quality_status_color = 'yellow' THEN sb.water_quality_status_reason
                    WHEN sb.operational_status_color = 'yellow' THEN sb.operational_status_reason
                    ELSE 'All monitored quality and operational signals are stable'
                END AS overall_status_reason
            FROM status_base sb
        ),
        status_final AS (
            SELECT
                ss.*,
                CASE ss.overall_status_color
                    WHEN 'gray' THEN 0
                    WHEN 'green' THEN 1
                    WHEN 'yellow' THEN 2
                    WHEN 'orange' THEN 3
                    WHEN 'red' THEN 4
                    ELSE 1
                END AS status_priority
            FROM status_scored ss
        )
        INSERT INTO dbo.mart_station_latest_status (
            station_id,
            station_name,
            governorate,
            latitude,
            longitude,
            water_source_type,
            plant_capacity_m3_day,
            last_update_time,
            latest_wqi_score,
            latest_wqi_class,
            latest_contamination_risk_score,
            latest_bacterial_regrowth_risk_score,
            active_alert_count,
            active_water_alert_count,
            active_process_alert_count,
            dominant_risk_parameter,
            latest_open_alert_type,
            latest_open_alert_severity,
            water_quality_status_color,
            water_quality_status_reason,
            operational_status_color,
            operational_status_reason,
            overall_status_color,
            overall_status_reason,
            station_status_color,
            station_status_reason,
            status_priority,
            data_freshness_minutes,
            updated_at
        )
        SELECT
            station_id,
            station_name,
            governorate,
            latitude,
            longitude,
            water_source_type,
            plant_capacity_m3_day,
            ISNULL(last_update_time, SYSUTCDATETIME()),
            latest_wqi_score,
            latest_wqi_class,
            latest_contamination_risk_score,
            latest_bacterial_regrowth_risk_score,
            active_alert_count,
            active_water_alert_count,
            active_process_alert_count,
            dominant_risk_parameter,
            latest_open_alert_type,
            latest_open_alert_severity,
            water_quality_status_color,
            water_quality_status_reason,
            operational_status_color,
            operational_status_reason,
            overall_status_color,
            overall_status_reason,
            overall_status_color,
            overall_status_reason,
            status_priority,
            data_freshness_minutes,
            SYSUTCDATETIME()
        FROM status_final;
    """))

    # ----------------------------------------------------------
    # mart_governorate_daily_summary (adds current status mix + dq)
    # ----------------------------------------------------------
    conn.execute(text("""
        DELETE FROM dbo.mart_governorate_daily_summary;

        WITH alert_daily AS (
            SELECT
                CAST(fa.event_timestamp AS DATE) AS alert_date,
                ds.governorate,

                COUNT(DISTINCT CONCAT(
                    fa.station_id, '|',
                    fa.alert_type, '|',
                    ISNULL(fa.stage_name, ''), '|',
                    CAST(CAST(fa.event_timestamp AS DATE) AS VARCHAR(10))
                )) AS alert_count,

                COUNT(DISTINCT CASE
                    WHEN fa.severity_level = 'critical' THEN CONCAT(
                        fa.station_id, '|',
                        fa.alert_type, '|',
                        ISNULL(fa.stage_name, ''), '|',
                        CAST(CAST(fa.event_timestamp AS DATE) AS VARCHAR(10))
                    )
                    ELSE NULL
                END) AS critical_alert_count,

                COUNT(DISTINCT fa.station_id) AS affected_stations_count

            FROM dbo.fact_alert fa
            INNER JOIN dbo.dim_station ds
                ON fa.station_id = ds.station_id
            GROUP BY CAST(fa.event_timestamp AS DATE), ds.governorate
        ),
        current_status AS (
            SELECT
                governorate,
                SUM(CASE WHEN overall_status_color = 'green' THEN 1 ELSE 0 END) AS current_green_station_count,
                SUM(CASE WHEN overall_status_color = 'yellow' THEN 1 ELSE 0 END) AS current_yellow_station_count,
                SUM(CASE WHEN overall_status_color = 'orange' THEN 1 ELSE 0 END) AS current_orange_station_count,
                SUM(CASE WHEN overall_status_color = 'red' THEN 1 ELSE 0 END) AS current_red_station_count,
                SUM(CASE WHEN overall_status_color = 'gray' THEN 1 ELSE 0 END) AS current_gray_station_count
            FROM dbo.mart_station_latest_status
            GROUP BY governorate
        ),
        dq_daily AS (
            SELECT
                summary_date,
                governorate,
                AVG(data_quality_score) AS avg_data_quality_score
            FROM dbo.mart_data_quality_monitor
            GROUP BY summary_date, governorate
        )
        INSERT INTO dbo.mart_governorate_daily_summary (
            summary_date,
            governorate,
            stations_count,
            avg_wqi,
            min_wqi,
            max_wqi,
            avg_contamination_risk,
            avg_bacterial_risk,
            alert_count,
            critical_alert_count,
            affected_stations_count,
            avg_data_quality_score,
            current_green_station_count,
            current_yellow_station_count,
            current_orange_station_count,
            current_red_station_count,
            current_gray_station_count,
            updated_at
        )
        SELECT
            CAST(fwqi.event_timestamp AS DATE) AS summary_date,
            ds.governorate,
            COUNT(DISTINCT fwqi.station_id) AS stations_count,
            AVG(fwqi.wqi_score) AS avg_wqi,
            MIN(fwqi.wqi_score) AS min_wqi,
            MAX(fwqi.wqi_score) AS max_wqi,
            AVG(fwqi.contamination_risk_score) AS avg_contamination_risk,
            AVG(fwqi.bacterial_regrowth_risk_score) AS avg_bacterial_risk,
            ISNULL(ad.alert_count, 0) AS alert_count,
            ISNULL(ad.critical_alert_count, 0) AS critical_alert_count,
            ISNULL(ad.affected_stations_count, 0) AS affected_stations_count,
            dq.avg_data_quality_score,
            ISNULL(cs.current_green_station_count, 0) AS current_green_station_count,
            ISNULL(cs.current_yellow_station_count, 0) AS current_yellow_station_count,
            ISNULL(cs.current_orange_station_count, 0) AS current_orange_station_count,
            ISNULL(cs.current_red_station_count, 0) AS current_red_station_count,
            ISNULL(cs.current_gray_station_count, 0) AS current_gray_station_count,
            SYSUTCDATETIME()
        FROM dbo.fact_water_quality_index fwqi
        INNER JOIN dbo.dim_station ds
            ON fwqi.station_id = ds.station_id
        LEFT JOIN alert_daily ad
            ON CAST(fwqi.event_timestamp AS DATE) = ad.alert_date
           AND ds.governorate = ad.governorate
        LEFT JOIN current_status cs
            ON ds.governorate = cs.governorate
        LEFT JOIN dq_daily dq
            ON CAST(fwqi.event_timestamp AS DATE) = dq.summary_date
           AND ds.governorate = dq.governorate
        GROUP BY
            CAST(fwqi.event_timestamp AS DATE),
            ds.governorate,
            ad.alert_count,
            ad.critical_alert_count,
            ad.affected_stations_count,
            dq.avg_data_quality_score,
            cs.current_green_station_count,
            cs.current_yellow_station_count,
            cs.current_orange_station_count,
            cs.current_red_station_count,
            cs.current_gray_station_count;
    """))

    # ----------------------------------------------------------
    # mart_parameter_trend (business trend uses valid readings only)
    # ----------------------------------------------------------
    conn.execute(text("""
        DELETE FROM dbo.mart_parameter_trend;

        INSERT INTO dbo.mart_parameter_trend (
            trend_date, time_bucket_hour, governorate, parameter_name,
            avg_value, min_value, max_value, breach_count, updated_at
        )
        SELECT CAST(fsr.event_timestamp AS DATE), DATEPART(HOUR, fsr.event_timestamp), ds.governorate, 'Raw Turbidity',
               AVG(fsr.raw_turbidity_ntu), MIN(fsr.raw_turbidity_ntu), MAX(fsr.raw_turbidity_ntu),
               SUM(CASE WHEN fsr.raw_turbidity_ntu > 50 THEN 1 ELSE 0 END), SYSUTCDATETIME()
        FROM dbo.fact_sensor_reading fsr
        INNER JOIN dbo.dim_station ds ON fsr.station_id = ds.station_id
        WHERE fsr.quality_flag = 'valid'
        GROUP BY CAST(fsr.event_timestamp AS DATE), DATEPART(HOUR, fsr.event_timestamp), ds.governorate

       UNION ALL

        SELECT CAST(fsr.event_timestamp AS DATE), DATEPART(HOUR, fsr.event_timestamp), ds.governorate, 'Filtered Turbidity',
               AVG(fsr.filtered_turbidity_ntu), MIN(fsr.filtered_turbidity_ntu), MAX(fsr.filtered_turbidity_ntu),
               SUM(CASE WHEN fsr.filtered_turbidity_ntu > 0.5 THEN 1 ELSE 0 END), SYSUTCDATETIME()
        FROM dbo.fact_sensor_reading fsr
        INNER JOIN dbo.dim_station ds ON fsr.station_id = ds.station_id
        WHERE fsr.quality_flag = 'valid'
        GROUP BY CAST(fsr.event_timestamp AS DATE), DATEPART(HOUR, fsr.event_timestamp), ds.governorate


        UNION ALL

        SELECT CAST(fsr.event_timestamp AS DATE), DATEPART(HOUR, fsr.event_timestamp), ds.governorate, 'Final Turbidity',
               AVG(fsr.final_turbidity_ntu), MIN(fsr.final_turbidity_ntu), MAX(fsr.final_turbidity_ntu),
               SUM(CASE WHEN fsr.final_turbidity_ntu > 0.5 THEN 1 ELSE 0 END), SYSUTCDATETIME()
        FROM dbo.fact_sensor_reading fsr
        INNER JOIN dbo.dim_station ds ON fsr.station_id = ds.station_id
        WHERE fsr.quality_flag = 'valid'
        GROUP BY CAST(fsr.event_timestamp AS DATE), DATEPART(HOUR, fsr.event_timestamp), ds.governorate

        UNION ALL

        SELECT CAST(fsr.event_timestamp AS DATE), DATEPART(HOUR, fsr.event_timestamp), ds.governorate, 'Final Residual Chlorine',
               AVG(fsr.final_residual_chlorine_mg_l), MIN(fsr.final_residual_chlorine_mg_l), MAX(fsr.final_residual_chlorine_mg_l),
               SUM(CASE WHEN fsr.final_residual_chlorine_mg_l < 0.2 OR fsr.final_residual_chlorine_mg_l > 1.0 THEN 1 ELSE 0 END), SYSUTCDATETIME()
        FROM dbo.fact_sensor_reading fsr
        INNER JOIN dbo.dim_station ds ON fsr.station_id = ds.station_id
        WHERE fsr.quality_flag = 'valid'
        GROUP BY CAST(fsr.event_timestamp AS DATE), DATEPART(HOUR, fsr.event_timestamp), ds.governorate

        UNION ALL

        SELECT CAST(fsr.event_timestamp AS DATE), DATEPART(HOUR, fsr.event_timestamp), ds.governorate, 'Final pH',
               AVG(fsr.final_ph), MIN(fsr.final_ph), MAX(fsr.final_ph),
               SUM(CASE WHEN fsr.final_ph < 6.5 OR fsr.final_ph > 8.5 THEN 1 ELSE 0 END), SYSUTCDATETIME()
        FROM dbo.fact_sensor_reading fsr
        INNER JOIN dbo.dim_station ds ON fsr.station_id = ds.station_id
        WHERE fsr.quality_flag = 'valid'
        GROUP BY CAST(fsr.event_timestamp AS DATE), DATEPART(HOUR, fsr.event_timestamp), ds.governorate;
    """))

    # ----------------------------------------------------------
    # mart_alert_monitor (adds alert domain for easier slicing)
    # ----------------------------------------------------------
    conn.execute(text("""
        DELETE FROM dbo.mart_alert_monitor;

        INSERT INTO dbo.mart_alert_monitor (
            alert_id, station_id, station_name, governorate,
            event_timestamp, alert_type, alert_domain, parameter_name, stage_name,
            severity_level, measured_value, alert_message, status, updated_at
        )
        SELECT
            fa.alert_id,
            fa.station_id,
            ds.station_name,
            ds.governorate,
            fa.event_timestamp,
            fa.alert_type,
            CASE
                WHEN fa.alert_type IN ('HIGH_FINAL_TURBIDITY', 'LOW_FINAL_CHLORINE', 'FINAL_PH_OUT_OF_RANGE') THEN 'water_quality'
                WHEN fa.alert_type IN ('FILTER_BACKWASH_NEEDED', 'FILTER_EFFLUENT_TURBIDITY_HIGH', 'SLUDGE_BLANKET_HIGH') THEN 'process'
                WHEN fa.alert_type IN ('RAW_AMMONIA_HIGH') THEN 'source_water'
                WHEN fa.alert_type IN ('BACTERIAL_REGROWTH_RISK', 'CONTAMINATION_RISK_HIGH') THEN 'derived_risk'
                ELSE 'other'
            END AS alert_domain,
            fa.parameter_code,
            fa.stage_name,
            fa.severity_level,
            fa.measured_value,
            fa.alert_message,
            fa.status,
            SYSUTCDATETIME()
        FROM dbo.fact_alert fa
        INNER JOIN dbo.dim_station ds
            ON fa.station_id = ds.station_id;
    """))

    # ----------------------------------------------------------
    # mart_data_quality_monitor (fixes double-counting by pre-agg)
    # ----------------------------------------------------------
    conn.execute(text("""
        DELETE FROM dbo.mart_data_quality_monitor;

        WITH reading_agg AS (
            SELECT
                CAST(event_timestamp AS DATE) AS summary_date,
                station_id,
                sensor_id,
                COUNT(*) AS total_readings_count,
                SUM(CASE WHEN quality_flag = 'valid' THEN 1 ELSE 0 END) AS valid_readings_count,
                SUM(CASE WHEN quality_flag = 'invalid' THEN 1 ELSE 0 END) AS invalid_readings_count,
                SUM(CASE WHEN quality_flag = 'partial' THEN 1 ELSE 0 END) AS partial_readings_count,
                SUM(CASE WHEN quality_flag = 'suspicious' THEN 1 ELSE 0 END) AS suspicious_readings_count
            FROM dbo.fact_sensor_reading
            GROUP BY CAST(event_timestamp AS DATE), station_id, sensor_id
        ),
        health_agg AS (
            SELECT
                CAST(event_timestamp AS DATE) AS summary_date,
                station_id,
                sensor_id,
                SUM(duplicate_readings_count) AS duplicate_readings_count,
                SUM(anomaly_count) AS anomaly_count,
                SUM(CASE WHEN stale_signal_flag = 1 THEN 1 ELSE 0 END) AS stale_signal_flag_count,
                SUM(CASE WHEN drift_risk_flag = 1 THEN 1 ELSE 0 END) AS drift_risk_flag_count,
                AVG(
                    CASE
                        WHEN health_status = 'healthy' THEN 95.0
                        WHEN health_status = 'degraded' THEN 75.0
                        ELSE 45.0
                    END
                ) AS data_quality_score
            FROM dbo.fact_sensor_health
            GROUP BY CAST(event_timestamp AS DATE), station_id, sensor_id
        ),
        base_keys AS (
            SELECT summary_date, station_id, sensor_id FROM reading_agg
            UNION
            SELECT summary_date, station_id, sensor_id FROM health_agg
        )
        INSERT INTO dbo.mart_data_quality_monitor (
            summary_date, station_id, station_name, governorate, sensor_id,
            total_readings_count, valid_readings_count, invalid_readings_count, partial_readings_count,
            duplicate_readings_count, suspicious_readings_count,
            anomaly_count, stale_signal_flag_count, drift_risk_flag_count,
            data_quality_score, data_quality_band, updated_at
        )
        SELECT
            bk.summary_date,
            bk.station_id,
            ds.station_name,
            ds.governorate,
            bk.sensor_id,
            ISNULL(ra.total_readings_count, 0) AS total_readings_count,
            ISNULL(ra.valid_readings_count, 0) AS valid_readings_count,
            ISNULL(ra.invalid_readings_count, 0) AS invalid_readings_count,
            ISNULL(ra.partial_readings_count, 0) AS partial_readings_count,
            ISNULL(ha.duplicate_readings_count, 0) AS duplicate_readings_count,
            ISNULL(ra.suspicious_readings_count, 0) AS suspicious_readings_count,
            ISNULL(ha.anomaly_count, 0) AS anomaly_count,
            ISNULL(ha.stale_signal_flag_count, 0) AS stale_signal_flag_count,
            ISNULL(ha.drift_risk_flag_count, 0) AS drift_risk_flag_count,
            ISNULL(ha.data_quality_score, 95.0) AS data_quality_score,
            CASE
                WHEN ISNULL(ha.data_quality_score, 95.0) >= 90 THEN 'Trusted'
                WHEN ISNULL(ha.data_quality_score, 95.0) >= 75 THEN 'Monitor'
                ELSE 'At Risk'
            END AS data_quality_band,
            SYSUTCDATETIME()
        FROM base_keys bk
        INNER JOIN dbo.dim_station ds
            ON bk.station_id = ds.station_id
        LEFT JOIN reading_agg ra
            ON bk.summary_date = ra.summary_date
           AND bk.station_id = ra.station_id
           AND bk.sensor_id = ra.sensor_id
        LEFT JOIN health_agg ha
            ON bk.summary_date = ha.summary_date
           AND bk.station_id = ha.station_id
           AND bk.sensor_id = ha.sensor_id;
    """))

    # ----------------------------------------------------------
    # mart_station_daily_snapshot (historical storytelling)
    # ----------------------------------------------------------
    conn.execute(text("""
        DELETE FROM dbo.mart_station_daily_snapshot;

        WITH wqi_daily AS (
            SELECT
                CAST(event_timestamp AS DATE) AS summary_date,
                station_id,
                AVG(wqi_score) AS avg_wqi,
                MIN(wqi_score) AS min_wqi,
                MAX(wqi_score) AS max_wqi,
                AVG(contamination_risk_score) AS avg_contamination_risk,
                AVG(bacterial_regrowth_risk_score) AS avg_bacterial_risk
            FROM dbo.fact_water_quality_index
            GROUP BY CAST(event_timestamp AS DATE), station_id
        ),
        alert_daily AS (
            SELECT
                CAST(event_timestamp AS DATE) AS summary_date,
                station_id,
                COUNT(*) AS alert_count,
                SUM(CASE WHEN severity_level = 'critical' THEN 1 ELSE 0 END) AS critical_alert_count
            FROM dbo.fact_alert
            GROUP BY CAST(event_timestamp AS DATE), station_id
        ),
        dq_daily AS (
            SELECT
                summary_date,
                station_id,
                AVG(data_quality_score) AS avg_data_quality_score,
                SUM(suspicious_readings_count) AS suspicious_readings_count,
                SUM(invalid_readings_count) AS invalid_readings_count,
                SUM(partial_readings_count) AS partial_readings_count
            FROM dbo.mart_data_quality_monitor
            GROUP BY summary_date, station_id
        )
        INSERT INTO dbo.mart_station_daily_snapshot (
            summary_date, station_id, station_name, governorate,
            avg_wqi, min_wqi, max_wqi,
            avg_contamination_risk, avg_bacterial_risk,
            alert_count, critical_alert_count,
            suspicious_readings_count, invalid_readings_count, partial_readings_count,
            avg_data_quality_score,
            snapshot_status_color, snapshot_status_reason,
            updated_at
        )
        SELECT
            wd.summary_date,
            wd.station_id,
            ds.station_name,
            ds.governorate,
            wd.avg_wqi,
            wd.min_wqi,
            wd.max_wqi,
            wd.avg_contamination_risk,
            wd.avg_bacterial_risk,
            ISNULL(ad.alert_count, 0) AS alert_count,
            ISNULL(ad.critical_alert_count, 0) AS critical_alert_count,
            ISNULL(dq.suspicious_readings_count, 0) AS suspicious_readings_count,
            ISNULL(dq.invalid_readings_count, 0) AS invalid_readings_count,
            ISNULL(dq.partial_readings_count, 0) AS partial_readings_count,
            dq.avg_data_quality_score,
            CASE
                WHEN wd.min_wqi < 60
                     OR ISNULL(ad.critical_alert_count, 0) >= 3 THEN 'red'

                WHEN wd.min_wqi < 75
                     OR ISNULL(ad.alert_count, 0) >= 12
                     OR ISNULL(dq.suspicious_readings_count, 0) >= 10 THEN 'orange'

                WHEN wd.avg_wqi < 90
                     OR ISNULL(ad.alert_count, 0) BETWEEN 3 AND 11
                     OR ISNULL(dq.suspicious_readings_count, 0) BETWEEN 3 AND 9 THEN 'yellow'

                ELSE 'green'
            END AS snapshot_status_color,

            CASE
                WHEN wd.min_wqi < 60 THEN 'Daily minimum WQI reached Unsafe range'
                WHEN ISNULL(ad.critical_alert_count, 0) >= 3 THEN 'Multiple critical alerts occurred during the day'
                WHEN wd.min_wqi < 75 THEN 'Daily minimum WQI reached Watch range'
                WHEN ISNULL(ad.alert_count, 0) >= 12 THEN 'High alert volume was recorded during the day'
                WHEN ISNULL(dq.suspicious_readings_count, 0) >= 10 THEN 'High suspicious-reading volume during the day'
                WHEN wd.avg_wqi < 90 THEN 'Average WQI was below Excellent'
                WHEN ISNULL(ad.alert_count, 0) BETWEEN 3 AND 11 THEN 'Some alerts were recorded during the day'
                WHEN ISNULL(dq.suspicious_readings_count, 0) BETWEEN 3 AND 9 THEN 'Some suspicious readings were recorded during the day'
                ELSE 'Daily performance remained stable'
            END AS snapshot_status_reason,
            SYSUTCDATETIME()
        FROM wqi_daily wd
        INNER JOIN dbo.dim_station ds
            ON wd.station_id = ds.station_id
        LEFT JOIN alert_daily ad
            ON wd.summary_date = ad.summary_date
           AND wd.station_id = ad.station_id
        LEFT JOIN dq_daily dq
            ON wd.summary_date = dq.summary_date
           AND wd.station_id = dq.station_id;
    """))

    # ----------------------------------------------------------
    # mart_system_readiness_summary (dashboard trust layer)
    # ----------------------------------------------------------
    conn.execute(text("""
        DELETE FROM dbo.mart_system_readiness_summary;

        WITH reading_daily AS (
            SELECT
                CAST(event_timestamp AS DATE) AS summary_date,
                COUNT(*) AS total_readings,
                SUM(CASE WHEN quality_flag = 'valid' THEN 1 ELSE 0 END) AS valid_readings,
                SUM(CASE WHEN quality_flag = 'suspicious' THEN 1 ELSE 0 END) AS suspicious_readings,
                SUM(CASE WHEN quality_flag = 'partial' THEN 1 ELSE 0 END) AS partial_readings,
                SUM(CASE WHEN quality_flag = 'invalid' THEN 1 ELSE 0 END) AS invalid_readings,
                COUNT(DISTINCT station_id) AS stations_reporting_count
            FROM dbo.fact_sensor_reading
            GROUP BY CAST(event_timestamp AS DATE)
        ),
        alert_daily AS (
            SELECT
                CAST(event_timestamp AS DATE) AS summary_date,
                SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) AS resolved_alert_count,
                SUM(CASE WHEN severity_level = 'critical' THEN 1 ELSE 0 END) AS critical_alert_events
            FROM dbo.fact_alert
            GROUP BY CAST(event_timestamp AS DATE)
        ),
        wqi_daily AS (
            SELECT
                CAST(event_timestamp AS DATE) AS summary_date,
                AVG(wqi_score) AS avg_wqi,
                SUM(CASE WHEN wqi_class = 'Unsafe' THEN 1 ELSE 0 END) AS unsafe_station_events,
                SUM(CASE WHEN wqi_class = 'Watch' THEN 1 ELSE 0 END) AS watch_station_events
            FROM dbo.fact_water_quality_index
            GROUP BY CAST(event_timestamp AS DATE)
        ),
        dq_daily AS (
            SELECT
                summary_date,
                AVG(data_quality_score) AS avg_data_quality_score,
                SUM(duplicate_readings_count) AS duplicate_readings_count
            FROM dbo.mart_data_quality_monitor
            GROUP BY summary_date
        ),
        current_station_status AS (
            SELECT
                COUNT(*) AS active_station_count,
                SUM(CASE WHEN overall_status_color = 'gray' THEN 1 ELSE 0 END) AS gray_station_count,
                SUM(CASE WHEN overall_status_color = 'red' THEN 1 ELSE 0 END) AS red_station_count,
                SUM(CASE WHEN overall_status_color = 'orange' THEN 1 ELSE 0 END) AS orange_station_count,
                SUM(CASE WHEN overall_status_color = 'yellow' THEN 1 ELSE 0 END) AS yellow_station_count,
                SUM(CASE WHEN overall_status_color = 'green' THEN 1 ELSE 0 END) AS green_station_count
            FROM dbo.mart_station_latest_status
        ),
        current_alerts AS (
            SELECT COUNT(*) AS open_alert_count FROM dbo.fact_alert WHERE status = 'open'
        )
        INSERT INTO dbo.mart_system_readiness_summary (
            summary_date,
            stations_reporting_count,
            active_station_count,
            total_readings,
            valid_readings,
            suspicious_readings,
            partial_readings,
            invalid_readings,
            valid_readings_pct,
            suspicious_readings_pct,
            invalid_readings_pct,
            duplicate_readings_count,
            open_alert_count,
            resolved_alert_count,
            critical_alert_events,
            avg_data_quality_score,
            avg_wqi,
            unsafe_station_events,
            watch_station_events,
            green_station_count,
            yellow_station_count,
            orange_station_count,
            red_station_count,
            gray_station_count,
            updated_at
        )
        SELECT
            rd.summary_date,
            rd.stations_reporting_count,
            css.active_station_count,
            rd.total_readings,
            rd.valid_readings,
            rd.suspicious_readings,
            rd.partial_readings,
            rd.invalid_readings,
            CAST(100.0 * rd.valid_readings / NULLIF(rd.total_readings, 0) AS DECIMAL(10,2)) AS valid_readings_pct,
            CAST(100.0 * rd.suspicious_readings / NULLIF(rd.total_readings, 0) AS DECIMAL(10,2)) AS suspicious_readings_pct,
            CAST(100.0 * rd.invalid_readings / NULLIF(rd.total_readings, 0) AS DECIMAL(10,2)) AS invalid_readings_pct,
            ISNULL(dq.duplicate_readings_count, 0) AS duplicate_readings_count,
            ca.open_alert_count,
            ISNULL(ad.resolved_alert_count, 0) AS resolved_alert_count,
            ISNULL(ad.critical_alert_events, 0) AS critical_alert_events,
            dq.avg_data_quality_score,
            wd.avg_wqi,
            ISNULL(wd.unsafe_station_events, 0) AS unsafe_station_events,
            ISNULL(wd.watch_station_events, 0) AS watch_station_events,
            css.green_station_count,
            css.yellow_station_count,
            css.orange_station_count,
            css.red_station_count,
            css.gray_station_count,
            SYSUTCDATETIME()
        FROM reading_daily rd
        LEFT JOIN alert_daily ad
            ON rd.summary_date = ad.summary_date
        LEFT JOIN wqi_daily wd
            ON rd.summary_date = wd.summary_date
        LEFT JOIN dq_daily dq
            ON rd.summary_date = dq.summary_date
        CROSS JOIN current_station_status css
        CROSS JOIN current_alerts ca;
    """))

# ==============================================================
# 10. Build alerts from pandas batch
# ==============================================================
def build_alerts(pdf):
    alerts = []

    for _, row in pdf.iterrows():
        station_id = row["station_id"]
        sensor_id = row["sensor_id"]
        event_timestamp = row["event_timestamp"]
        event_date_key = row["event_date_key"]
        event_time_key = row["event_time_key"]

        def push_alert(alert_type, parameter_code, stage_name, severity, value, tmin, tmax, msg):
            alerts.append({
                "alert_id": f"{station_id}-{alert_type}-{row['reading_id']}",
                "station_id": station_id,
                "sensor_id": sensor_id,
                "event_timestamp": event_timestamp,
                "event_date_key": int(event_date_key),
                "event_time_key": int(event_time_key),
                "alert_type": alert_type,
                "parameter_code": parameter_code,
                "stage_name": stage_name,
                "severity_level": severity,
                "measured_value": None if pd.isna(value) else float(value),
                "threshold_min": None if tmin is None else float(tmin),
                "threshold_max": None if tmax is None else float(tmax),
                "alert_message": msg,
                "status": "open"
            })

        if pd.notna(row["final_residual_chlorine_mg_l"]) and row["final_residual_chlorine_mg_l"] < 0.20:
            push_alert(
                "LOW_FINAL_CHLORINE",
                "final_residual_chlorine_mg_l",
                "final_outlet",
                "critical" if row["final_residual_chlorine_mg_l"] < 0.10 else "warning",
                row["final_residual_chlorine_mg_l"],
                0.20,
                1.00,
                "Residual chlorine at final outlet is below safe threshold."
            )

        if pd.notna(row["final_turbidity_ntu"]) and row["final_turbidity_ntu"] > 0.50:
            push_alert(
                "HIGH_FINAL_TURBIDITY",
                "final_turbidity_ntu",
                "final_outlet",
                "critical" if row["final_turbidity_ntu"] > 1.0 else "warning",
                row["final_turbidity_ntu"],
                0.0,
                0.50,
                "Final outlet turbidity exceeds acceptable drinking-water range."
            )

        if pd.notna(row["final_ph"]) and (row["final_ph"] < 6.5 or row["final_ph"] > 8.5):
            push_alert(
                "FINAL_PH_OUT_OF_RANGE",
                "final_ph",
                "final_outlet",
                "warning",
                row["final_ph"],
                6.5,
                8.5,
                "Final outlet pH is outside the recommended operating range."
            )

        if pd.notna(row["head_loss_m"]) and row["head_loss_m"] > 2.8:
            push_alert(
                "FILTER_BACKWASH_NEEDED",
                "head_loss_m",
                "filtration_outlet",
                "critical",
                row["head_loss_m"],
                0.2,
                2.8,
                "Filter head loss indicates urgent backwash requirement."
            )

        if pd.notna(row["filtered_turbidity_ntu"]) and row["filtered_turbidity_ntu"] > 0.50:
            push_alert(
                "FILTER_EFFLUENT_TURBIDITY_HIGH",
                "filtered_turbidity_ntu",
                "filtration_outlet",
                "warning",
                row["filtered_turbidity_ntu"],
                0.0,
                0.50,
                "Filtered water turbidity is above recommended value."
            )

        if pd.notna(row["raw_ammonia_mg_l"]) and row["raw_ammonia_mg_l"] > 0.30:
            push_alert(
                "RAW_AMMONIA_HIGH",
                "raw_ammonia_mg_l",
                "raw_intake",
                "warning",
                row["raw_ammonia_mg_l"],
                0.0,
                0.30,
                "Raw water ammonia is elevated and may indicate contamination pressure."
            )

        if pd.notna(row["sludge_blanket_level_m"]) and row["sludge_blanket_level_m"] > 2.5:
            push_alert(
                "SLUDGE_BLANKET_HIGH",
                "sludge_blanket_level_m",
                "sedimentation_outlet",
                "warning",
                row["sludge_blanket_level_m"],
                0.0,
                2.5,
                "Sludge blanket level is high and may impact clarification performance."
            )

        if pd.notna(row["bacterial_regrowth_risk_score"]) and row["bacterial_regrowth_risk_score"] >= 60:
            push_alert(
                "BACTERIAL_REGROWTH_RISK",
                "bacterial_regrowth_risk_score",
                "derived",
                "critical",
                row["bacterial_regrowth_risk_score"],
                0.0,
                30.0,
                "Proxy bacterial regrowth risk is critically elevated."
            )

        if pd.notna(row["contamination_risk_score"]) and row["contamination_risk_score"] >= 60:
            push_alert(
                "CONTAMINATION_RISK_HIGH",
                "contamination_risk_score",
                "derived",
                "critical",
                row["contamination_risk_score"],
                0.0,
                30.0,
                "Composite contamination risk score is critically high."
            )

    return pd.DataFrame(alerts)

# ==============================================================
# 11. Write batch to warehouse
# ==============================================================
def write_to_warehouse(df, epoch_id):
    try:
        if df.rdd.isEmpty():
            print(f"ℹ️ Batch {epoch_id} is empty. Skipping.")
            return

        pdf = df.toPandas()

        business_pdf = pdf[
            (pdf["quality_flag"] == "valid") &
            (pdf["raw_turbidity_ntu"].notna()) &
            (pdf["raw_ph"].notna()) &
            (pdf["settled_turbidity_ntu"].notna()) &
            (pdf["filtered_turbidity_ntu"].notna()) &
            (pdf["final_residual_chlorine_mg_l"].notna()) &
            (pdf["final_ph"].notna()) &
            (pdf["final_turbidity_ntu"].notna())
        ].copy()

        if pdf.empty:
            print(f"ℹ️ Batch {epoch_id} has no rows after conversion. Skipping.")
            return

        print(f"✅ Processing batch {epoch_id} with {len(pdf)} records...")

        # ------------------------------------------------------
        # Fact 1: fact_sensor_reading
        # ------------------------------------------------------
        fact_sensor_cols = [
            "reading_id", "station_id", "sensor_id", "event_timestamp",
            "event_date_key", "event_time_key",
            "raw_turbidity_ntu", "raw_ph", "raw_conductivity_us_cm",
            "raw_temperature_c", "raw_ammonia_mg_l", "raw_alkalinity_mg_l",
            "settled_turbidity_ntu", "sludge_blanket_level_m", "estimated_settling_efficiency_pct",
            "filtered_turbidity_ntu", "head_loss_m", "filtration_rate_m_h",
            "final_residual_chlorine_mg_l", "final_ph", "final_turbidity_ntu",
            "turbidity_removal_pct", "chlorine_compliance_flag", "filter_backwash_needed_flag",
            "contamination_risk_score", "bacterial_regrowth_risk_score",
            "quality_flag", "anomaly_flag", "breach_flag", "severity_level",
            "ingestion_time", "processing_time"
        ]
        fact_sensor_pdf = pdf[fact_sensor_cols].copy()

        # ------------------------------------------------------
        # Fact 2: fact_water_quality_index
        # ------------------------------------------------------
        fact_wqi_pdf = business_pdf[[
            "station_id", "event_timestamp", "event_date_key", "event_time_key",
            "wqi_score", "wqi_class", "process_performance_score",
            "contamination_risk_score", "bacterial_regrowth_risk_score"
        ]].copy()

        fact_wqi_pdf["num_parameters_used"] = 9
        fact_wqi_pdf["num_breached_parameters"] = (
            (business_pdf["final_residual_chlorine_mg_l"] < 0.20).fillna(False).astype(int) +
            (business_pdf["final_turbidity_ntu"] > 0.30).fillna(False).astype(int) +
            ((business_pdf["final_ph"] < 6.5) | (business_pdf["final_ph"] > 8.5)).fillna(False).astype(int) +
            (business_pdf["filtered_turbidity_ntu"] > 0.30).fillna(False).astype(int) +
            (business_pdf["head_loss_m"] > 2.8).fillna(False).astype(int) +
            (business_pdf["raw_ammonia_mg_l"] > 0.30).fillna(False).astype(int)
        )

        # ------------------------------------------------------
        # Fact 3: fact_alert
        # ------------------------------------------------------
        fact_alert_pdf = build_alerts(business_pdf)
        business_station_pdf = business_pdf[["station_id"]].drop_duplicates().copy()

        # ------------------------------------------------------
        # Fact 4: fact_sensor_health
        # ------------------------------------------------------
        fact_health_pdf = pdf[[
            "station_id", "sensor_id", "event_timestamp", "event_date_key", "event_time_key", "quality_flag"
        ]].copy()

        fact_health_pdf["uptime_percentage"] = np.select(
            [
                pdf["quality_flag"] == "valid",
                pdf["quality_flag"] == "partial",
                pdf["quality_flag"] == "suspicious",
                pdf["quality_flag"] == "invalid"
            ],
            [100.0, 85.0, 70.0, 40.0],
            default=60.0
        )
        fact_health_pdf["missing_readings_count"] = np.where(pdf["quality_flag"] == "partial", 1, 0)
        fact_health_pdf["duplicate_readings_count"] = 0
        fact_health_pdf["anomaly_count"] = pdf["anomaly_flag"].astype(int)
        fact_health_pdf["stale_signal_flag"] = 0
        fact_health_pdf["drift_risk_flag"] = np.where(
            ((pdf["final_residual_chlorine_mg_l"] < 0.10) & (pdf["final_turbidity_ntu"] < 0.30)) |
            ((pdf["raw_turbidity_ntu"] > 120) & (pdf["settled_turbidity_ntu"] < 1.0)),
            1,
            0
        )

        def health_status_fn(row):
            if row["quality_flag"] == "invalid":
                return "critical"
            if row["drift_risk_flag"] == 1 or row["anomaly_count"] >= 1:
                return "degraded"
            if row["missing_readings_count"] >= 1 or row["quality_flag"] == "suspicious":
                return "degraded"
            return "healthy"

        fact_health_pdf["health_status"] = fact_health_pdf.apply(health_status_fn, axis=1)
        fact_health_pdf = fact_health_pdf.drop(columns=["quality_flag"])

        # ------------------------------------------------------
        # Write all to SQL Server
        # ------------------------------------------------------
        with engine.begin() as conn:
            fact_sensor_pdf.to_sql(
                "fact_sensor_reading",
                con=conn,
                schema="dbo",
                if_exists="append",
                index=False
            )

            if not fact_wqi_pdf.empty:
                fact_wqi_pdf.to_sql(
                    "fact_water_quality_index",
                    con=conn,
                    schema="dbo",
                    if_exists="append",
                    index=False
                )

            if not business_station_pdf.empty:
                stage_table = "stg_fact_alert_batch"
                station_stage_table = "stg_business_station_batch"

                conn.execute(text(f"""
                    IF OBJECT_ID('dbo.{stage_table}', 'U') IS NOT NULL
                        DROP TABLE dbo.{stage_table};
                """))

                conn.execute(text(f"""
                    CREATE TABLE dbo.{stage_table} (
                        station_id VARCHAR(50) NULL,
                        sensor_id VARCHAR(80) NULL,
                        event_timestamp DATETIME2 NULL,
                        event_date_key INT NULL,
                        event_time_key INT NULL,
                        alert_type NVARCHAR(100) NULL,
                        parameter_code VARCHAR(60) NULL,
                        stage_name NVARCHAR(50) NULL,
                        severity_level NVARCHAR(20) NULL,
                        measured_value FLOAT NULL,
                        threshold_min FLOAT NULL,
                        threshold_max FLOAT NULL,
                        alert_message NVARCHAR(300) NULL,
                        status NVARCHAR(30) NULL
                    );
                """))

                if not fact_alert_pdf.empty:
                    fact_alert_pdf[[
                        "station_id", "sensor_id", "event_timestamp",
                        "event_date_key", "event_time_key",
                        "alert_type", "parameter_code", "stage_name",
                        "severity_level", "measured_value",
                        "threshold_min", "threshold_max",
                        "alert_message", "status"
                    ]].to_sql(
                        stage_table,
                        con=conn,
                        schema="dbo",
                        if_exists="append",
                        index=False
                    )

                conn.execute(text(f"""
                    ;WITH latest_src AS (
                        SELECT
                            station_id,
                            sensor_id,
                            event_timestamp,
                            event_date_key,
                            event_time_key,
                            alert_type,
                            parameter_code,
                            stage_name,
                            severity_level,
                            measured_value,
                            threshold_min,
                            threshold_max,
                            alert_message,
                            status,

                            CONCAT(
                                station_id, '-',
                                alert_type, '-',
                                REPLACE(ISNULL(stage_name, 'na'), ' ', '_'), '-',
                                CONVERT(VARCHAR(8), CAST(event_timestamp AS DATE), 112), '-',
                                REPLACE(CONVERT(VARCHAR(8), CAST(event_timestamp AS TIME), 108), ':', '')
                            ) AS new_alert_id,

                            ROW_NUMBER() OVER (
                                PARTITION BY station_id, alert_type, ISNULL(stage_name, '')
                                ORDER BY event_timestamp DESC
                            ) AS rn
                        FROM dbo.{stage_table}
                    )

                    MERGE dbo.fact_alert AS tgt
                    USING (
                        SELECT *
                        FROM latest_src
                        WHERE rn = 1
                    ) AS src

                    ON tgt.alert_id = src.new_alert_id

                    WHEN MATCHED THEN
                        UPDATE SET
                            tgt.sensor_id = src.sensor_id,
                            tgt.event_timestamp = src.event_timestamp,
                            tgt.event_date_key = src.event_date_key,
                            tgt.event_time_key = src.event_time_key,
                            tgt.parameter_code = src.parameter_code,
                            tgt.severity_level = src.severity_level,
                            tgt.measured_value = src.measured_value,
                            tgt.threshold_min = src.threshold_min,
                            tgt.threshold_max = src.threshold_max,
                            tgt.alert_message = src.alert_message,
                            tgt.status = 'open'

                    WHEN NOT MATCHED THEN
                        INSERT (
                            alert_id,
                            station_id,
                            sensor_id,
                            event_timestamp,
                            event_date_key,
                            event_time_key,
                            alert_type,
                            parameter_code,
                            stage_name,
                            severity_level,
                            measured_value,
                            threshold_min,
                            threshold_max,
                            alert_message,
                            status
                        )
                        VALUES (
                            src.new_alert_id,
                            src.station_id,
                            src.sensor_id,
                            src.event_timestamp,
                            src.event_date_key,
                            src.event_time_key,
                            src.alert_type,
                            src.parameter_code,
                            src.stage_name,
                            src.severity_level,
                            src.measured_value,
                            src.threshold_min,
                            src.threshold_max,
                            src.alert_message,
                            'open'
                        );
                """))


                conn.execute(text(f"""
                    IF OBJECT_ID('dbo.{station_stage_table}', 'U') IS NOT NULL
                        DROP TABLE dbo.{station_stage_table};
                """))

                business_station_pdf.to_sql(
                    station_stage_table,
                    con=conn,
                    schema="dbo",
                    if_exists="replace",
                    index=False
                )

                conn.execute(text(f"""
                    UPDATE tgt
                    SET tgt.status = 'resolved'
                    FROM dbo.fact_alert tgt
                    WHERE tgt.status = 'open'
                      AND EXISTS (
                          SELECT 1
                          FROM dbo.{station_stage_table} bs
                          WHERE bs.station_id = tgt.station_id
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM dbo.{stage_table} src
                          WHERE src.station_id = tgt.station_id
                            AND src.alert_type = tgt.alert_type
                            AND ISNULL(src.stage_name, '') = ISNULL(tgt.stage_name, '')
                      );
                """))

                conn.execute(text(f"""DROP TABLE dbo.{station_stage_table};"""))
                conn.execute(text(f"""DROP TABLE dbo.{stage_table};"""))

            fact_health_pdf.to_sql(
                "fact_sensor_health",
                con=conn,
                schema="dbo",
                if_exists="append",
                index=False
            )

            refresh_marts_sql(conn)

        print(f"✅ Batch {epoch_id} written successfully to facts and marts refreshed.")

    except Exception as e:
        print(f"❌ Error in batch {epoch_id}: {e}")
        raise

# ==============================================================
# 12. Start streaming query
# ==============================================================
print("✅ EWIS Spark processor is now consuming Kafka stream, validating, scoring, alerting, and updating the warehouse...")

query = warehouse_clean_stream.writeStream \
    .foreachBatch(write_to_warehouse) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/opt/shared/checkpoints/ewis_enterprise_checkpoint") \
    .start()

query.awaitTermination()
