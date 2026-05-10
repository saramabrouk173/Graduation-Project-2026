IF DB_ID('EWIS_Warehouse') IS NULL
BEGIN
    CREATE DATABASE EWIS_Warehouse;
END
GO

USE EWIS_Warehouse;
GO

/* ============================================================
   Drop old objects safely
============================================================ */

IF OBJECT_ID('dbo.mart_system_readiness_summary', 'U') IS NOT NULL DROP TABLE dbo.mart_system_readiness_summary;
IF OBJECT_ID('dbo.mart_station_daily_snapshot', 'U') IS NOT NULL DROP TABLE dbo.mart_station_daily_snapshot;
IF OBJECT_ID('dbo.mart_data_quality_monitor', 'U') IS NOT NULL DROP TABLE dbo.mart_data_quality_monitor;
IF OBJECT_ID('dbo.mart_alert_monitor', 'U') IS NOT NULL DROP TABLE dbo.mart_alert_monitor;
IF OBJECT_ID('dbo.mart_parameter_trend', 'U') IS NOT NULL DROP TABLE dbo.mart_parameter_trend;
IF OBJECT_ID('dbo.mart_governorate_daily_summary', 'U') IS NOT NULL DROP TABLE dbo.mart_governorate_daily_summary;
IF OBJECT_ID('dbo.mart_station_latest_status', 'U') IS NOT NULL DROP TABLE dbo.mart_station_latest_status;
GO

IF OBJECT_ID('dbo.fact_sensor_health', 'U') IS NOT NULL DROP TABLE dbo.fact_sensor_health;
IF OBJECT_ID('dbo.fact_alert', 'U') IS NOT NULL DROP TABLE dbo.fact_alert;
IF OBJECT_ID('dbo.fact_water_quality_index', 'U') IS NOT NULL DROP TABLE dbo.fact_water_quality_index;
IF OBJECT_ID('dbo.fact_sensor_reading', 'U') IS NOT NULL DROP TABLE dbo.fact_sensor_reading;
GO

IF OBJECT_ID('dbo.ref_kpi_definition', 'U') IS NOT NULL DROP TABLE dbo.ref_kpi_definition;
IF OBJECT_ID('dbo.dim_time', 'U') IS NOT NULL DROP TABLE dbo.dim_time;
IF OBJECT_ID('dbo.dim_date', 'U') IS NOT NULL DROP TABLE dbo.dim_date;
IF OBJECT_ID('dbo.dim_threshold_rule', 'U') IS NOT NULL DROP TABLE dbo.dim_threshold_rule;
IF OBJECT_ID('dbo.dim_parameter', 'U') IS NOT NULL DROP TABLE dbo.dim_parameter;
IF OBJECT_ID('dbo.dim_sensor', 'U') IS NOT NULL DROP TABLE dbo.dim_sensor;
IF OBJECT_ID('dbo.dim_station', 'U') IS NOT NULL DROP TABLE dbo.dim_station;
GO

/* ============================================================
   1) Dimension / Reference Tables
============================================================ */

CREATE TABLE dbo.dim_station (
    station_key INT IDENTITY(1,1) PRIMARY KEY,
    station_id VARCHAR(50) NOT NULL UNIQUE,
    station_name NVARCHAR(200) NOT NULL,
    governorate NVARCHAR(100) NOT NULL,
    city NVARCHAR(100) NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    water_source_type NVARCHAR(50) NOT NULL,
    station_category NVARCHAR(50) NOT NULL DEFAULT 'Drinking',
    plant_capacity_m3_day INT NULL,
    operational_status NVARCHAR(50) NOT NULL,
    commission_date DATE NULL,
    data_source NVARCHAR(100) NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.dim_sensor (
    sensor_key INT IDENTITY(1,1) PRIMARY KEY,
    sensor_id VARCHAR(80) NOT NULL UNIQUE,
    station_id VARCHAR(50) NOT NULL,
    sensor_name NVARCHAR(150) NOT NULL,
    sensor_stage NVARCHAR(50) NOT NULL,
    sensor_type NVARCHAR(100) NOT NULL,
    manufacturer NVARCHAR(100) NULL,
    model NVARCHAR(100) NULL,
    unit_group NVARCHAR(50) NULL,
    installation_date DATE NULL,
    calibration_date DATE NULL,
    sensor_status NVARCHAR(50) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.dim_parameter (
    parameter_key INT IDENTITY(1,1) PRIMARY KEY,
    parameter_code VARCHAR(60) NOT NULL UNIQUE,
    parameter_name NVARCHAR(150) NOT NULL,
    parameter_category NVARCHAR(50) NOT NULL,
    unit NVARCHAR(50) NOT NULL,
    description NVARCHAR(300) NULL,
    stage_name NVARCHAR(50) NULL,
    is_online BIT NOT NULL DEFAULT 1,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.dim_threshold_rule (
    rule_key INT IDENTITY(1,1) PRIMARY KEY,
    parameter_code VARCHAR(60) NOT NULL,
    stage_name NVARCHAR(50) NOT NULL,
    water_type NVARCHAR(50) NOT NULL DEFAULT 'Drinking',
    safe_min FLOAT NULL,
    safe_max FLOAT NULL,
    warning_min FLOAT NULL,
    warning_max FLOAT NULL,
    critical_min FLOAT NULL,
    critical_max FLOAT NULL,
    rule_weight FLOAT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE NULL,
    rule_source NVARCHAR(200) NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.ref_kpi_definition (
    kpi_code VARCHAR(80) PRIMARY KEY,
    kpi_name NVARCHAR(150) NOT NULL,
    kpi_group NVARCHAR(80) NOT NULL,
    definition NVARCHAR(500) NOT NULL,
    source_object NVARCHAR(150) NOT NULL,
    refresh_grain NVARCHAR(50) NOT NULL,
    interpretation_hint NVARCHAR(300) NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.dim_date (
    date_key INT NOT NULL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_number INT NOT NULL,
    month_number INT NOT NULL,
    month_name NVARCHAR(20) NOT NULL,
    quarter_number INT NOT NULL,
    year_number INT NOT NULL,
    week_of_year INT NOT NULL,
    day_name NVARCHAR(20) NOT NULL,
    is_weekend BIT NOT NULL
);
GO

CREATE TABLE dbo.dim_time (
    time_key INT NOT NULL PRIMARY KEY,
    full_time TIME(0) NOT NULL UNIQUE,
    hour_number INT NOT NULL,
    minute_number INT NOT NULL,
    second_number INT NOT NULL,
    quarter_hour_bucket NVARCHAR(10) NOT NULL,
    hour_bucket NVARCHAR(20) NOT NULL
);
GO

/* ============================================================
   2) Fact Tables
============================================================ */

CREATE TABLE dbo.fact_sensor_reading (
    reading_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    reading_id VARCHAR(100) NOT NULL UNIQUE,
    station_id VARCHAR(50) NOT NULL,
    sensor_id VARCHAR(80) NOT NULL,
    event_timestamp DATETIME2 NOT NULL,
    event_date_key INT NOT NULL,
    event_time_key INT NOT NULL,
    raw_turbidity_ntu FLOAT NULL,
    raw_ph FLOAT NULL,
    raw_conductivity_us_cm FLOAT NULL,
    raw_temperature_c FLOAT NULL,
    raw_ammonia_mg_l FLOAT NULL,
    raw_alkalinity_mg_l FLOAT NULL,
    settled_turbidity_ntu FLOAT NULL,
    sludge_blanket_level_m FLOAT NULL,
    estimated_settling_efficiency_pct FLOAT NULL,
    filtered_turbidity_ntu FLOAT NULL,
    head_loss_m FLOAT NULL,
    filtration_rate_m_h FLOAT NULL,
    final_residual_chlorine_mg_l FLOAT NULL,
    final_ph FLOAT NULL,
    final_turbidity_ntu FLOAT NULL,
    turbidity_removal_pct FLOAT NULL,
    chlorine_compliance_flag BIT NULL,
    filter_backwash_needed_flag BIT NULL,
    contamination_risk_score FLOAT NULL,
    bacterial_regrowth_risk_score FLOAT NULL,
    quality_flag NVARCHAR(50) NOT NULL,
    anomaly_flag BIT NOT NULL DEFAULT 0,
    breach_flag BIT NOT NULL DEFAULT 0,
    severity_level NVARCHAR(20) NOT NULL,
    ingestion_time DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    processing_time DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.fact_water_quality_index (
    wqi_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    station_id VARCHAR(50) NOT NULL,
    event_timestamp DATETIME2 NOT NULL,
    event_date_key INT NOT NULL,
    event_time_key INT NOT NULL,
    wqi_score FLOAT NOT NULL,
    wqi_class NVARCHAR(50) NOT NULL,
    process_performance_score FLOAT NULL,
    num_parameters_used INT NOT NULL,
    num_breached_parameters INT NOT NULL,
    contamination_risk_score FLOAT NULL,
    bacterial_regrowth_risk_score FLOAT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.fact_alert (
    alert_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    alert_id VARCHAR(120) NOT NULL UNIQUE,
    station_id VARCHAR(50) NOT NULL,
    sensor_id VARCHAR(80) NOT NULL,
    event_timestamp DATETIME2 NOT NULL,
    event_date_key INT NOT NULL,
    event_time_key INT NOT NULL,
    alert_type NVARCHAR(100) NOT NULL,
    parameter_code VARCHAR(60) NULL,
    stage_name NVARCHAR(50) NULL,
    severity_level NVARCHAR(20) NOT NULL,
    measured_value FLOAT NULL,
    threshold_min FLOAT NULL,
    threshold_max FLOAT NULL,
    alert_message NVARCHAR(300) NOT NULL,
    status NVARCHAR(30) NOT NULL DEFAULT 'open',
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.fact_sensor_health (
    health_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    station_id VARCHAR(50) NOT NULL,
    sensor_id VARCHAR(80) NOT NULL,
    event_timestamp DATETIME2 NOT NULL,
    event_date_key INT NOT NULL,
    event_time_key INT NOT NULL,
    uptime_percentage FLOAT NULL,
    missing_readings_count INT NOT NULL DEFAULT 0,
    duplicate_readings_count INT NOT NULL DEFAULT 0,
    anomaly_count INT NOT NULL DEFAULT 0,
    stale_signal_flag BIT NOT NULL DEFAULT 0,
    drift_risk_flag BIT NOT NULL DEFAULT 0,
    health_status NVARCHAR(30) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

/* ============================================================
   3) Dashboard / Semantic Marts
============================================================ */

CREATE TABLE dbo.mart_station_latest_status (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name NVARCHAR(200) NOT NULL,
    governorate NVARCHAR(100) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    water_source_type NVARCHAR(50) NOT NULL,
    plant_capacity_m3_day INT NULL,
    last_update_time DATETIME2 NOT NULL,
    latest_wqi_score FLOAT NULL,
    latest_wqi_class NVARCHAR(50) NULL,
    latest_contamination_risk_score FLOAT NULL,
    latest_bacterial_regrowth_risk_score FLOAT NULL,
    active_alert_count INT NOT NULL DEFAULT 0,
    active_water_alert_count INT NOT NULL DEFAULT 0,
    active_process_alert_count INT NOT NULL DEFAULT 0,
    dominant_risk_parameter NVARCHAR(100) NULL,
    latest_open_alert_type NVARCHAR(100) NULL,
    latest_open_alert_severity NVARCHAR(20) NULL,
    water_quality_status_color NVARCHAR(20) NULL,
    water_quality_status_reason NVARCHAR(300) NULL,
    operational_status_color NVARCHAR(20) NULL,
    operational_status_reason NVARCHAR(300) NULL,
    overall_status_color NVARCHAR(20) NULL,
    overall_status_reason NVARCHAR(300) NULL,
    station_status_color NVARCHAR(20) NULL,
    station_status_reason NVARCHAR(300) NULL,
    status_priority INT NULL,
    data_freshness_minutes INT NULL,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.mart_governorate_daily_summary (
    summary_date DATE NOT NULL,
    governorate NVARCHAR(100) NOT NULL,
    stations_count INT NOT NULL,
    avg_wqi FLOAT NULL,
    min_wqi FLOAT NULL,
    max_wqi FLOAT NULL,
    avg_contamination_risk FLOAT NULL,
    avg_bacterial_risk FLOAT NULL,
    alert_count INT NOT NULL DEFAULT 0,
    critical_alert_count INT NOT NULL DEFAULT 0,
    affected_stations_count INT NOT NULL DEFAULT 0,
    avg_data_quality_score FLOAT NULL,
    current_green_station_count INT NOT NULL DEFAULT 0,
    current_yellow_station_count INT NOT NULL DEFAULT 0,
    current_orange_station_count INT NOT NULL DEFAULT 0,
    current_red_station_count INT NOT NULL DEFAULT 0,
    current_gray_station_count INT NOT NULL DEFAULT 0,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_mart_governorate_daily_summary PRIMARY KEY (summary_date, governorate)
);
GO

CREATE TABLE dbo.mart_parameter_trend (
    trend_date DATE NOT NULL,
    time_bucket_hour INT NOT NULL,
    governorate NVARCHAR(100) NOT NULL,
    parameter_name NVARCHAR(100) NOT NULL,
    avg_value FLOAT NULL,
    min_value FLOAT NULL,
    max_value FLOAT NULL,
    breach_count INT NOT NULL DEFAULT 0,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_mart_parameter_trend PRIMARY KEY (trend_date, time_bucket_hour, governorate, parameter_name)
);
GO

CREATE TABLE dbo.mart_alert_monitor (
    alert_id VARCHAR(120) PRIMARY KEY,
    station_id VARCHAR(50) NOT NULL,
    station_name NVARCHAR(200) NOT NULL,
    governorate NVARCHAR(100) NOT NULL,
    event_timestamp DATETIME2 NOT NULL,
    alert_type NVARCHAR(100) NOT NULL,
    alert_domain NVARCHAR(50) NULL,
    parameter_name NVARCHAR(100) NULL,
    stage_name NVARCHAR(50) NULL,
    severity_level NVARCHAR(20) NOT NULL,
    measured_value FLOAT NULL,
    alert_message NVARCHAR(300) NOT NULL,
    status NVARCHAR(30) NOT NULL,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.mart_data_quality_monitor (
    summary_date DATE NOT NULL,
    station_id VARCHAR(50) NOT NULL,
    station_name NVARCHAR(200) NOT NULL,
    governorate NVARCHAR(100) NOT NULL,
    sensor_id VARCHAR(80) NOT NULL,
    total_readings_count INT NOT NULL DEFAULT 0,
    valid_readings_count INT NOT NULL DEFAULT 0,
    invalid_readings_count INT NOT NULL DEFAULT 0,
    partial_readings_count INT NOT NULL DEFAULT 0,
    duplicate_readings_count INT NOT NULL DEFAULT 0,
    suspicious_readings_count INT NOT NULL DEFAULT 0,
    anomaly_count INT NOT NULL DEFAULT 0,
    stale_signal_flag_count INT NOT NULL DEFAULT 0,
    drift_risk_flag_count INT NOT NULL DEFAULT 0,
    data_quality_score FLOAT NULL,
    data_quality_band NVARCHAR(30) NULL,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_mart_data_quality_monitor PRIMARY KEY (summary_date, station_id, sensor_id)
);
GO

CREATE TABLE dbo.mart_station_daily_snapshot (
    summary_date DATE NOT NULL,
    station_id VARCHAR(50) NOT NULL,
    station_name NVARCHAR(200) NOT NULL,
    governorate NVARCHAR(100) NOT NULL,
    avg_wqi FLOAT NULL,
    min_wqi FLOAT NULL,
    max_wqi FLOAT NULL,
    avg_contamination_risk FLOAT NULL,
    avg_bacterial_risk FLOAT NULL,
    alert_count INT NOT NULL DEFAULT 0,
    critical_alert_count INT NOT NULL DEFAULT 0,
    suspicious_readings_count INT NOT NULL DEFAULT 0,
    invalid_readings_count INT NOT NULL DEFAULT 0,
    partial_readings_count INT NOT NULL DEFAULT 0,
    avg_data_quality_score FLOAT NULL,
    snapshot_status_color NVARCHAR(20) NULL,
    snapshot_status_reason NVARCHAR(300) NULL,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_mart_station_daily_snapshot PRIMARY KEY (summary_date, station_id)
);
GO

CREATE TABLE dbo.mart_system_readiness_summary (
    summary_date DATE NOT NULL PRIMARY KEY,
    stations_reporting_count INT NOT NULL DEFAULT 0,
    active_station_count INT NOT NULL DEFAULT 0,
    total_readings INT NOT NULL DEFAULT 0,
    valid_readings INT NOT NULL DEFAULT 0,
    suspicious_readings INT NOT NULL DEFAULT 0,
    partial_readings INT NOT NULL DEFAULT 0,
    invalid_readings INT NOT NULL DEFAULT 0,
    valid_readings_pct DECIMAL(10,2) NULL,
    suspicious_readings_pct DECIMAL(10,2) NULL,
    invalid_readings_pct DECIMAL(10,2) NULL,
    duplicate_readings_count INT NOT NULL DEFAULT 0,
    open_alert_count INT NOT NULL DEFAULT 0,
    resolved_alert_count INT NOT NULL DEFAULT 0,
    critical_alert_events INT NOT NULL DEFAULT 0,
    avg_data_quality_score FLOAT NULL,
    avg_wqi FLOAT NULL,
    unsafe_station_events INT NOT NULL DEFAULT 0,
    watch_station_events INT NOT NULL DEFAULT 0,
    green_station_count INT NOT NULL DEFAULT 0,
    yellow_station_count INT NOT NULL DEFAULT 0,
    orange_station_count INT NOT NULL DEFAULT 0,
    red_station_count INT NOT NULL DEFAULT 0,
    gray_station_count INT NOT NULL DEFAULT 0,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

/* ============================================================
   4) Helpful Indexes
============================================================ */

CREATE INDEX IX_dim_station_governorate ON dbo.dim_station(governorate);
CREATE INDEX IX_dim_sensor_station_id ON dbo.dim_sensor(station_id);
CREATE INDEX IX_dim_parameter_stage_name ON dbo.dim_parameter(stage_name);
CREATE INDEX IX_dim_threshold_rule_parameter_stage ON dbo.dim_threshold_rule(parameter_code, stage_name);

CREATE INDEX IX_fact_sensor_reading_station_time ON dbo.fact_sensor_reading(station_id, event_timestamp DESC);
CREATE INDEX IX_fact_sensor_reading_sensor_time ON dbo.fact_sensor_reading(sensor_id, event_timestamp DESC);
CREATE INDEX IX_fact_sensor_reading_date_time ON dbo.fact_sensor_reading(event_date_key, event_time_key);
CREATE INDEX IX_fact_sensor_reading_quality ON dbo.fact_sensor_reading(quality_flag, severity_level);

CREATE INDEX IX_fact_wqi_station_time ON dbo.fact_water_quality_index(station_id, event_timestamp DESC);
CREATE INDEX IX_fact_wqi_date ON dbo.fact_water_quality_index(event_date_key);

CREATE INDEX IX_fact_alert_station_time ON dbo.fact_alert(station_id, event_timestamp DESC);
CREATE INDEX IX_fact_alert_status_severity ON dbo.fact_alert(status, severity_level);
CREATE INDEX IX_fact_alert_type ON dbo.fact_alert(alert_type);

CREATE INDEX IX_fact_sensor_health_station_sensor_time ON dbo.fact_sensor_health(station_id, sensor_id, event_timestamp DESC);

CREATE INDEX IX_mart_station_latest_status_governorate ON dbo.mart_station_latest_status(governorate);
CREATE INDEX IX_mart_station_latest_status_overall_color ON dbo.mart_station_latest_status(overall_status_color, status_priority);
CREATE INDEX IX_mart_governorate_daily_summary_governorate ON dbo.mart_governorate_daily_summary(governorate);
CREATE INDEX IX_mart_parameter_trend_parameter ON dbo.mart_parameter_trend(parameter_name);
CREATE INDEX IX_mart_alert_monitor_governorate_status ON dbo.mart_alert_monitor(governorate, status);
CREATE INDEX IX_mart_station_daily_snapshot_governorate_date ON dbo.mart_station_daily_snapshot(governorate, summary_date DESC);
GO

/* ============================================================
   5) Seed dim_date (2025-2027)
============================================================ */

DECLARE @StartDate DATE = '2025-01-01';
DECLARE @EndDate   DATE = '2027-12-31';

;WITH DateSeries AS (
    SELECT @StartDate AS full_date
    UNION ALL
    SELECT DATEADD(DAY, 1, full_date)
    FROM DateSeries
    WHERE full_date < @EndDate
)
INSERT INTO dbo.dim_date (
    date_key,
    full_date,
    day_number,
    month_number,
    month_name,
    quarter_number,
    year_number,
    week_of_year,
    day_name,
    is_weekend
)
SELECT
    CAST(CONVERT(VARCHAR(8), full_date, 112) AS INT) AS date_key,
    full_date,
    DAY(full_date),
    MONTH(full_date),
    DATENAME(MONTH, full_date),
    DATEPART(QUARTER, full_date),
    YEAR(full_date),
    DATEPART(WEEK, full_date),
    DATENAME(WEEKDAY, full_date),
    CASE WHEN DATENAME(WEEKDAY, full_date) IN ('Friday', 'Saturday') THEN 1 ELSE 0 END
FROM DateSeries
OPTION (MAXRECURSION 2000);
GO

/* ============================================================
   6) Seed dim_time (00:00:00 to 23:59:59)
============================================================ */

;WITH TimeSeries AS (
    SELECT CAST('00:00:00' AS TIME(0)) AS full_time
    UNION ALL
    SELECT CAST(DATEADD(SECOND, 1, CAST(full_time AS DATETIME)) AS TIME(0))
    FROM TimeSeries
    WHERE full_time < '23:59:59'
)
INSERT INTO dbo.dim_time (
    time_key,
    full_time,
    hour_number,
    minute_number,
    second_number,
    quarter_hour_bucket,
    hour_bucket
)
SELECT
    (DATEPART(HOUR, full_time) * 10000) + (DATEPART(MINUTE, full_time) * 100) + DATEPART(SECOND, full_time) AS time_key,
    full_time,
    DATEPART(HOUR, full_time),
    DATEPART(MINUTE, full_time),
    DATEPART(SECOND, full_time),
    CASE
        WHEN DATEPART(MINUTE, full_time) < 15 THEN '00'
        WHEN DATEPART(MINUTE, full_time) < 30 THEN '15'
        WHEN DATEPART(MINUTE, full_time) < 45 THEN '30'
        ELSE '45'
    END,
    RIGHT('0' + CAST(DATEPART(HOUR, full_time) AS VARCHAR(2)), 2) + CHAR(58) + '00'
FROM TimeSeries
OPTION (MAXRECURSION 0);
GO

/* ============================================================
   7) KPI dictionary seed (dashboard definitions)
============================================================ */

INSERT INTO dbo.ref_kpi_definition (
    kpi_code, kpi_name, kpi_group, definition, source_object, refresh_grain, interpretation_hint
)
VALUES
('AVG_WQI', 'Average WQI', 'Water Quality', 'Average Water Quality Index computed from valid treated-water readings only.', 'dbo.fact_water_quality_index', 'event / daily', 'Higher is better; 90+ is Excellent.'),
('OPEN_ALERTS', 'Open Alerts', 'Operational Risk', 'Current unresolved alert incidents only. Repeated alert occurrences are merged into a single open incident per station, type and stage.', 'dbo.fact_alert', 'current', 'Represents active operational or water-quality issues requiring attention.'),
('CRITICAL_STATIONS', 'Critical Stations', 'Operational Risk', 'Stations whose overall status is red because of Unsafe WQI, critical water-safety alert, or severe operational state.', 'dbo.mart_station_latest_status', 'current', 'Represents stations that need urgent action.'),
('VALID_READING_PCT', 'Valid Reading %', 'Data Trust', 'Percentage of fact_sensor_reading rows classified as valid over total readings.', 'dbo.mart_system_readiness_summary', 'daily', 'Higher is better; low values indicate poor telemetry trust.'),
('DATA_QUALITY_SCORE', 'Data Quality Score', 'Data Trust', 'Average score derived from sensor health state, anomalies, drift risk, and stale/duplicate indicators.', 'dbo.mart_data_quality_monitor', 'daily / sensor', '90+ Trusted, 75-89 Monitor, <75 At Risk.'),
('OVERALL_STATUS', 'Overall Station Status', 'Decision Support', 'Worst-case explainable status derived from separate water-quality and operational status layers.', 'dbo.mart_station_latest_status', 'current', 'Used for map colors and triage queues.'),
('WATER_STATUS', 'Water Quality Status', 'Decision Support', 'Status derived from WQI and water-safety alerts affecting the final water delivered.', 'dbo.mart_station_latest_status', 'current', 'Separates outcome quality from process condition.'),
('OPS_STATUS', 'Operational Status', 'Decision Support', 'Status derived from process alerts, telemetry freshness, and sensor-health evidence.', 'dbo.mart_station_latest_status', 'current', 'Highlights operational risk before water quality degrades.');
GO

/* ============================================================
   8) Helpful View for latest station readings
============================================================ */

IF OBJECT_ID('dbo.vw_latest_station_readings', 'V') IS NOT NULL
    DROP VIEW dbo.vw_latest_station_readings;
GO

CREATE VIEW dbo.vw_latest_station_readings AS
SELECT
    fsr.station_id,
    fsr.sensor_id,
    fsr.event_timestamp,
    fsr.raw_turbidity_ntu,
    fsr.raw_ph,
    fsr.raw_conductivity_us_cm,
    fsr.raw_temperature_c,
    fsr.raw_ammonia_mg_l,
    fsr.raw_alkalinity_mg_l,
    fsr.settled_turbidity_ntu,
    fsr.sludge_blanket_level_m,
    fsr.filtered_turbidity_ntu,
    fsr.head_loss_m,
    fsr.filtration_rate_m_h,
    fsr.final_residual_chlorine_mg_l,
    fsr.final_ph,
    fsr.final_turbidity_ntu,
    fsr.turbidity_removal_pct,
    fsr.contamination_risk_score,
    fsr.bacterial_regrowth_risk_score,
    fsr.quality_flag,
    fsr.severity_level
FROM dbo.fact_sensor_reading fsr;
GO

PRINT '✅ EWIS_Warehouse world-class schema created successfully.';
GO
