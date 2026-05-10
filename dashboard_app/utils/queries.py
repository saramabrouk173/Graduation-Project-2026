NATIONAL_KPI_QUERY = """
SELECT TOP 1 *
FROM dbo.mart_system_readiness_summary
ORDER BY summary_date DESC;
"""

STATION_STATUS_QUERY = """
SELECT *
FROM dbo.mart_station_latest_status;
"""

STATUS_DISTRIBUTION_QUERY = """
SELECT
    overall_status_color,
    COUNT(*) AS station_count
FROM dbo.mart_station_latest_status
GROUP BY overall_status_color
ORDER BY station_count DESC;
"""

GOVERNORATE_SUMMARY_QUERY = """
WITH latest_day AS (
    SELECT MAX(summary_date) AS latest_summary_date
    FROM dbo.mart_governorate_daily_summary
)
SELECT g.*
FROM dbo.mart_governorate_daily_summary g
INNER JOIN latest_day d
    ON g.summary_date = d.latest_summary_date;
"""

PARAMETER_TREND_QUERY = """
SELECT *
FROM dbo.mart_parameter_trend;
"""

ALERT_MONITOR_QUERY = """
SELECT *
FROM dbo.mart_alert_monitor;
"""

OPEN_ALERTS_QUERY = """
SELECT *
FROM dbo.mart_alert_monitor
WHERE status = 'open';
"""

DATA_QUALITY_QUERY = """
SELECT *
FROM dbo.mart_data_quality_monitor;
"""

STATION_DAILY_QUERY = """
SELECT *
FROM dbo.mart_station_daily_snapshot;
"""