from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Sara",
    "start_date": datetime(2026, 5, 4),
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    dag_id="EWIS_Enterprise_Final",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    description="EWIS enterprise water intelligence pipeline for drinking water treatment monitoring"
) as dag:

    setup_kafka = BashOperator(
        task_id="setup_kafka",
        bash_command="python /opt/airflow/dags/scripts/kafka_setup.py"
    )

    run_database_schema = BashOperator(
        task_id="run_database_schema",
        bash_command="python /opt/airflow/dags/scripts/run_schema.py"
    )

    setup_reference = BashOperator(
        task_id="setup_reference",
        bash_command="python /opt/airflow/dags/scripts/reference_data.py"
    )

    setup_metadata = BashOperator(
        task_id="setup_metadata",
        bash_command="python /opt/airflow/dags/scripts/meta_data.py"
    )

    spark_processing = BashOperator(
        task_id="spark_processing",
        bash_command="""
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
/opt/shared/spark_processor.py
"""
    )

    stream_sensors = BashOperator(
        task_id="stream_sensors",
        bash_command="python /opt/airflow/dags/scripts/sensor_data_generator.py"
    )

    setup_kafka >> run_database_schema >> setup_reference >> setup_metadata
    setup_metadata >> spark_processing
    setup_metadata >> stream_sensors  