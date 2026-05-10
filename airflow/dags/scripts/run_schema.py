import os
import re
from urllib.parse import quote_plus
from sqlalchemy import create_engine

# ==============================================================
# 1. Config
# ==============================================================
SCHEMA_FILE = "/opt/airflow/dags/scripts/EWIS_Database_Schema.sql"

def main():
    if not os.path.exists(SCHEMA_FILE):
        raise FileNotFoundError(f"❌ Schema file not found: {SCHEMA_FILE}")

    print(f"📄 Loading schema file from: {SCHEMA_FILE}")

    # ==============================================================
    # 2. Read SQL script
    # ==============================================================
    with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
        sql_script = f.read()

    # Split on lines containing only GO
    batches = re.split(r'^\s*GO\s*$', sql_script, flags=re.MULTILINE | re.IGNORECASE)
    batches = [batch.strip() for batch in batches if batch.strip()]

    print(f"📦 Found {len(batches)} SQL batches to execute.")

    # ==============================================================
    # 3. Connect to SQL Server
    # ==============================================================
    password = quote_plus("YourStrong@Password123")
    connection_str = f"mssql+pytds://sa:{password}@ewis_sql_server:1433/master"
    engine = create_engine(connection_str)

    # ==============================================================
    # 4. Execute SQL batches
    # ==============================================================
    try:
        with engine.begin() as conn:
            for i, batch in enumerate(batches, start=1):
                print(f"▶️ Executing batch {i}/{len(batches)}...")

                # Important:
                # pytds interprets % as formatting placeholders,
                # so we escape them before execution.
                safe_batch = batch.replace("%", "%%")

                conn.exec_driver_sql(safe_batch)

        print("✅ Database schema executed successfully.")

    except Exception as e:
        print(f"❌ Failed to execute database schema: {e}")
        raise

if __name__ == "__main__":
    main()