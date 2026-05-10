import os
import pandas as pd
from datetime import date
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

# ==============================================================
# 1. Config
# ==============================================================
FLAG_FILE = "/opt/airflow/dags/.reference_sent"
PARAMETERS_CSV_PATH = "/opt/shared/reference_parameters.csv"
RULES_CSV_PATH = "/opt/shared/reference_threshold_rules.csv"

def main():
    if os.path.exists(FLAG_FILE):
        print("⏭️ Reference data already generated and loaded. Skipping...")
        raise SystemExit(0)

    # ==============================================================
    # 2. Parameter Catalog
    # ==============================================================
    parameters = [
        # ---------------- RAW INTAKE ----------------
        {
            "parameter_code": "raw_turbidity_ntu",
            "parameter_name": "Raw Water Turbidity",
            "parameter_category": "physical",
            "unit": "NTU",
            "description": "Turbidity of incoming untreated water at raw water intake.",
            "stage_name": "raw_intake",
            "is_online": 1
        },
        {
            "parameter_code": "raw_ph",
            "parameter_name": "Raw Water pH",
            "parameter_category": "chemical",
            "unit": "pH",
            "description": "pH at raw water intake before coagulation and treatment.",
            "stage_name": "raw_intake",
            "is_online": 1
        },
        {
            "parameter_code": "raw_conductivity_us_cm",
            "parameter_name": "Raw Water Conductivity",
            "parameter_category": "chemical",
            "unit": "uS/cm",
            "description": "Electrical conductivity of raw water indicating dissolved salts.",
            "stage_name": "raw_intake",
            "is_online": 1
        },
        {
            "parameter_code": "raw_temperature_c",
            "parameter_name": "Raw Water Temperature",
            "parameter_category": "physical",
            "unit": "C",
            "description": "Raw water temperature affecting process kinetics and disinfection.",
            "stage_name": "raw_intake",
            "is_online": 1
        },
        {
            "parameter_code": "raw_ammonia_mg_l",
            "parameter_name": "Raw Water Ammonia",
            "parameter_category": "chemical",
            "unit": "mg/L",
            "description": "Ammonia concentration in raw water as a contamination indicator.",
            "stage_name": "raw_intake",
            "is_online": 1
        },
        {
            "parameter_code": "raw_alkalinity_mg_l",
            "parameter_name": "Raw Water Alkalinity",
            "parameter_category": "chemical",
            "unit": "mg/L as CaCO3",
            "description": "Alkalinity of raw water important for coagulation performance.",
            "stage_name": "raw_intake",
            "is_online": 1
        },

        # ---------------- SEDIMENTATION ----------------
        {
            "parameter_code": "settled_turbidity_ntu",
            "parameter_name": "Settled Water Turbidity",
            "parameter_category": "physical",
            "unit": "NTU",
            "description": "Turbidity after sedimentation and clarification.",
            "stage_name": "sedimentation_outlet",
            "is_online": 1
        },
        {
            "parameter_code": "sludge_blanket_level_m",
            "parameter_name": "Sludge Blanket Level",
            "parameter_category": "operational",
            "unit": "m",
            "description": "Estimated sludge blanket level inside sedimentation basin.",
            "stage_name": "sedimentation_outlet",
            "is_online": 1
        },
        {
            "parameter_code": "estimated_settling_efficiency_pct",
            "parameter_name": "Estimated Settling Efficiency",
            "parameter_category": "derived",
            "unit": "%",
            "description": "Estimated sedimentation removal efficiency based on inlet/outlet turbidity.",
            "stage_name": "sedimentation_outlet",
            "is_online": 1
        },

        # ---------------- FILTRATION ----------------
        {
            "parameter_code": "filtered_turbidity_ntu",
            "parameter_name": "Filtered Water Turbidity",
            "parameter_category": "physical",
            "unit": "NTU",
            "description": "Turbidity after filtration stage.",
            "stage_name": "filtration_outlet",
            "is_online": 1
        },
        {
            "parameter_code": "head_loss_m",
            "parameter_name": "Filter Head Loss",
            "parameter_category": "operational",
            "unit": "m",
            "description": "Head loss across the filter media indicating clogging load.",
            "stage_name": "filtration_outlet",
            "is_online": 1
        },
        {
            "parameter_code": "filtration_rate_m_h",
            "parameter_name": "Filtration Rate",
            "parameter_category": "operational",
            "unit": "m/h",
            "description": "Filtration rate through filter bed.",
            "stage_name": "filtration_outlet",
            "is_online": 1
        },

        # ---------------- FINAL OUTLET ----------------
        {
            "parameter_code": "final_residual_chlorine_mg_l",
            "parameter_name": "Final Residual Chlorine",
            "parameter_category": "chemical",
            "unit": "mg/L",
            "description": "Free residual chlorine at final outlet before distribution.",
            "stage_name": "final_outlet",
            "is_online": 1
        },
        {
            "parameter_code": "final_ph",
            "parameter_name": "Final Water pH",
            "parameter_category": "chemical",
            "unit": "pH",
            "description": "Final treated water pH at plant outlet.",
            "stage_name": "final_outlet",
            "is_online": 1
        },
        {
            "parameter_code": "final_turbidity_ntu",
            "parameter_name": "Final Water Turbidity",
            "parameter_category": "physical",
            "unit": "NTU",
            "description": "Final treated water turbidity at outlet.",
            "stage_name": "final_outlet",
            "is_online": 1
        },

        # ---------------- DERIVED ----------------
        {
            "parameter_code": "turbidity_removal_pct",
            "parameter_name": "Overall Turbidity Removal",
            "parameter_category": "derived",
            "unit": "%",
            "description": "Estimated overall turbidity removal from raw intake to final outlet.",
            "stage_name": "derived",
            "is_online": 1
        },
        {
            "parameter_code": "contamination_risk_score",
            "parameter_name": "Contamination Risk Score",
            "parameter_category": "derived",
            "unit": "score",
            "description": "Composite score indicating chemical/physical process risk.",
            "stage_name": "derived",
            "is_online": 1
        },
        {
            "parameter_code": "bacterial_regrowth_risk_score",
            "parameter_name": "Bacterial Regrowth Risk Score",
            "parameter_category": "derived",
            "unit": "score",
            "description": "Proxy score for microbial regrowth risk based on chlorine, turbidity, temperature, pH, and ammonia.",
            "stage_name": "derived",
            "is_online": 1
        },
        {
            "parameter_code": "process_performance_score",
            "parameter_name": "Treatment Process Performance Score",
            "parameter_category": "derived",
            "unit": "score",
            "description": "Composite score reflecting treatment stage effectiveness.",
            "stage_name": "derived",
            "is_online": 1
        }
    ]

    df_parameters = pd.DataFrame(parameters)

    # ==============================================================
    # 3. Threshold Rules
    #    Notes:
    #    - safe range = preferred operating range
    #    - warning = early warning
    #    - critical = likely unacceptable / urgent
    #    - rule_weight used later in Spark scoring logic
    # ==============================================================

    today = date.today()

    rules = [
        # ---------------- RAW INTAKE ----------------
        {
            "parameter_code": "raw_turbidity_ntu",
            "stage_name": "raw_intake",
            "water_type": "Drinking",
            "safe_min": 0.0, "safe_max": 50.0,
            "warning_min": 50.0, "warning_max": 150.0,
            "critical_min": 150.0, "critical_max": 1000.0,
            "rule_weight": 0.12,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Operational Reference Rules"
        },
        {
            "parameter_code": "raw_ph",
            "stage_name": "raw_intake",
            "water_type": "Drinking",
            "safe_min": 6.8, "safe_max": 8.2,
            "warning_min": 6.5, "warning_max": 8.5,
            "critical_min": 0.0, "critical_max": 14.0,
            "rule_weight": 0.08,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Operational Reference Rules"
        },
        {
            "parameter_code": "raw_conductivity_us_cm",
            "stage_name": "raw_intake",
            "water_type": "Drinking",
            "safe_min": 100.0, "safe_max": 1200.0,
            "warning_min": 1200.0, "warning_max": 1800.0,
            "critical_min": 1800.0, "critical_max": 6000.0,
            "rule_weight": 0.05,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Operational Reference Rules"
        },
        {
            "parameter_code": "raw_temperature_c",
            "stage_name": "raw_intake",
            "water_type": "Drinking",
            "safe_min": 10.0, "safe_max": 30.0,
            "warning_min": 30.0, "warning_max": 35.0,
            "critical_min": 35.0, "critical_max": 50.0,
            "rule_weight": 0.05,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Operational Reference Rules"
        },
        {
            "parameter_code": "raw_ammonia_mg_l",
            "stage_name": "raw_intake",
            "water_type": "Drinking",
            "safe_min": 0.0, "safe_max": 0.30,
            "warning_min": 0.30, "warning_max": 0.60,
            "critical_min": 0.60, "critical_max": 5.0,
            "rule_weight": 0.12,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Operational Reference Rules"
        },
        {
            "parameter_code": "raw_alkalinity_mg_l",
            "stage_name": "raw_intake",
            "water_type": "Drinking",
            "safe_min": 40.0, "safe_max": 180.0,
            "warning_min": 20.0, "warning_max": 220.0,
            "critical_min": 0.0, "critical_max": 500.0,
            "rule_weight": 0.06,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Operational Reference Rules"
        },

        # ---------------- SEDIMENTATION ----------------
        {
            "parameter_code": "settled_turbidity_ntu",
            "stage_name": "sedimentation_outlet",
            "water_type": "Drinking",
            "safe_min": 0.0, "safe_max": 10.0,
            "warning_min": 10.0, "warning_max": 20.0,
            "critical_min": 20.0, "critical_max": 200.0,
            "rule_weight": 0.10,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Operational Reference Rules"
        },
        {
            "parameter_code": "sludge_blanket_level_m",
            "stage_name": "sedimentation_outlet",
            "water_type": "Drinking",
            "safe_min": 0.0, "safe_max": 1.5,
            "warning_min": 1.5, "warning_max": 2.5,
            "critical_min": 2.5, "critical_max": 6.0,
            "rule_weight": 0.06,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Operational Reference Rules"
        },
        {
            "parameter_code": "estimated_settling_efficiency_pct",
            "stage_name": "sedimentation_outlet",
            "water_type": "Drinking",
            "safe_min": 70.0, "safe_max": 100.0,
            "warning_min": 50.0, "warning_max": 70.0,
            "critical_min": 0.0, "critical_max": 50.0,
            "rule_weight": 0.08,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Operational Reference Rules"
        },

        # ---------------- FILTRATION ----------------
        {
            "parameter_code": "filtered_turbidity_ntu",
            "stage_name": "filtration_outlet",
            "water_type": "Drinking",
            "safe_min": 0.0, "safe_max": 0.30,
            "warning_min": 0.30, "warning_max": 1.0,
            "critical_min": 1.0, "critical_max": 50.0,
            "rule_weight": 0.12,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Operational Reference Rules"
        },
        {
            "parameter_code": "head_loss_m",
            "stage_name": "filtration_outlet",
            "water_type": "Drinking",
            "safe_min": 0.2, "safe_max": 2.0,
            "warning_min": 2.0, "warning_max": 2.8,
            "critical_min": 2.8, "critical_max": 6.0,
            "rule_weight": 0.08,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Operational Reference Rules"
        },
        {
            "parameter_code": "filtration_rate_m_h",
            "stage_name": "filtration_outlet",
            "water_type": "Drinking",
            "safe_min": 4.0, "safe_max": 9.0,
            "warning_min": 9.0, "warning_max": 12.0,
            "critical_min": 12.0, "critical_max": 20.0,
            "rule_weight": 0.06,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Operational Reference Rules"
        },

        # ---------------- FINAL OUTLET ----------------
        {
            "parameter_code": "final_residual_chlorine_mg_l",
            "stage_name": "final_outlet",
            "water_type": "Drinking",
            "safe_min": 0.20, "safe_max": 1.00,
            "warning_min": 0.10, "warning_max": 1.50,
            "critical_min": 0.0, "critical_max": 3.0,
            "rule_weight": 0.16,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Drinking Water Safety Rules"
        },
        {
            "parameter_code": "final_ph",
            "stage_name": "final_outlet",
            "water_type": "Drinking",
            "safe_min": 6.5, "safe_max": 8.5,
            "warning_min": 6.2, "warning_max": 8.8,
            "critical_min": 0.0, "critical_max": 14.0,
            "rule_weight": 0.10,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Drinking Water Safety Rules"
        },
        {
            "parameter_code": "final_turbidity_ntu",
            "stage_name": "final_outlet",
            "water_type": "Drinking",
            "safe_min": 0.0, "safe_max": 0.30,
            "warning_min": 0.30, "warning_max": 1.0,
            "critical_min": 1.0, "critical_max": 10.0,
            "rule_weight": 0.16,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Drinking Water Safety Rules"
        },

        # ---------------- DERIVED ----------------
        {
            "parameter_code": "turbidity_removal_pct",
            "stage_name": "derived",
            "water_type": "Drinking",
            "safe_min": 95.0, "safe_max": 100.0,
            "warning_min": 85.0, "warning_max": 95.0,
            "critical_min": 0.0, "critical_max": 85.0,
            "rule_weight": 0.10,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Derived Analytics Rules"
        },
        {
            "parameter_code": "contamination_risk_score",
            "stage_name": "derived",
            "water_type": "Drinking",
            "safe_min": 0.0, "safe_max": 30.0,
            "warning_min": 30.0, "warning_max": 60.0,
            "critical_min": 60.0, "critical_max": 100.0,
            "rule_weight": 0.20,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Derived Analytics Rules"
        },
        {
            "parameter_code": "bacterial_regrowth_risk_score",
            "stage_name": "derived",
            "water_type": "Drinking",
            "safe_min": 0.0, "safe_max": 30.0,
            "warning_min": 30.0, "warning_max": 60.0,
            "critical_min": 60.0, "critical_max": 100.0,
            "rule_weight": 0.20,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Derived Analytics Rules"
        },
        {
            "parameter_code": "process_performance_score",
            "stage_name": "derived",
            "water_type": "Drinking",
            "safe_min": 80.0, "safe_max": 100.0,
            "warning_min": 60.0, "warning_max": 80.0,
            "critical_min": 0.0, "critical_max": 60.0,
            "rule_weight": 0.20,
            "effective_from": today,
            "effective_to": None,
            "rule_source": "EWIS Derived Analytics Rules"
        }
    ]

    df_rules = pd.DataFrame(rules)

    # ==============================================================
    # 4. Save CSVs
    # ==============================================================
    try:
        df_parameters.to_csv(PARAMETERS_CSV_PATH, index=False, encoding="utf-8-sig")
        print(f"✅ reference_parameters.csv saved to {PARAMETERS_CSV_PATH}")
    except Exception as e:
        print(f"❌ Failed saving parameter catalog CSV: {e}")
        raise

    try:
        df_rules.to_csv(RULES_CSV_PATH, index=False, encoding="utf-8-sig")
        print(f"✅ reference_threshold_rules.csv saved to {RULES_CSV_PATH}")
    except Exception as e:
        print(f"❌ Failed saving threshold rules CSV: {e}")
        raise

    # ==============================================================
    # 5. Load into SQL Server
    # ==============================================================
    try:
        password = quote_plus("YourStrong@Password123")
        connection_str = f"mssql+pytds://sa:{password}@ewis_sql_server:1433/EWIS_Warehouse"
        engine = create_engine(connection_str)

        with engine.begin() as conn:
            conn.execute(text("DELETE FROM dbo.dim_threshold_rule"))
            conn.execute(text("DELETE FROM dbo.dim_parameter"))

            df_parameters.to_sql(
                "dim_parameter",
                con=conn,
                schema="dbo",
                if_exists="append",
                index=False
            )

            df_rules.to_sql(
                "dim_threshold_rule",
                con=conn,
                schema="dbo",
                if_exists="append",
                index=False
            )

        print("✅ dim_parameter and dim_threshold_rule loaded successfully into SQL Server.")

    except Exception as e:
        print(f"❌ Failed loading reference data into SQL Server: {e}")
        raise

    # ==============================================================
    # 6. Create flag file
    # ==============================================================
    with open(FLAG_FILE, "w") as f:
        f.write("sent")

    print("✅ Reference data pipeline completed successfully.")

if __name__ == "__main__":
    main()