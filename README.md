

\# EWIS — Enterprise Water Intelligence System



EWIS is an end-to-end data engineering and business intelligence project for monitoring drinking-water treatment stations.



The system simulates sensor telemetry, streams operational readings, processes and validates water-quality signals, builds SQL Server analytical marts, and exposes an interactive Streamlit dashboard for executive and operational monitoring.



\---



\## Overview



Water-treatment stations generate continuous telemetry from sensors and operational systems. EWIS transforms this telemetry into reliable business intelligence by providing:



\- Station-level operational monitoring

\- Water Quality Index tracking

\- Alert detection and monitoring

\- Data quality and reliability scoring

\- Governorate-level performance comparison

\- Executive-ready analytical dashboards

\- BI-ready SQL Server marts



The project demonstrates a full data pipeline from simulated sensor events to dashboard-ready marts.



\---



\## Architecture



```text

Sensor Data Generator

&#x20;       ↓

Kafka Topics

&#x20;       ↓

Spark Processing

&#x20;       ↓

SQL Server Data Warehouse

&#x20;       ↓

Analytical BI Marts

&#x20;       ↓

Streamlit Dashboard

````



\*\*\*



\## Technology Stack



| Layer            | Tools                |

| ---------------- | -------------------- |

| Orchestration    | Apache Airflow       |

| Streaming        | Apache Kafka         |

| Processing       | Apache Spark         |

| Database         | Microsoft SQL Server |

| Dashboard        | Streamlit            |

| Visualization    | Plotly, PyDeck       |

| Language         | Python               |

| Containerization | Docker Compose       |



\*\*\*



\## Project Structure



```text

EWIS/

│

├── docker-compose.yml

│

├── airflow/

│   ├── Dockerfile

│   ├── dags/

│   │   ├── ewis\_main\_dag.py

│   │   └── scripts/

│   │       ├── EWIS\_Database\_Schema.sql

│   │       ├── kafka\_setup.py

│   │       ├── meta\_data.py

│   │       ├── reference\_data.py

│   │       ├── run\_schema.py

│   │       └── sensor\_data\_generator.py

│   │

│   └── logs/                  # Runtime logs, ignored in Git

│

├── dashboard\_app/

│   ├── Home.py

│   ├── requirements.txt

│   ├── .streamlit/

│   │   └── config.toml

│   │

│   ├── assets/

│   │   ├── logo.png

│   │   └── styles.css

│   │

│   ├── pages/

│   │   ├── 1\_Executive\_Overview.py

│   │   ├── 2\_Governorate\_Overview.py

│   │   ├── 3\_Station\_Monitoring.py

│   │   ├── 4\_Water\_Quality\_Trends.py

│   │   └── 5\_Data\_Trust.py

│   │

│   └── utils/

│       ├── db.py

│       ├── formatters.py

│       ├── queries.py

│       └── ui.py

│

├── shared/

│   ├── reference\_parameters.csv

│   ├── reference\_threshold\_rules.csv

│   ├── sensors\_metadata.csv

│   ├── spark\_processor.py

│   ├── stations\_metadata.csv

│   └── checkpoints/           # Runtime Spark checkpoints, ignored in Git

│

└── work/

```



\*\*\*



\## Airflow Pipeline



The main DAG file is:



```text

airflow/dags/ewis\_main\_dag.py

```



The Airflow pipeline includes tasks such as:



\*   Running the SQL Server database schema

\*   Setting up Kafka topics

\*   Loading metadata

\*   Loading reference data

\*   Streaming simulated sensor readings

\*   Running Spark processing

\*   Building analytical marts



The active DAG name used in the project is:



```text

EWIS\_Enterprise\_Final

```



\*\*\*



\## Data Generation and Streaming



The project includes a sensor data generator:



```text

airflow/dags/scripts/sensor\_data\_generator.py

```



This script simulates water-treatment telemetry and sends generated readings into the streaming layer.



Kafka setup is handled by:



```text

airflow/dags/scripts/kafka\_setup.py

```



\*\*\*



\## Database Schema



The SQL Server schema is defined in:



```text

airflow/dags/scripts/EWIS\_Database\_Schema.sql

```



The schema includes operational tables, reference tables, and BI marts.



Schema execution is handled by:



```text

airflow/dags/scripts/run\_schema.py

```



\*\*\*



\## Reference and Metadata Files



Reference and metadata setup is handled using:



```text

airflow/dags/scripts/meta\_data.py

airflow/dags/scripts/reference\_data.py

```



The shared reference files include:



```text

shared/reference\_parameters.csv

shared/reference\_threshold\_rules.csv

shared/sensors\_metadata.csv

shared/stations\_metadata.csv

```



\*\*\*



\## Analytical Data Marts



The dashboard consumes the following SQL Server marts:



| Mart                             | Purpose                                                                           |

| -------------------------------- | --------------------------------------------------------------------------------- |

| `mart\_station\_latest\_status`     | Latest station operational and water-quality status                               |

| `mart\_system\_readiness\_summary`  | National platform readiness and data validity summary                             |

| `mart\_governorate\_daily\_summary` | Governorate-level WQI, station mix, and alert summaries                           |

| `mart\_parameter\_trend`           | Time-bucketed parameter trends and breach counts                                  |

| `mart\_station\_daily\_snapshot`    | Daily station WQI and historical station status                                   |

| `mart\_alert\_monitor`             | Open and historical alert monitoring                                              |

| `mart\_data\_quality\_monitor`      | Sensor reliability, invalid readings, suspicious readings, and drift-risk metrics |



\*\*\*



\## KPI Reference Table



The project also includes a KPI reference table:



```text

ref\_kpi\_definition

```



This table documents KPI metadata such as:



\*   KPI code

\*   KPI name

\*   KPI group

\*   Business definition

\*   Source object

\*   Refresh grain

\*   Interpretation hint



This table supports KPI governance and explainability. It is used as a semantic reference layer, but it is not exposed as a dedicated dashboard page in the current Streamlit dashboard.



\*\*\*



\## Streamlit Dashboard



The Streamlit dashboard is located in:



```text

dashboard\_app/

```



The main dashboard entry point is:



```text

dashboard\_app/Home.py

```



\*\*\*



\## Dashboard Pages



The dashboard includes the following pages:



\### 1. Home



The landing page provides a high-level executive summary:



\*   Platform readiness score

\*   System operational status

\*   Stable station coverage

\*   Affected stations

\*   Data validity

\*   BI mart readiness

\*   System overview

\*   Recommended navigation



\*\*\*



\### 2. Executive Overview



National-level monitoring page showing:



\*   Station status distribution

\*   Open alert types

\*   Live station map

\*   Priority stations table

\*   National KPIs



Main marts used:



\*   `mart\_system\_readiness\_summary`

\*   `mart\_station\_latest\_status`

\*   `mart\_alert\_monitor`



\*\*\*



\### 3. Governorate Performance



Regional performance page showing:



\*   Average WQI by governorate

\*   Current station status mix

\*   Governorate ranking

\*   Regional alert load

\*   Critical alerts



Main mart used:



\*   `mart\_governorate\_daily\_summary`



\*\*\*



\### 4. Station Monitoring



Station drill-down page showing:



\*   Selected station details

\*   Latest WQI

\*   Active alerts

\*   Water-quality alerts

\*   Process alerts

\*   Data freshness

\*   Open alerts for station

\*   Data trust metrics

\*   Station daily WQI history



Main marts used:



\*   `mart\_station\_latest\_status`

\*   `mart\_alert\_monitor`

\*   `mart\_station\_daily\_snapshot`

\*   `mart\_data\_quality\_monitor`



\*\*\*



\### 5. Water Quality Trends



Water-quality analytics page showing:



\*   Parameter trend by governorate

\*   Breach load by governorate

\*   Breach heatmap across parameters

\*   Station daily WQI distribution

\*   Parameter trend records



Main marts used:



\*   `mart\_parameter\_trend`

\*   `mart\_station\_daily\_snapshot`



\*\*\*



\### 6. Data Trust



Data reliability page showing:



\*   Average data quality score

\*   Valid readings

\*   Suspicious readings

\*   Invalid readings

\*   Duplicate readings

\*   Drift-risk flags

\*   Data quality band distribution

\*   Lowest-quality sensors

\*   Data quality by governorate

\*   Data quality details table



Main marts used:



\*   `mart\_data\_quality\_monitor`

\*   `mart\_system\_readiness\_summary`



\*\*\*

## Dashboard Screenshots

### Home
![Home Dashboard](docs/screenshots/home.png)

### Executive Overview
![Executive Overview](docs/screenshots/executive_overview.png)

### Governorate Performance
![Governorate Performance](docs/screenshots/governate_performance.png)

### Station Monitoring
![Station Monitoring](docs/screenshots/station_monitoring.png)

### Water Quality Trends
![Water Quality Trends](docs/screenshots/water_quality_trends.png)

### Data Trust
![Data Trust](docs/screenshots/data_trust.png)



\## How to Run the Project



\### 1. Start Docker services



From the project root folder:



```powershell

docker compose up -d

```



This starts the containerized services defined in:



```text

docker-compose.yml

```



Depending on the environment, the services may include:



\*   Airflow

\*   Kafka

\*   Spark

\*   SQL Server

\*   Supporting services



\*\*\*



\### 2. Open Airflow



After Docker services are running, open Airflow in the browser.



Common Airflow URL:



```text

http://localhost:8080

```



Then trigger the main DAG:



```text

EWIS\_Enterprise\_Final

```



or the active DAG configured in:



```text

airflow/dags/ewis\_main\_dag.py

```



\*\*\*



\### 3. Run the Streamlit dashboard



Open a terminal and navigate to the dashboard folder:



```powershell

cd "C:\\Users\\scisa\\Desktop\\projects\\Depi Project\\dashboard\_app"

```



Run Streamlit:



```powershell

streamlit run Home.py

```



Then open:



```text

http://localhost:8501

```



If port `8501` is already in use:



```powershell

streamlit run Home.py --server.port 8502

```



Then open:



```text

http://localhost:8502

```



\*\*\*



\## Important Clean Rerun Note



Before running the full pipeline from scratch again, remove old runtime state files.



The project contains runtime markers and checkpoints such as:



```text

airflow/dags/.metadata\_sent

airflow/dags/.reference\_sent

shared/checkpoints/

airflow/logs/

```



These files are generated during previous pipeline runs.



If the project is rerun from the beginning, old flags and checkpoints may cause steps to be skipped or may make streaming jobs continue from previous offsets.



For a clean rerun, remove runtime state such as:



```powershell

Remove-Item -Force .\\airflow\\dags\\.metadata\_sent -ErrorAction SilentlyContinue

Remove-Item -Force .\\airflow\\dags\\.reference\_sent -ErrorAction SilentlyContinue

Remove-Item -Recurse -Force .\\shared\\checkpoints -ErrorAction SilentlyContinue

```



Airflow logs can also be removed before pushing to GitHub:



```powershell

Remove-Item -Recurse -Force .\\airflow\\logs -ErrorAction SilentlyContinue

```



Do not delete source code, SQL scripts, dashboard files, reference CSVs, or Docker configuration.



\*\*\*



\## What Should Not Be Pushed to GitHub



Do not push runtime-generated files such as:



```text

airflow/logs/

shared/checkpoints/

\_\_pycache\_\_/

\*.pyc

.metadata\_sent

.reference\_sent

project\_tree.txt

```



These files are generated during execution and are not part of the source code.



\*\*\*



\## Dashboard Requirements



The Streamlit dashboard dependencies are stored in:



```text

dashboard\_app/requirements.txt

```



Install requirements using:



```powershell

pip install -r dashboard\_app/requirements.txt

```



\*\*\*



\## Running Only the Dashboard



If the database and marts are already available, the dashboard can be run independently:



```powershell

cd "C:\\Users\\scisa\\Desktop\\projects\\Depi Project\\dashboard\_app"

streamlit run Home.py

```



\*\*\*



\## Dashboard Refresh



The dashboard pages use automatic refresh:



```python

st\_autorefresh(interval=30000)

```



This means the dashboard refreshes approximately every 30 seconds to show updated mart data.



\*\*\*



\## Key Business Logic



\### Open Alerts



Only currently open alerts are treated as active:



```sql

WHERE status = 'open'

```



\### Station Status



Station status is driven by:



```text

overall\_status\_color

```



Typical status colors:



| Color  | Meaning                       |

| ------ | ----------------------------- |

| Green  | Stable                        |

| Yellow | Monitoring required           |

| Orange | Elevated risk                 |

| Red    | Critical attention required   |

| Gray   | Missing or unavailable signal |



\### Data Trust



Data reliability is evaluated using:



\*   Valid readings

\*   Invalid readings

\*   Partial readings

\*   Duplicate readings

\*   Suspicious readings

\*   Stale signal flags

\*   Drift-risk indicators

\*   Data quality score

\*   Data quality band



\*\*\*



\## Project Value



EWIS demonstrates an end-to-end data engineering solution that connects streaming telemetry, data quality controls, mart modeling, orchestration, and business intelligence visualization.



The project shows how raw sensor readings can be transformed into trusted operational intelligence for water infrastructure monitoring.



\*\*\*



\## Author



Built as an end-to-end data engineering and BI project for water intelligence monitoring.



&#x20;



