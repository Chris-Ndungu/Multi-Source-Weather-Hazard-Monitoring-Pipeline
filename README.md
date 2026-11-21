# Multi-Source Weather & Hazard Monitoring Pipeline

A 3-week hands-on project built to prepare for the **Airflow DAG Authoring (Airflow 3)** certification while aligning batch and storage patterns with **Microsoft Fabric (DP-700)** practices. The pipeline pairs **WeatherAPI.com** with a secondary hazard or environmental source—such as **USGS Earthquake GeoJSON**, **Open-Meteo Air Quality**, **Open-Meteo Marine**, or **AISHub.net** (if feed access is active)—and progresses across three weekly milestones.

## Architecture & Core Rules

* **Airflow 3 SDK (`airflow.sdk`):** Author DAGs and tasks using `@dag` and `@task` from `airflow.sdk`, keeping DAG files compatible with Airflow 3 DAG bundles.

* **Parse-Time Discipline:** Keep top-level DAG code free of API calls, database queries, or heavy logic. Fetch runtime context (`get_current_context()`), Connections, and Variables inside task execution.

* **Lightweight XComs & Idempotent Windows:** Pass compact metadata or summaries via XComs, avoid `do_xcom_push=False` surprises on downstream consumers, and parameterize time windows (`logical_date` / `data_interval_start` / `data_interval_end`) so scheduled runs and backfills stay deterministic.

* **Orchestration vs. Storage Boundary:** Use Airflow 3 for scheduling, dynamic mapping, branching, trigger rules, and Asset-driven triggers, while treating **Microsoft Fabric** (Lakehouse `Files/` and `Tables/` across Bronze, Silver, and Gold layers) as the downstream storage and analytics target.

## 3-Week Roadmap & Deliverables

### Week 1: Ingestion, TaskFlow & Configuration

* **Ingestion DAG (`dags/ingest_weather_hazards.py`):** Pulls weather data from WeatherAPI.com and event/environmental data from the paired source using `@task` functions and date-windowed parameters.

* **Connections & Variables:** Configures Airflow Connections and Variables for endpoints, credentials, locations, and thresholds (via the Airflow UI or `airflow_settings.yaml`), read inside tasks rather than at parse time.

* **XCom & Error Notes:** Captures practical findings on payload sizing, `do_xcom_push=False`, and DAG parse vs. task runtime failures.

### Week 2: Dynamic Task Mapping, Branching & Trigger Rules

* **Dynamic Mapping (`.expand(...)`):** Scales ingestion or evaluation across multiple locations, coordinates, or records without duplicating task definitions.

* **Conditional Branching (`@task.branch`):** Routes runs along alert vs. normal summary paths based on wind speed, seismic magnitude, or environmental thresholds.

* **Explicit Trigger Rules:** Rejoins active and skipped branches cleanly (`none_failed_min_one_success`) and runs post-run telemetry/audit tasks (`all_done`).

### Week 3: Assets, Fabric Integration & Exam Polish

* **Asset-Driven DAGs (`dags/downstream_asset_summary.py`):** Emits Airflow 3 **Assets** from upstream ingestion/evaluation and triggers downstream summary or Fabric Lakehouse/warehouse steps when updates occur.

* **Airflow 3 & DP-700 Alignment:** Organizes outputs along partitioned Bronze/Silver/Gold conventions while auditing `start_date`, `catchup`, timetables, and zero top-level side effects.

* **Exam Traps Reference:** Maintains a concise checklist of high-yield Airflow 3 authoring pitfalls and debugging notes.

## Local Setup (Astro CLI)

1. **Add runtime dependencies to `requirements.txt`:**

   ```text
   requests
   ```

2. **Start the local Airflow environment:**

   ```bash
   astro dev start
   ```

3. **Import Connections and Variables (optional, via `airflow_settings.yaml`):**

   ```bash
   astro dev object import
   ```

4. **Validate DAG parsing and inspect logs during development:**

   ```bash
   astro dev parse
   astro dev logs
   ```

## Repository Layout

```text
airflow-capstone/
├── dags/
│   ├── ingest_weather_hazards.py
│   ├── hazard_branching_pipeline.py
│   └── downstream_asset_summary.py
├── include/
│   └── fixtures/
├── tests/
├── airflow_settings.yaml
├── requirements.txt
└── README.md
```