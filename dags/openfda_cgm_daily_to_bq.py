from __future__ import annotations

from datetime import datetime, timedelta
import json
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context, ShortCircuitOperator
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ===== Parâmetros =====
PROJECT_ID  = "bigquery-471718"              # ajuste para seu projeto
DATASET     = "openfda"                      # dataset de destino
TABLE       = "cgm_device_events_daily"      # tabela destino
LOCATION    = "US"                           # região do dataset
GCP_CONN_ID = "google_cloud_default"         # conexão GCP no Astronomer

API_HOST = "https://api.fda.gov"
ENDPOINT = "device/event.json"
GENERIC_NAME_QUERY = 'device.generic_name:%22continuous+glucose%22'  # Continuous Glucose Monitors

# ===== Utilidades =====
def last_day_of_month(year: int, month: int) -> int:
    d = datetime(year, month, 28) + timedelta(days=4)
    return (d.replace(day=1) - timedelta(days=1)).day

def generate_query_url(year: int, month: int) -> tuple[str, str, str]:
    start_date = f"{year}{month:02d}01"
    end_date   = f"{year}{month:02d}{last_day_of_month(year, month):02d}"
    url = (
        f"{API_HOST}/{ENDPOINT}"
        f"?search={GENERIC_NAME_QUERY}"
        f"+AND+date_received:[{start_date}+TO+{end_date}]"
        f"&count=date_received"
    )
    return url, start_date, end_date

# ===== Gate: só processa meses 1..6 =====
def is_jan_to_jun() -> bool:
    ctx = get_current_context()
    return ctx["logical_date"].month <= 6

# ===== Extração diária → XCom =====
def fetch_openfda_daily() -> None:
    """
    XCom 'openfda_daily':
      rows: [{'date':'YYYY-MM-DD','events':INT64}, ...]
      window_start/window_end: datas do mês lógico
      endpoint: string informativa
    """
    ctx = get_current_context()
    ti = ctx["ti"]
    logical_date = ctx["logical_date"]
    year, month = logical_date.year, logical_date.month

    url, s_yyyymmdd, e_yyyymmdd = generate_query_url(year, month)
    window_start = f"{s_yyyymmdd[:4]}-{s_yyyymmdd[4:6]}-{s_yyyymmdd[6:]}"
    window_end   = f"{e_yyyymmdd[:4]}-{e_yyyymmdd[4:6]}-{e_yyyymmdd[6:]}"

    payload = {"rows": [], "window_start": window_start, "window_end": window_end, "endpoint": "device/event"}

    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            ti.xcom_push(key="openfda_daily", value=payload); return

        results = resp.json().get("results", [])
        if not results:
            ti.xcom_push(key="openfda_daily", value=payload); return

        rows = []
        for r in results:
            dt = datetime.strptime(str(r["time"]), "%Y%m%d").strftime("%Y-%m-%d")
            rows.append({"date": dt, "events": int(r["count"])})
        payload["rows"] = rows

        ti.xcom_push(key="openfda_daily", value=payload)
    except Exception:
        ti.xcom_push(key="openfda_daily", value=payload)

# ===== Monta config do job BigQuery (apenas DDL) → XCom =====
def prepare_bq_job_config_daily() -> None:
    """
    DDL-only (compatível com BigQuery Sandbox, sem DML):
      1) CREATE SCHEMA IF NOT EXISTS
      2) CREATE TABLE IF NOT EXISTS (schema)
      3) CREATE OR REPLACE TABLE target AS
         SELECT DISTINCT por event_date de (target UNION ALL novas_linhas)
    """
    ctx = get_current_context()
    ti = ctx["ti"]

    x = ti.xcom_pull(task_ids="fetch_openfda_daily", key="openfda_daily") or {}
    rows = x.get("rows", [])
    window_start = x.get("window_start", "1970-01-01")
    window_end   = x.get("window_end", "1970-01-01")
    endpoint     = x.get("endpoint", "device/event")

    rows_json = json.dumps(rows, ensure_ascii=False)

    ddl = f"""
    -- parâmetros (usados só para colunas informativas)
    DECLARE ws DATE DEFAULT @window_start;
    DECLARE we DATE DEFAULT @window_end;

    -- 1) garante dataset
    CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{DATASET}` OPTIONS(location = '{LOCATION}');

    -- 2) garante tabela com schema
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{TABLE}` (
      event_date   DATE,
      events       INT64,
      endpoint     STRING,
      window_start DATE,
      window_end   DATE,
      ingested_at  TIMESTAMP
    );

    -- 3) recria a tabela como união do conteúdo atual + novas linhas (sem DML)
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.{TABLE}` AS
    WITH incoming AS (
      SELECT
        CAST(JSON_VALUE(j, '$.date')   AS DATE)  AS event_date,
        CAST(JSON_VALUE(j, '$.events') AS INT64) AS events,
        @endpoint                                   AS endpoint,
        ws                                          AS window_start,
        we                                          AS window_end,
        CURRENT_TIMESTAMP()                         AS ingested_at
      FROM UNNEST(JSON_QUERY_ARRAY(@rows_json)) AS j
    ),
    combined AS (
      SELECT * FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
      UNION ALL
      SELECT * FROM incoming
    )
    SELECT
      event_date,
      ANY_VALUE(events)      AS events,
      ANY_VALUE(endpoint)    AS endpoint,
      MIN(window_start)      AS window_start,
      MAX(window_end)        AS window_end,
      MAX(ingested_at)       AS ingested_at
    FROM combined
    GROUP BY event_date;
    """

    configuration = {
        "query": {
            "useLegacySql": False,
            "parameterMode": "NAMED",
            "query": ddl,
            "queryParameters": [
                {"name": "rows_json",    "parameterType": {"type": "STRING"}, "parameterValue": {"value": rows_json}},
                {"name": "window_start", "parameterType": {"type": "DATE"},   "parameterValue": {"value": window_start}},
                {"name": "window_end",   "parameterType": {"type": "DATE"},   "parameterValue": {"value": window_end}},
                {"name": "endpoint",     "parameterType": {"type": "STRING"}, "parameterValue": {"value": endpoint}},
            ],
        }
    }
    ti.xcom_push(key="bq_job_config_daily", value=configuration)

# ===== DAG =====
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="openfda_cgm_daily_to_bq",
    default_args=default_args,
    description="OpenFDA device/event (CGM) → contagem diária (jan–jun, DDL-only) → BigQuery",
    schedule="@monthly",
    start_date=datetime(2020, 1, 1),
    catchup=True,
    max_active_runs=1,
)

gate_h1 = ShortCircuitOperator(
    task_id="only_jan_to_jun",
    python_callable=is_jan_to_jun,
    dag=dag,
)

fetch_daily = PythonOperator(
    task_id="fetch_openfda_daily",
    python_callable=fetch_openfda_daily,
    dag=dag,
)

prepare_bq_job = PythonOperator(
    task_id="prepare_bq_job_config_daily",
    python_callable=prepare_bq_job_config_daily,
    dag=dag,
)

# Um único operador que executa as instruções DDL acima
bq_load_daily = BigQueryInsertJobOperator(
    task_id="bq_load_daily",
    gcp_conn_id=GCP_CONN_ID,
    location=LOCATION,
    configuration=XComArg(prepare_bq_job, key="bq_job_config_daily"),
    dag=dag,
)

# Encadeamento
gate_h1 >> fetch_daily >> prepare_bq_job >> bq_load_daily
