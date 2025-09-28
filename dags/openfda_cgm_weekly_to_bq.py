from __future__ import annotations

from datetime import datetime, timedelta
import json
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ===== Parâmetros =====
PROJECT_ID  = "bigquery-471718"              # ajuste para seu projeto
DATASET     = "openfda"                      # dataset de destino
TABLE       = "cgm_device_events_daily"      # tabela de destino (diária)
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

# ===== Task: extrair contagem diária → XCom =====
def fetch_openfda_daily() -> None:
    """
    Extrai contagens diárias do mês lógico e publica no XCom:
      key='openfda_daily' -> {
         'rows': [{'date': 'YYYY-MM-DD', 'events': INT64}, ...],
         'window_start': 'YYYY-MM-DD',
         'window_end':   'YYYY-MM-DD',
         'endpoint':     'device/event'
      }
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

        # results: [{"time":"YYYYMMDD","count":N},...]
        rows = []
        for r in results:
            dt = datetime.strptime(str(r["time"]), "%Y%m%d").strftime("%Y-%m-%d")
            rows.append({"date": dt, "events": int(r["count"])})
        payload["rows"] = rows

        ti.xcom_push(key="openfda_daily", value=payload)

    except Exception:
        ti.xcom_push(key="openfda_daily", value=payload)

# ===== Task: preparar configuração do job de carga → XCom =====
def prepare_bq_job_config_daily() -> None:
    """
    Monta o dicionário 'configuration' para o BigQueryInsertJobOperator (carga diária idempotente).
    """
    ctx = get_current_context()
    ti = ctx["ti"]

    x = ti.xcom_pull(task_ids="fetch_openfda_daily", key="openfda_daily") or {}
    rows = x.get("rows", [])
    window_start = x.get("window_start", "1970-01-01")
    window_end   = x.get("window_end", "1970-01-01")
    endpoint     = x.get("endpoint", "device/event")

    rows_json = json.dumps(rows, ensure_ascii=False)

    configuration = {
        "query": {
            "useLegacySql": False,
            "parameterMode": "NAMED",
            "query": f"""
            DECLARE ws DATE DEFAULT @window_start;
            DECLARE we DATE DEFAULT @window_end;

            WITH src AS (
              SELECT
                CAST(JSON_VALUE(j, '$.date')   AS DATE)  AS event_date,
                CAST(JSON_VALUE(j, '$.events') AS INT64) AS events
              FROM UNNEST(JSON_QUERY_ARRAY(@rows_json)) AS j
            )
            -- limpeza idempotente da janela alvo (mês lógico)
            DELETE FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
            WHERE event_date BETWEEN ws AND we;

            -- inserção diária
            INSERT INTO `{PROJECT_ID}.{DATASET}.{TABLE}`
            (event_date, events, endpoint, window_start, window_end, ingested_at)
            SELECT
              s.event_date, s.events, @endpoint, ws, we, CURRENT_TIMESTAMP()
            FROM src s
            WHERE s.event_date BETWEEN ws AND we;
            """,
            "queryParameters": [
                {"name": "rows_json",    "parameterType": {"type": "STRING"}, "parameterValue": {"value": rows_json}},
                {"name": "window_start", "parameterType": {"type": "DATE"},   "parameterValue": {"value": window_start}},
                {"name": "window_end",   "parameterType": {"type": "DATE"},   "parameterValue": {"value": window_end}},
                {"name": "endpoint",     "parameterType": {"type": "STRING"}, "parameterValue": {"value": endpoint}},
            ],
        }
    }

    ti.xcom_push(key="bq_job_config_daily", value=configuration)

# ===== DDL de criação no BQ =====
def bq_create_sql_daily() -> str:
    return f"""
    CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{DATASET}`
    OPTIONS(location = '{LOCATION}');

    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{TABLE}` (
      event_date   DATE,
      events       INT64,
      endpoint     STRING,
      window_start DATE,
      window_end   DATE,
      ingested_at  TIMESTAMP
    )
    PARTITION BY DATE_TRUNC(event_date, MONTH);
    """

# ===== Definição da DAG =====
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="openfda_cgm_daily_to_bq",
    default_args=default_args,
    description="OpenFDA device/event (CGM) → daily counts → BigQuery",
    schedule="@monthly",
    start_date=datetime(2020, 1, 1),
    catchup=True,
    max_active_runs=1,
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

bq_create = BigQueryInsertJobOperator(
    task_id="bq_create_objects",
    gcp_conn_id=GCP_CONN_ID,
    location=LOCATION,
    configuration={"query": {"query": bq_create_sql_daily(), "useLegacySql": False}},
    dag=dag,
)

bq_load_daily = BigQueryInsertJobOperator(
    task_id="bq_load_daily",
    gcp_conn_id=GCP_CONN_ID,
    location=LOCATION,
    configuration=XComArg(prepare_bq_job, key="bq_job_config_daily"),
    dag=dag,
)

fetch_daily >> prepare_bq_job
[prepare_bq_job, bq_create] >> bq_load_daily
