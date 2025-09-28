"""
OpenFDA (device/event - Continuous Glucose Monitors)
DAG mensal: coleta diária do mês em execução, agrega por semana e grava no BigQuery.
- Endpoint: https://api.fda.gov/device/event.json
- Filtro: device.generic_name:"continuous glucose"
- Janela: date_received:[YYYYMMDD TO YYYYMMDD]
- Agregação: count=date_received → soma semanal (W-MON)
- Saída intermediária: DataFrame → XCom (lista de dicts)
- Persistência: BigQuery (cria dataset/tabela, limpa janela e insere)
"""

from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import pandas as pd
import requests

# =========================
# Parâmetros editáveis
# =========================
PROJECT_ID  = "bigquery-471718"          # seu projeto GCP
DATASET     = "openfda"                  # dataset destino
TABLE       = "cgm_device_events_weekly" # tabela destino
LOCATION    = "US"                       # região do dataset (US/EU/southamerica-east1)
GCP_CONN_ID = "google_cloud_default"     # conexão com chave JSON

API_HOST = "https://api.fda.gov"
ENDPOINT = "device/event.json"
GENERIC_NAME_QUERY = 'device.generic_name:%22continuous+glucose%22'

# =========================
# Utilidades de datas
# =========================
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

# =========================
# Task: extrair e agregar semanal
# =========================
def fetch_openfda_data() -> None:
    ctx = get_current_context()
    ti = ctx["ti"]
    logical_date = ctx["logical_date"]    # pendulum DateTime
    year, month = logical_date.year, logical_date.month

    url, start_yyyymmdd, end_yyyymmdd = generate_query_url(year, month)
    resp = requests.get(url, timeout=30)

    if resp.status_code != 200:
        ti.xcom_push(key="openfda_weekly", value=[])
        return

    results = resp.json().get("results", [])
    if not results:
        ti.xcom_push(key="openfda_weekly", value=[])
        return

    df = pd.DataFrame(results)                  # ["time","count"]
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d")

    weekly = (
        df.resample("W-MON", on="time")["count"]
          .sum()
          .reset_index()
          .rename(columns={"time": "week_end", "count": "events"})
    )
    weekly["week_start"] = weekly["week_end"] - pd.to_timedelta(6, unit="D")

    window_start = f"{start_yyyymmdd[:4]}-{start_yyyymmdd[4:6]}-{start_yyyymmdd[6:]}"
    window_end   = f"{end_yyyymmdd[:4]}-{end_yyyymmdd[4:6]}-{end_yyyymmdd[6:]}"

    weekly_out = (
        weekly[["week_start", "week_end", "events"]]
        .assign(
            week_start=lambda x: x["week_start"].dt.strftime("%Y-%m-%d"),
            week_end=lambda x: x["week_end"].dt.strftime("%Y-%m-%d"),
        )
        .to_dict(orient="records")
    )

    ti.xcom_push(
        key="openfda_weekly",
        value={
            "rows": weekly_out,
            "window_start": window_start,
            "window_end": window_end,
            "endpoint": "device/event",
            "filter": "device.generic_name:continuous glucose",
        },
    )

# =========================
# DDL de criação no BigQuery
# =========================
def bq_create_sql() -> str:
    return f"""
    CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{DATASET}`
    OPTIONS(location = '{LOCATION}');

    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{TABLE}` (
      week_start   DATE,
      week_end     DATE,
      events       INT64,
      endpoint     STRING,
      window_start DATE,
      window_end   DATE,
      ingested_at  TIMESTAMP
    )
    PARTITION BY DATE_TRUNC(week_start, MONTH);
    """

# =========================
# DAG (Airflow 3)
# =========================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="openfda_cgm_weekly_to_bq",
    default_args=default_args,
    description="OpenFDA device/event (CGM) → weekly → BigQuery",
    schedule="@monthly",              # Airflow 3: use 'schedule'
    start_date=datetime(2020, 1, 1),
    catchup=True,
    max_active_runs=1
)

fetch_data_task = PythonOperator(
    task_id="fetch_openfda_data",
    python_callable=fetch_openfda_data,  # sem provide_context
    dag=dag,
)

bq_create = BigQueryInsertJobOperator(
    task_id="bq_create_objects",
    gcp_conn_id=GCP_CONN_ID,
    location=LOCATION,
    configuration={"query": {"query": bq_create_sql(), "useLegacySql": False}},
    dag=dag,
)

bq_load = BigQueryInsertJobOperator(
    task_id="bq_load_weekly",
    gcp_conn_id=GCP_CONN_ID,
    location=LOCATION,
    configuration={
        "query": {
            "query": """
            DECLARE ws DATE DEFAULT @window_start;
            DECLARE we DATE DEFAULT @window_end;

            WITH src AS (
              SELECT
                CAST(JSON_VALUE(x, '$.week_start') AS DATE) AS week_start,
                CAST(JSON_VALUE(x, '$.week_end')   AS DATE) AS week_end,
                CAST(JSON_VALUE(x, '$.events')     AS INT64) AS events
              FROM UNNEST(JSON_QUERY_ARRAY(@rows_json, '$')) AS x
            )
            DELETE FROM `{{ params.project }}.{{ params.dataset }}.{{ params.table }}`
            WHERE week_start BETWEEN ws AND we;

            INSERT INTO `{{ params.project }}.{{ params.dataset }}.{{ params.table }}`
            (week_start, week_end, events, endpoint, window_start, window_end, ingested_at)
            SELECT
              s.week_start,
              s.week_end,
              s.events,
              @endpoint,
              ws,
              we,
              CURRENT_TIMESTAMP()
            FROM src s
            WHERE s.week_start BETWEEN ws AND we;
            """,
            "useLegacySql": False,
            "parameterMode": "NAMED",
            "queryParameters": [
                {
                    "name": "rows_json",
                    "parameterType": {"type": "STRING"},
                    "param
