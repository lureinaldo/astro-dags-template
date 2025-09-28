"""
OpenFDA (device/event - Continuous Glucose Monitors)
DAG mensal: coleta diária do mês em execução, agrega por semana e grava no BigQuery.

- Endpoint: https://api.fda.gov/device/event.json
- Filtro: device.generic_name:"continuous glucose"
- Janela: date_received:[YYYYMMDD TO YYYYMMDD]
- Agregação: count=date_received → soma semanal (W-MON)
- Persistência: BigQuery (cria dataset/tabela, limpeza idempotente da janela e inserção)
"""

from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import json

# =========================
# Parâmetros editáveis
# =========================
PROJECT_ID  = "bigquery-471718"            # ajuste para seu projeto GCP
DATASET     = "openfda"                    # dataset de destino
TABLE       = "cgm_device_events_weekly"   # tabela de destino
LOCATION    = "US"                         # ex.: US, EU, southamerica-east1
GCP_CONN_ID = "google_cloud_default"       # Connection GCP (Service Account Key)

API_HOST = "https://api.fda.gov"
ENDPOINT = "device/event.json"
GENERIC_NAME_QUERY = 'device.generic_name:%22continuous+glucose%22'  # Continuous Glucose Monitors

# =========================
# Utilidades de datas
# =========================
def last_day_of_month(year: int, month: int) -> int:
    """Retorna o último dia do mês-alvo."""
    d = datetime(year, month, 28) + timedelta(days=4)   # avança para o mês seguinte
    return (d.replace(day=1) - timedelta(days=1)).day   # volta ao último dia do mês-alvo

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
# Task: extrair e agregar semanal → XCom padronizado
# =========================
def fetch_openfda_data() -> None:
    """
    Publica em XCom um dicionário com:
      rows: [{week_start, week_end, events}, ...]
      window_start: 'YYYY-MM-DD'
      window_end:   'YYYY-MM-DD'
      endpoint:     'device/event'
    Mesmo sem dados, mantém as chaves (rows=[]).
    """
    ctx = get_current_context()
    ti = ctx["ti"]
    logical_date = ctx["logical_date"]
    year, month = logical_date.year, logical_date.month

    url, start_yyyymmdd, end_yyyymmdd = generate_query_url(year, month)
    window_start = f"{start_yyyymmdd[:4]}-{start_yyyymmdd[4:6]}-{start_yyyymmdd[6:]}"
    window_end   = f"{end_yyyymmdd[:4]}-{end_yyyymmdd[4:6]}-{end_yyyymmdd[6:]}"

    payload = {
        "rows": [],
        "window_start": window_start,
        "window_end": window_end,
        "endpoint": "device/event",
    }

    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            ti.xcom_push(key="openfda_weekly", value=payload)
            return

        results = resp.json().get("results", [])
        if not results:
            ti.xcom_push(key="openfda_weekly", value=payload)
            return

        df = pd.DataFrame(results)            # ["time","count"]
        df["time"] = pd.to_datetime(df["time"], format="%Y%m%d")

        weekly = (
            df.resample("W-MON", on="time")["count"]
              .sum()
              .reset_index()
              .rename(columns={"time": "week_end", "count": "events"})
        )
        weekly["week_start"] = weekly["week_end"] - pd.to_timedelta(6, unit="D")

        payload["rows"] = (
            weekly[["week_start", "week_end", "events"]]
            .assign(
                week_start=lambda x: x["week_start"].dt.strftime("%Y-%m-%d"),
                week_end=lambda x: x["week_end"].dt.strftime("%Y-%m-%d"),
            )
            .to_dict(orient="records")
        )

        ti.xcom_push(key="openfda_weekly", value=payload)

    except Exception:
        ti.xcom_push(key="openfda_weekly", value=payload)

# =========================
# Task: preparar o job config do BigQuery → XCom
# =========================
def prepare_bq_job_config() -> None:
    """
    Constrói o dicionário 'configuration' completo para o BigQueryInsertJobOperator,
    usando o XCom da extração. Evita Jinja complexo e StrictUndefined.
    """
    ctx = get_current_context()
    ti = ctx["ti"]

    x = ti.xcom_pull(task_ids="fetch_openfda_data", key="openfda_weekly") or {}
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
                CAST(JSON_VALUE(j, '$.week_start') AS DATE) AS week_start,
                CAST(JSON_VALUE(j, '$.week_end')   AS DATE) AS week_end,
                CAST(JSON_VALUE(j, '$.events')     AS INT64) AS events
              FROM UNNEST(JSON_QUERY_ARRAY(@rows_json)) AS j
            )
            -- limpeza idempotente da janela alvo
            DELETE FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
            WHERE week_start BETWEEN ws AND we;

            -- inserção
            INSERT INTO `{PROJECT_ID}.{DATASET}.{TABLE}`
            (week_start, week_end, events, endpoint, window_start, window_end, ingested_at)
            SELECT
              s.week_start, s.week_end, s.events,
              @endpoint, ws, we, CURRENT_TIMESTAMP()
            FROM src s
            WHERE s.week_start BETWEEN ws AND we;
            """,
            "queryParameters": [
                {
                    "name": "rows_json",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": rows_json},
                },
                {
                    "name": "window_start",
                    "parameterType": {"type": "DATE"},
                    "parameterValue": {"value": window_start},
                },
                {
                    "name": "window_end",
                    "parameterType": {"type": "DATE"},
                    "parameterValue": {"value": window_end},
                },
                {
                    "name": "endpoint",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": endpoint},
                },
            ],
        }
    }

    ti.xcom_push(key="bq_job_config", value=configuration)

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
# Definição da DAG (Airflow 3)
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
    schedule="@monthly",
    start_date=datetime(2020, 1, 1),
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,  # <- permite injetar dict nativo via template
)

# Tarefas
fetch_data_task = PythonOperator(
    task_id="fetch_openfda_data",
    python_callable=fetch_openfda_data,
    dag=dag,
)

prepare_bq_job = PythonOperator(
    task_id="prepare_bq_job_config",
    python_callable=prepare_bq_job_config,
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
    # Com render_template_as_native_obj=True, isto injeta o dict do XCom sem converter para string
    configuration="{{ ti.xcom_pull(task_ids='prepare_bq_job_config', key='bq_job_config') }}",
    dag=dag,
)

# Dependências: extração → preparar job; criação pode rodar em paralelo antes do load
fetch_data_task >> prepare_bq_job
[prepare_bq_job, bq_create] >> bq_load
