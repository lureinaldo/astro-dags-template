from __future__ import annotations

from datetime import datetime, timedelta
import json
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# =========================
# Parâmetros editáveis
# =========================
PROJECT_ID  = "bigquery-471718"            # ajuste para seu projeto GCP
DATASET     = "openfda"                    # dataset de destino
TABLE       = "cgm_device_events_daily"    # tabela de destino (diária)
LOCATION    = "US"                         # região do dataset (ex.: US, EU)
GCP_CONN_ID = "google_cloud_default"       # conexão GCP no Astronomer/Airflow

API_HOST = "https://api.fda.gov"
ENDPOINT = "device/event.json"
GENERIC_NAME_QUERY = 'device.generic_name:%22continuous+glucose%22'  # Continuous Glucose Monitors

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
# Task: extrair contagem diária → XCom
# =========================
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

        rows = []
        for r in results:
            dt = datetime.strptime(str(r["time"]), "%Y%m%d").strftime("%Y-%m-%d")
            rows.append({"date": dt, "events": int(r["count"])})
        payload["rows"] = rows

        ti.xcom_push(key="openfda_daily", value=payload)
    except Exception:
        ti.xcom_push(key="openfda_daily", value=payload)

# =========================
# Task: montar config do job BQ (SEM DML) → XCom
# =========================
def prepare_bq_job_config_daily() -> None:
    """
    Evita DML no Sandbox usando DDL:
    CREATE OR REPLACE TABLE ... AS
      (linhas fora da janela) UNION ALL (linhas da janela convertidas do JSON).
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

            /* Recria a tabela substituindo somente a janela [ws..we] */
            CREATE OR REPLACE TABLE `{PROJECT
