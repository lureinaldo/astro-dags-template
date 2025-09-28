# dags/openfda_cgm_stage_pipeline.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from google.cloud import bigquery
import pendulum
import pandas as pd
import requests
import time
from datetime import date
from typing import Any, Dict, List

# ========================= Config =========================
GCP_PROJECT     = "bigquery-471718"          # seu projeto GCP
BQ_DATASET      = "openfda"                  # dataset de destino
BQ_TABLE_STAGE  = "cgm_device_events_stage"  # tabela "flat" (stage)
BQ_TABLE_COUNT  = "cgm_device_events_daily"  # contagem diária final (particionada)
BQ_LOCATION     = "US"                       # região do dataset
GCP_CONN_ID     = "google_cloud_default"     # conexão no Astronomer

# Jan -> Jun/2020 (inclusive)
TEST_START = date(2020, 1, 1)
TEST_END   = date(2020, 6, 30)

DEVICE_GENERIC = "continuous glucose"        # filtro do dispositivo

TIMEOUT_S   = 30
MAX_RETRIES = 3

# HTTP session
SESSION = requests.Session()
SESSION.headers.update(
    {"User-Agent": "didactic-openfda-etl/1.0 (contato: exemplo@dominio.com)"}
)

# ========================= Helpers =========================
def _search_expr_by_day(day: date, generic_name: str) -> str:
    """Expressão de busca por dia para device/event (openFDA)."""
    d = day.strftime("%Y%m%d")
    # Campos: device.generic_name e date_received (endpoint device/event)
    return f'device.generic_name:"{generic_name}" AND date_received:{d}'

def _openfda_get(url: str, params: Dict[str, str]) -> Dict[str, Any]:
    """GET com retentativas para a API openFDA."""
    for attempt in range(1, MAX_RETRIES + 1):
        r = SESSION.get(url, params=params, timeout=TIMEOUT_S)
        if r.status_code == 404:
            return {"results": []}
        if 200 <= r.status_code < 300:
            return r.json()
        # log simples de erro
        try:
            print("[openFDA][err]", r.status_code, r.json())
        except Exception:
            print("[openFDA][err-text]", r.status_code, r.text[:500])
        if attempt < MAX_RETRIES and r.status_code in (429, 500, 502, 503, 504):
            time.sleep(attempt)  # backoff linear simples
            continue
        r.raise_for_status()

def _to_flat_device(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Normaliza alguns campos relevantes do device/event.
    Mantém colunas simples e estáveis para o exercício.
    """
    flat: List[Dict[str, Any]] = []
    for ev in rows:
        device_list = (ev or {}).get("device", []) or []
        d0 = device_list[0] if device_list else {}
        flat.append({
            "report_number":     ev.get("report_number"),
            "date_received":     ev.get("date_received"),
            "event_type":        ev.get("event_type"),
            "device_generic":    d0.get("generic_name"),
            "device_brand":      d0.get("brand_name"),
            "manufacturer_name": ev.get("manufacturer_name"),
            "source_country":    ev.get("source_country"),
        })
    df = pd.DataFrame(flat)
    if df.empty:
        return df
    # Tipos e limpeza mínimos
    df["report_number"] = df["report_number"].astype(str)
    df["date_received"] = pd.to_datetime(
        df["date_received"], format="%Y%m%d", errors="coerce"
    ).dt.date
    df = df.drop_duplicates(subset=["report_number"], keep="first")
    return df

def _ensure_dataset(client: bigquery.Client, project: str, dataset: str, location: str) -> None:
    ds_id = f"{project}.{dataset}"
    ds = bigquery.Dataset(ds_id)
    ds.location = location
    client.create_dataset(ds, exists_ok=True)  # idempotente

# ========================= DAG =========================
@dag(
    dag_id="openfda_cgm_stage_pipeline",
    description=(
        "openFDA device/event (Continuous Glucose Monitors) → "
        "normaliza (STAGE) → agrega diário (BQ). Jan–Jun/2020."
    ),
    schedule="@once",                                     # roda uma vez
    start_date=pendulum.datetime(2025, 9, 28, tz="UTC"),  # ajuste se desejar
    catchup=False,
    max_active_runs=1,
    tags=["didatico", "openfda", "device", "bigquery", "etl"],
)
def openfda_cgm_stage_pipeline():

    @task(retries=0)
    def extract_transform_load() -> Dict[str, str]:
        """
        ETL completo no mesmo task para evitar XCom grande:
        - Busca dia a dia, com paginação (limit=1000, skip)
        - Normaliza (flat)
        - Grava STAGE no BigQuery com WRITE_TRUNCATE (idempotente)
        - Retorna metadados mínimos para o próximo task
        """
        base_url = "https://api.fda.gov/device/event.json"
        all_rows: List[Dict[str, Any]] = []

        day = TEST_START
        n_calls = 0
        while day <= TEST_END:
            limit = 1000
            skip = 0
            total_dia = 0
            while True:
                params = {
                    "search": _search_expr_by_day(day, DEVICE_GENERIC),
                    "limit": str(limit),
                    "skip": str(skip),
                }
                payload = _openfda_get(base_url, params)
                rows = payload.get("results", []) or []
                all_rows.exte_
