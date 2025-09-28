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
BQ_TABLE_COUNT  = "cgm_device_events_daily"  # contagem diária final
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
    # exists_ok=True evita exceção se já existir
    client.create_dataset(ds, exists_ok=True)

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
                all_rows.extend(rows)
                total_dia += len(rows)
                n_calls += 1
                if len(rows) < limit:
                    break
                skip += limit
                time.sleep(0.25)  # respeita rate limit
            print(f"[fetch] {day}: {total_dia} registros.")
            day = date.fromordinal(day.toordinal() + 1)
        print(f"[fetch] Jan–Jun/2020: {n_calls} chamadas, {len(all_rows)} registros no total.")

        # Normaliza
        df = _to_flat_device(all_rows)
        print(f"[normalize] linhas pós-normalização: {len(df)}")
        if not df.empty:
            print("[normalize] preview:\n", df.head(10).to_string(index=False))

        # Cliente BigQuery
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION)
        client: bigquery.Client = bq_hook.get_client()
        _ensure_dataset(client, GCP_PROJECT, BQ_DATASET, BQ_LOCATION)

        # Esquema da STAGE
        schema_stage = [
            bigquery.SchemaField("report_number",     "STRING"),
            bigquery.SchemaField("date_received",     "DATE"),
            bigquery.SchemaField("event_type",        "STRING"),
            bigquery.SchemaField("device_generic",    "STRING"),
            bigquery.SchemaField("device_brand",      "STRING"),
            bigquery.SchemaField("manufacturer_name", "STRING"),
            bigquery.SchemaField("source_country",    "STRING"),
        ]

        # DataFrame vazio com colunas corretas, se necessário
        if df.empty:
            df = pd.DataFrame({f.name: pd.Series(dtype="object") for f in schema_stage})
            # Ajusta tipos básicos
            df = df.astype({"date_received": "datetime64[ns]"}).assign(
                date_received=lambda x: pd.NaT
            )

        table_id_stage = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}"
        job_config_stage = bigquery.LoadJobConfig(
            schema=schema_stage,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = client.load_table_from_dataframe(
            dataframe=df,
            destination=table_id_stage,
            job_config=job_config_stage,
            location=BQ_LOCATION,
        )
        load_job.result()
        print(f"[stage] Gravados {len(df)} registros em {table_id_stage}.")

        return {
            "start":  TEST_START.strftime("%Y-%m-%d"),
            "end":    TEST_END.strftime("%Y-%m-%d"),
            "generic": DEVICE_GENERIC,
        }

    @task(retries=0)
    def build_daily_counts(meta: Dict[str, str]) -> None:
        """
        Agrega contagem diária a partir da STAGE e salva a tabela final.
        """
        start, end, generic = meta["start"], meta["end"], meta["generic"]

        query_sql = f"""
        SELECT
          date_received AS day,
          COUNT(*)      AS events,
          '{generic}'   AS device_generic
        FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}`
        WHERE date_received BETWEEN DATE('{start}') AND DATE('{end}')
        GROUP BY day
        ORDER BY day
        """

        # Cliente BigQuery
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION)
        client: bigquery.Client = bq_hook.get_client()
        _ensure_dataset(client, GCP_PROJECT, BQ_DATASET, BQ_LOCATION)

        # Executa a consulta e converte para DataFrame
        query_job = client.query(query_sql, location=BQ_LOCATION)
        df_counts = query_job.result().to_dataframe(create_bqstorage_client=False)

        if df_counts.empty:
            print("[counts] Nenhuma linha para agregar.")
            df_counts = pd.DataFrame(
                {"day": pd.Series(dtype="datetime64[ns]"),
                 "events": pd.Series(dtype="int64"),
                 "device_generic": pd.Series(dtype="object")}
            )

        schema_counts = [
            bigquery.SchemaField("day",            "DATE"),
            bigquery.SchemaField("events",         "INTEGER"),
            bigquery.SchemaField("device_generic", "STRING"),
        ]

        table_id_counts = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_COUNT}"
        job_config_counts = bigquery.LoadJobConfig(
            schema=schema_counts,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        # Ajusta tipo de 'day' para datetime, BigQuery converte para DATE
        if not df_counts.empty:
            df_counts["day"] = pd.to_datetime(df_counts["day"], errors="coerce")

        load_job2 = client.load_table_from_dataframe(
            dataframe=df_counts,
            destination=table_id_counts,
            job_config=job_config_counts,
            location=BQ_LOCATION,
        )
        load_job2.result()
        print(f"[counts] {len(df_counts)} linhas gravadas em {table_id_counts}.")

    build_daily_counts(extract_transform_load())

openfda_cgm_stage_pipeline()
