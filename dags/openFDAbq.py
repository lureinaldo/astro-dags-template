"""
OpenFDA (device/event - Continuous Glucose Monitors)
DAG mensal: coleta diária do mês em execução, agrega por semana e grava no BigQuery.
- Busca: https://api.fda.gov/device/event.json
- Filtro: device.generic_name:"continuous glucose"
- Janela: date_received:[YYYYMMDD TO YYYYMMDD]
- Agregação: count=date_received  → soma semanal (W-MON)
- Saída intermediária: DataFrame → XCom (lista de dicts)
- Persistência: BigQuery (cria dataset/tabela, limpa janela e insere)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import pendulum

# =========================
# Parâmetros editáveis
# =========================
PROJECT_ID = "bigquery-471718"         # ex.: "meu-projeto"
DATASET    = "openfda"                 # dataset de destino
TABLE      = "cgm_device_events_weekly"
LOCATION   = "US"                      # região do seu dataset (ex.: "US", "EU", "southamerica-east1")
GCP_CONN_ID = "google_cloud_default"   # conn_id com chave JSON

API_HOST = "https://api.fda.gov"
ENDPOINT = "device/event.json"
GENERIC_NAME_QUERY = 'device.generic_name:%22continuous+glucose%22'  # Continuous Glucose Monitors

# =========================
# Utilidades de datas
# =========================
def last_day_of_month(year: int, month: int) -> int:
    """Retorna o último dia do mês/ano informados."""
    # regra segura: ir para o dia 28, somar 4 dias, voltar para o dia 1 e subtrair 1
    d = datetime(year, month, 28) + timedelta(days=4)
    return (d.replace(day=1) - timedelta(days=1)).day

def yyyymmdd(d: datetime) -> str:
    return d.strftime("%Y%m%d")

# =========================
# Monta URL de consulta
# =========================
def generate_query_url(year: int, month: int) -> str:
    start_date = f"{year}{month:02d}01"
    end_date = f"{year}{month:02d}{last_day_of_month(year, month):02d}"
    # device/event + filtro generic_name + janela por date_received + count diário
    query = (
        f"{API_HOST}/{ENDPOINT}"
        f"?search={GENERIC_NAME_QUERY}"
        f"+AND+date_received:[{start_date}+TO+{end_date}]"
        f"&count=date_received"
    )
    return query, start_date, end_date

# =========================
# Task: extrair e agregar semanal
# =========================
def fetch_openfda_data(**kwargs):
    """
    - Obtém o mês/ano da execução (execution_date).
    - Consulta device/event com count=date_received.
    - Converte para DataFrame, agrega por semana (W-MON) e publica no XCom.
    XCom: lista de dicts com week_start, week_end, events, window_start, window_end.
    """
    ti = kwargs["ti"]
    # Em DAG @monthly, execution_date representa o mês agendado
    execution_date = kwargs["execution_date"]  # pendulum DateTime
    year = execution_date.year
    month = execution_date.month

    url, start_yyyymmdd, end_yyyymmdd = generate_query_url(year, month)
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        ti.xcom_push(key="openfda_weekly", value=[])
        return

    payload = resp.json()
    results = payload.get("results", [])
    if not results:
        ti.xcom_push(key="openfda_weekly", value=[])
        return

    # DataFrame diário
    df = pd.DataFrame(results)  # colunas: ["time","count"]
    # garantir parsing explícito (YYYYMMDD)
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d")

    # Agregação semanal: W-MON (semanas terminando na 2a feira)
    weekly = (
        df.resample("W-MON", on="time")["count"]
          .sum()
          .reset_index()
          .rename(columns={"time": "week_end", "count": "events"})
    )
    # Definir início da semana (6 dias antes do fim)
    weekly["week_start"] = weekly["week_end"] - pd.to_timedelta(6, unit="D")

    # Metadados da janela (para idempotência na carga)
    window_start = f"{start_yyyymmdd[:4]}-{start_yyyymmdd[4:6]}-{start_yyyymmdd[6:]}"
    window_end   = f"{end_yyyymmdd[:4]}-{end_yyyymmdd[4:6]}-{end_yyyymmdd[6:]}"

    # Normalizar tipos para JSON
    weekly_out = (
        weekly[["week_start", "week_end", "events"]]
        .assign(
            week_start=lambda x: x["week_start"].dt.strftime("%Y-%m-%d"),
            week_end=lambda x: x["week_end"].dt.strftime("%Y-%m-%d"),
        )
        .to_dict(orient="records")
    )

    # Publica no XCom
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
# Task: criar dataset/tabela no BQ
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
# DAG
# =========================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Início em 2020-01-01 para cobrir desde 2020 (cada run processa seu próprio mês)
dag = DAG(
    dag_id="openfda_cgm_weekly_to_bq",
    default_args=default_args,
    description="OpenFDA device/event (CGM) → weekly → BigQuery",
    schedule_interval="@monthly",
    start_date=datetime(2020, 1, 1),
    catchup=True,
    max_active_tasks=1,
)

fetch_data_task = PythonOperator(
    task_id="fetch_openfda_data",
    python_callable=fetch_openfda_data,
    provide_context=True,
    dag=dag,
)

# Criação de dataset/tabela
bq_create = BigQueryInsertJobOperator(
    task_id="bq_create_objects",
    gcp_conn_id=GCP_CONN_ID,
    location=LOCATION,
    configuration={"query": {"query": bq_create_sql(), "useLegacySql": False}},
    dag=dag,
)

# Carga idempotente: apaga a janela do mês e insere as linhas do XCom
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
            -- limpar janela alvo (idempotência)
            DELETE FROM `{{ params.project }}.{{ params.dataset }}.{{ params.table }}`
            WHERE week_start BETWEEN ws AND we;

            -- inserir dados
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
                    "parameterValue": {
                        # converte o XCom (lista de dicts) para JSON
                        "value": "{{ ti.xcom_pull(task_ids='fetch_openfda_data', key='openfda_weekly')['rows'] | tojson }}",
                    },
                },
                {
                    "name": "window_start",
                    "parameterType": {"type": "DATE"},
                    "parameterValue": {
                        "value": "{{ ti.xcom_pull(task_ids='fetch_openfda_data', key='openfda_weekly')['window_start'] }}",
                    },
                },
                {
                    "name": "window_end",
                    "parameterType": {"type": "DATE"},
                    "parameterValue": {
                        "value": "{{ ti.xcom_pull(task_ids='fetch_openfda_data', key='openfda_weekly')['window_end'] }}",
                    },
                },
                {
                    "name": "endpoint",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {
                        "value": "{{ ti.xcom_pull(task_ids='fetch_openfda_data', key='openfda_weekly')['endpoint'] }}",
                    },
                },
            ],
        }
    },
    params={"project": PROJECT_ID, "dataset": DATASET, "table": TABLE},
    dag=dag,
)

fetch_data_task >> bq_create >> bq_load
