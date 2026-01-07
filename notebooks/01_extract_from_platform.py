import time
import requests
from pyspark.sql import functions as F

# 1) URL base de tu plataforma (Dremio)
DREMIO_BASE_URL = "https://TU_HOST_DREMIO"  # <- cámbialo (sin /api/v3)

# 2) Token: guárdalo en Databricks Secrets y léelo así
# Ejemplo: scope="iot", key="dremio_pat"
DREMIO_PAT = dbutils.secrets.get("iot", "dremio_pat")

headers = {
    "Authorization": f"Bearer {DREMIO_PAT}",
    "Content-Type": "application/json"
}

# Dónde guardaremos el resultado para Power BI
gold_path = "/mnt/iot/gold/daily_first_sample"
table_name = "iot_gold_daily_first_sample"

def submit_sql(sql: str) -> str:
    # POST /api/v3/sql  -> devuelve un job id
    r = requests.post(
        f"{DREMIO_BASE_URL}/api/v3/sql",
        headers=headers,
        json={"sql": sql},
        timeout=60
    )
    r.raise_for_status()
    out = r.json()
    return out["id"]  # job id


def wait_job(job_id: str, poll_s: float = 1.5, timeout_s: int = 300) -> dict:
    # GET /api/v3/job/{id} hasta COMPLETED/FAILED/CANCELED
    t0 = time.time()
    while True:
        r = requests.get(f"{DREMIO_BASE_URL}/api/v3/job/{job_id}", headers=headers, timeout=30)
        r.raise_for_status()
        info = r.json()
        state = info.get("jobState")

        if state in ("COMPLETED", "FAILED", "CANCELED"):
            return info

        if time.time() - t0 > timeout_s:
            raise TimeoutError(f"Timeout esperando el job {job_id}. Último estado: {state}")

        time.sleep(poll_s)


def fetch_all_rows(job_id: str, page_size: int = 500, max_pages: int = 200):
    # GET /api/v3/job/{id}/results?limit=500&offset=...
    # (Dremio limita el limit máximo a 500) :contentReference[oaicite:5]{index=5}
    rows = []
    offset = 0

    for _ in range(max_pages):
        r = requests.get(
            f"{DREMIO_BASE_URL}/api/v3/job/{job_id}/results",
            headers=headers,
            params={"limit": page_size, "offset": offset},
            timeout=60
        )
        r.raise_for_status()
        data = r.json()

        batch = data.get("rows", [])
        rows.extend(batch)

        if len(batch) < page_size:
            break

        offset += page_size

    return rows
