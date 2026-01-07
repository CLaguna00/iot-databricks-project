# ============================================================
# 01_extract_from_platform.py
# Extrae datos desde la plataforma (Dremio SQL API) -> Delta (Gold)
# Caso: 1ª muestra diaria por Tag (la lógica está en sql/first_sample_daily.sql)
#
# Requisitos:
# - Este notebook/script debe vivir dentro de un Databricks Repo.
# - Debes tener un Secret en Databricks:
#     scope: iot
#     key:   dremio_pat
# - Debes configurar la URL base de Dremio (sin /api/v3)
#
# Salida:
# - Delta en:  /mnt/iot/gold/daily_first_sample
# - Tabla:     iot_gold_daily_first_sample
# ============================================================

import os
import time
import requests
from typing import Dict, List, Any, Optional

from pyspark.sql import functions as F

# -----------------------------
# 0) CONFIGURACIÓN (AJUSTA AQUÍ)
# -----------------------------

# URL base de Dremio (SIN /api/v3). Ejemplo: https://dremio.miempresa.com
DREMIO_BASE_URL = "https://dremio.cepsacorp.com"

# Secrets (NO hardcodear tokens)
SECRET_SCOPE = "iot"
SECRET_KEY = "dremio_pat"

# SQL file (relativo al notebook dentro del repo)
SQL_REL_PATH = "../sql/first_sample_daily.sql"

# Salida (Gold)
GOLD_PATH = "/mnt/iot/gold/daily_first_sample"
TABLE_NAME = "iot_gold_daily_first_sample"

# Timestamp format de tu campo Momento (según tus ejemplos)
MOMENTO_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"

# Paginación Dremio
PAGE_SIZE = 500          # Dremio limita el máximo a 500
MAX_PAGES = 500          # seguridad; sube si esperas muchísimas filas
JOB_TIMEOUT_S = 600      # 10 min
POLL_S = 1.5

# Modo de escritura:
# - "overwrite": más simple para empezar (reemplaza toda la Gold)
# - "append": no recomendado si puedes re-ejecutar días ya existentes
WRITE_MODE = "overwrite"


# -----------------------------
# 1) UTILIDADES
# -----------------------------

def _get_repo_file_path(rel_path: str) -> str:
    """
    Devuelve ruta absoluta a un fichero del repo, partiendo del directorio de trabajo.
    En Repos suele funcionar bien con rutas relativas, pero esto es más robusto.
    """
    cwd = os.getcwd()
    return os.path.normpath(os.path.join(cwd, rel_path))


def load_sql_from_repo(rel_path: str) -> str:
    sql_path = _get_repo_file_path(rel_path)
    if not os.path.exists(sql_path):
        raise FileNotFoundError(
            f"No encuentro el SQL en: {sql_path}\n"
            f"Asegúrate de que el notebook está en /notebooks y el SQL en /sql."
        )
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read().strip()
    if not sql:
        raise ValueError(f"El archivo SQL está vacío: {sql_path}")
    return sql


def get_headers() -> Dict[str, str]:
    pat = 1qNZvyNAQHal4shw8DBe69uWCa1f59mE33goiFi+ljxjGT2jQ+vJpAM7oUac+Q==
    return {
        "Authorization": f"Bearer {pat}",
        "Content-Type": "application/json",
    }


def submit_sql(sql: str, headers: Dict[str, str]) -> str:
    """
    POST /api/v3/sql  -> devuelve job id
    """
    url = f"{DREMIO_BASE_URL}/api/v3/sql"
    r = requests.post(url, headers=headers, json={"sql": sql}, timeout=60)
    r.raise_for_status()
    out = r.json()
    if "id" not in out:
        raise RuntimeError(f"Respuesta inesperada al enviar SQL: {out}")
    return out["id"]


def wait_job(job_id: str, headers: Dict[str, str], poll_s: float = POLL_S, timeout_s: int = JOB_TIMEOUT_S) -> Dict[str, Any]:
    """
    GET /api/v3/job/{id} hasta COMPLETED/FAILED/CANCELED
    """
    url = f"{DREMIO_BASE_URL}/api/v3/job/{job_id}"
    t0 = time.time()
    last_state = None

    while True:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        info = r.json()
        state = info.get("jobState")
        if state and state != last_state:
            print(f"[Dremio] Job {job_id} -> {state}")
            last_state = state

        if state in ("COMPLETED", "FAILED", "CANCELED"):
            return info

        if time.time() - t0 > timeout_s:
            raise TimeoutError(f"Timeout esperando el job {job_id}. Último estado: {state}")

        time.sleep(poll_s)


def fetch_all_rows(job_id: str, headers: Dict[str, str], page_size: int = PAGE_SIZE, max_pages: int = MAX_PAGES) -> List[Dict[str, Any]]:
    """
    GET /api/v3/job/{id}/results?limit=...&offset=...
    Descarga todas las filas paginando.
    """
    url = f"{DREMIO_BASE_URL}/api/v3/job/{job_id}/results"
    rows: List[Dict[str, Any]] = []
    offset = 0

    for page in range(max_pages):
        r = requests.get(
            url,
            headers=headers,
            params={"limit": page_size, "offset": offset},
            timeout=60
        )
        r.raise_for_status()
        data = r.json()

        batch = data.get("rows", [])
        rows.extend(batch)

        print(f"[Dremio] Página {page+1}: +{len(batch)} filas (total {len(rows)})")

        if len(batch) < page_size:
            break

        offset += page_size

    return rows


# -----------------------------
# 2) EXTRACCIÓN
# -----------------------------

print("Cargando SQL desde el repo...")
sql = load_sql_from_repo(SQL_REL_PATH)
print("OK. Enviando SQL a Dremio...")

headers = get_headers()

job_id = submit_sql(sql, headers=headers)
job_info = wait_job(job_id, headers=headers)

if job_info.get("jobState") != "COMPLETED":
    raise RuntimeError(f"El job {job_id} terminó en {job_info.get('jobState')}. Detalle: {job_info}")

print("Descargando resultados...")
rows = fetch_all_rows(job_id, headers=headers)

print(f"Filas totales recibidas: {len(rows)}")
if len(rows) == 0:
    print("No hay filas. No se sobrescribe la tabla Gold.")
    dbutils.notebook.exit("OK (sin datos)")

# Convertir a Spark DataFrame
df = spark.createDataFrame(rows)
print("Esquema recibido:")
df.printSchema()
display(df.limit(20))


# -----------------------------
# 3) TRANSFORMACIÓN MÍNIMA PARA BI (Gold)
# -----------------------------
# Normaliza campos comunes (ajusta nombres si tu query devuelve otros)
# Esperado: Momento, Tag, Valor, Unidades, Descripcion, Site, Fuente, rn (opcional)

gold_df = df

# Si vienen nombres raros/espacios en columnas, mejor normalizarlos aquí
# (y dejar un esquema BI consistente)
rename_map = {
    "Momento": "momento",
    "Tag": "tag",
    "Valor": "valor",
    "Unidades": "unidades",
    "Descripcion": "descripcion",
    "Site": "site",
    "Fuente": "fuente",
}
for old, new in rename_map.items():
    if old in gold_df.columns and new not in gold_df.columns:
        gold_df = gold_df.withColumnRenamed(old, new)

# Parseo y tipos
if "momento" not in gold_df.columns:
    raise ValueError("No encuentro la columna 'Momento'/'momento' en el resultado. Revisa tu SQL.")

gold_df = (
    gold_df
    .withColumn("event_time", F.to_timestamp(F.col("momento"), MOMENTO_FORMAT))
    .withColumn("event_date", F.to_date(F.col("event_time")))
)

# Valor numérico
if "valor" in gold_df.columns:
    gold_df = gold_df.withColumn("valor_num", F.col("valor").cast("double"))

# Quitar rn si viene
if "rn" in gold_df.columns:
    gold_df = gold_df.drop("rn")

# Filtros mínimos de calidad
gold_df = gold_df.filter(F.col("event_time").isNotNull())

display(gold_df.limit(20))


# -----------------------------
# 4) ESCRITURA DELTA + TABLA SQL
# -----------------------------
print(f"Escribiendo Gold en Delta: {GOLD_PATH} (mode={WRITE_MODE})")

writer = (
    gold_df.write
    .format("delta")
    .mode(WRITE_MODE)
    .option("overwriteSchema", "true")
)

# Particionar por fecha (muy útil para Power BI)
writer = writer.partitionBy("event_date")
writer.save(GOLD_PATH)

# Registrar tabla para consultarla desde Databricks SQL / Power BI
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME}
USING DELTA
LOCATION '{GOLD_PATH}'
""")

print(f"Tabla lista: {TABLE_NAME}")
display(spark.table(TABLE_NAME).orderBy(F.col("event_time").desc()).limit(50))
