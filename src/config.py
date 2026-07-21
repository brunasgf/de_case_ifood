"""
Configurações centrais do pipeline.

Em Databricks, os paths abaixo apontam tipicamente para o DBFS ou para um
external location montado em cloud storage (S3/ADLS/GCS), por exemplo:

    LANDING_PATH = "s3://ifood-datalake/landing/yellow_tripdata"
    BRONZE_PATH  = "s3://ifood-datalake/bronze/yellow_tripdata"
    SILVER_PATH  = "s3://ifood-datalake/silver/yellow_tripdata"

Localmente (fora do Databricks), usamos o filesystem local para simular
as três camadas do Data Lake. Basta trocar os paths abaixo para rodar
em outro ambiente.
"""

import os
from pathlib import Path


def _default_datalake_root() -> str:
    """Escolhe um path padrão que funciona sem configuração manual, tanto
    no Databricks quanto localmente -- sem depender de nenhum path fixo do
    ambiente onde o código foi originalmente escrito.

    No Databricks Free Edition (o antigo "Community Edition") e em
    clusters Unity Catalog com compute compartilhado/serverless: o DBFS
    vem desativado (`/dbfs/...` falha com `OSError: Operation not
    supported`) e o disco local do driver também é bloqueado para o Spark
    fora de `/Workspace` (`LocalFilesystemAccessDeniedException`). A forma
    suportada de ter armazenamento gravável nesse ambiente é um Volume do
    Unity Catalog -- todo workspace novo já vem com um catálogo
    `workspace` e schema `default` prontos, então usamos esse Volume como
    padrão (é preciso criá-lo uma vez, ver README, seção "Databricks Free
    Edition"). Pode ser sobrescrito via env var DATALAKE_ROOT para apontar
    para outro catálogo/schema, ou para S3/ADLS/GCS em produção fora do
    Databricks.
    """
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return "/Volumes/workspace/default/ifood_case/data_lake"
    return str(Path(__file__).resolve().parents[1] / "data_lake")

DATALAKE_ROOT = os.environ.get("DATALAKE_ROOT", _default_datalake_root())


def _to_spark_path(path: str) -> str:
    """
    Path que o Spark deve usar para ler/escrever.

    """
    if "://" in path or path.startswith("/dbfs") or path.startswith("/Volumes"):
        return path
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return f"file://{path}"
    return path

DATALAKE_ROOT_SPARK = _to_spark_path(DATALAKE_ROOT)

TABLE_FORMAT = os.environ.get("IFOOD_TABLE_FORMAT", "delta")

LANDING_PATH_LOCAL = f"{DATALAKE_ROOT}/landing/yellow_tripdata"
LANDING_PATH = f"{DATALAKE_ROOT_SPARK}/landing/yellow_tripdata"

CATALOG = os.environ.get(
    "DATALAKE_CATALOG", "workspace" if os.environ.get("DATABRICKS_RUNTIME_VERSION") else ""
)
_catalog_prefix = f"{CATALOG}." if CATALOG else ""

BRONZE_TABLE = f"{_catalog_prefix}bronze.yellow_tripdata"

SILVER_TABLE = f"{_catalog_prefix}consumo.yellow_tripdata"

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

YEAR = 2023
MONTHS = [1, 2, 3, 4, 5]


REQUIRED_COLUMNS = [
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]

EXTRA_COLUMNS = [
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
]