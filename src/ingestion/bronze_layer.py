"""
Etapa 2 - Bronze
================
Lê TODOS os arquivos parquet originais da landing zone com PySpark e grava
uma tabela Delta na camada Bronze.

Responsabilidades desta camada:
  - Ler os dados exatamente como a fonte forneceu (nenhuma linha é
    descartada aqui -- a Bronze é a fonte da verdade "as-is" dentro do
    Data Lake, útil para reprocessamentos e auditoria);
  - Normalizar divergências de SCHEMA entre meses (não de conteúdo). A NYC
    TLC muda o schema dos arquivos entre meses de 2023: a coluna
    "Airport_fee"/"airport_fee" muda de capitalização e nem sempre está
    presente em todos os meses. Por isso, cada arquivo mensal é lido
    SEPARADAMENTE, normalizado para um schema canônico explícito (nomes em
    lowercase, tipos fixos, colunas ausentes viram NULL), e só depois os
    meses são unidos com `unionByName`;
  - Adicionar metadados técnicos de ingestão (arquivo de origem, timestamp
    de ingestão) para rastreabilidade/lineage;
  - Gravar em Delta Lake, particionado por ano/mês de referência do
    arquivo, o que garante ingestão idempotente (sobrescreve só a partição
    do mês reprocessado) e leitura eficiente nas camadas seguintes.

Uso:
    python -m src.ingestion.bronze_layer
"""

import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.config import BRONZE_TABLE, LANDING_PATH, LANDING_PATH_LOCAL, TABLE_FORMAT  
from src.utils.spark_session import get_spark 

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

RAW_SCHEMA: dict[str, str] = {
    "vendorid": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "double",
    "trip_distance": "double",
    "ratecodeid": "double",
    "store_and_fwd_flag": "string",
    "pulocationid": "int",
    "dolocationid": "int",
    "payment_type": "long",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}


def _list_month_files(landing_path_local: str) -> list[tuple[int, int, str]]:
    """Lista os arquivos parquet na landing zone, extraindo (ano, mês, path)
    a partir da estrutura de partição Hive-style year=YYYY/month=MM/."""
    root = Path(landing_path_local)
    found = []
    for f in sorted(root.glob("year=*/month=*/*.parquet")):
        year = int(f.parent.parent.name.split("=")[1])
        month = int(f.parent.name.split("=")[1])
        found.append((year, month, str(f)))
    return found


def _read_one_month(spark, year: int, month: int, file_path: str) -> DataFrame:
    """Lê um único arquivo mensal e normaliza para o schema canônico."""
    raw = spark.read.parquet(file_path)
    lower_map = {c.lower(): c for c in raw.columns}

    select_exprs = []
    for canonical_name, spark_type in RAW_SCHEMA.items():
        if canonical_name in lower_map:
            select_exprs.append(
                F.col(lower_map[canonical_name]).cast(spark_type).alias(canonical_name)
            )
        else:
            select_exprs.append(F.lit(None).cast(spark_type).alias(canonical_name))

    return (
        raw.select(*select_exprs)
        .withColumn("year", F.lit(year).cast("int"))
        .withColumn("month", F.lit(month).cast("int"))
        .withColumn("_source_file", F.lit(file_path))
    )


def read_landing(spark, landing_path: str = LANDING_PATH, landing_path_local: str = LANDING_PATH_LOCAL) -> DataFrame:
    """Lê todos os meses da landing zone, cada um normalizado para o mesmo
    schema canônico, e os une com unionByName."""
    months = _list_month_files(landing_path_local)
    if not months:
        raise FileNotFoundError(f"Nenhum arquivo parquet encontrado em {landing_path_local}")

    dfs = [_read_one_month(spark, year, month, path) for year, month, path in months]
    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df)
    return result


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Adiciona metadados técnicos de auditoria/lineage restantes
    (_source_file já é adicionado por mês em _read_one_month)."""
    return df.withColumn("_ingestion_timestamp", F.current_timestamp())


def write_bronze(df: DataFrame, spark, table: str = BRONZE_TABLE) -> None:
    """Grava como tabela gerenciada (sem location explícita) -- no Unity
    Catalog, tabelas externas exigem LOCATION num path de cloud storage
    registrado (External Location), não um Volume; deixando sem location,
    o Databricks escolhe e gerencia o armazenamento físico sozinho. Fora
    do Databricks, `saveAsTable` grava no warehouse padrão do Spark."""
    
    schema_name = ".".join(table.split(".")[:-1])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    (
        df.write.format(TABLE_FORMAT)
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    logger.info("Tabela Bronze disponível em: %s (formato: %s)", table, TABLE_FORMAT)


def run(landing_path: str = LANDING_PATH, landing_path_local: str = LANDING_PATH_LOCAL,
        bronze_table: str = BRONZE_TABLE):
    spark = get_spark()
    logger.info("Lendo landing zone em %s", landing_path)
    df = read_landing(spark, landing_path, landing_path_local)
    df = add_ingestion_metadata(df)

    row_count = df.count()
    logger.info("Registros lidos da landing zone: %d", row_count)

    write_bronze(df, spark, bronze_table)
    logger.info("Ingestão Bronze concluída.")
    return df


if __name__ == "__main__":
    run()