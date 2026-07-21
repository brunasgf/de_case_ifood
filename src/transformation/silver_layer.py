"""
Etapa 3 - Silver / Camada de Consumo
=====================================
Lê a tabela Bronze, aplica regras de qualidade de dado e grava a tabela
final de CONSUMO, disponível para os usuários finais via SQL.

Regras de qualidade aplicadas (e por quê):
  1. Remove linhas em que tpep_pickup_datetime ou tpep_dropoff_datetime
     são nulos -- essas duas colunas são exigidas na camada de consumo,
     então um registro sem elas não é utilizável.
  2. Remove linhas em que dropoff < pickup -- corrida com duração negativa
     é fisicamente impossível e indica erro de medição do taxímetro.
  3. Remove linhas em que a data de pickup está fora da janela solicitada
     pelo case (2023-01-01 a 2023-05-31 inclusive);
  4. NÃO removemos valores de total_amount negativos: eles representam
     estornos/ajustes reais do domínio de negócio (débito ao passageiro),
     não erro de captura -- removê-los enviesaria a média para cima.
  5. Deduplica linhas 100% idênticas (defensivo, custo baixo).

A tabela final é particionada por (pickup_year, pickup_month).
"""

import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.config import ( 
    BRONZE_TABLE,
    REQUIRED_COLUMNS,
    SILVER_TABLE,
    TABLE_FORMAT,
    YEAR,
)
from src.utils.spark_session import get_spark 

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

WINDOW_START = f"{YEAR}-01-01 00:00:00"
WINDOW_END = f"{YEAR}-06-01 00:00:00"  # exclusivo


def select_and_cast(df: DataFrame) -> DataFrame:
    """Seleciona apenas as colunas exigidas + extras úteis e aplica tipagem
    explícita.

    """
    return df.select(
        F.col("vendorid").cast("int").alias("VendorID"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("pulocationid").cast("int").alias("PULocationID"),
        F.col("dolocationid").cast("int").alias("DOLocationID"),
        F.col("payment_type").cast("int").alias("payment_type"),
    )


def apply_quality_rules(df: DataFrame) -> DataFrame:
    before = df.count()

    df = df.dropDuplicates()

    df = df.filter(
        F.col("tpep_pickup_datetime").isNotNull()
        & F.col("tpep_dropoff_datetime").isNotNull()
    )
    df = df.filter(F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
    df = df.filter(
        (F.col("tpep_pickup_datetime") >= F.lit(WINDOW_START))
        & (F.col("tpep_pickup_datetime") < F.lit(WINDOW_END))
    )

    after = df.count()
    logger.info(
        "Regras de qualidade aplicadas: %d -> %d linhas (%.2f%% removidas)",
        before,
        after,
        100 * (before - after) / before if before else 0,
    )
    return df


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Colunas derivadas usadas para particionamento e para as análises
    (ano/mês/hora do pickup)."""
    return (
        df.withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    )


def write_silver(df: DataFrame, spark, table: str = SILVER_TABLE) -> None:
    """Grava como tabela gerenciada ."""
    schema_name = ".".join(table.split(".")[:-1])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    (
        df.write.format(TABLE_FORMAT)
        .mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    logger.info("Tabela de Consumo disponível em: %s (formato: %s)", table, TABLE_FORMAT)


def run(bronze_table: str = BRONZE_TABLE, silver_table: str = SILVER_TABLE):
    spark = get_spark()
    logger.info("Lendo tabela Bronze: %s", bronze_table)
    df = spark.table(bronze_table)

    df = select_and_cast(df)

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes na camada de consumo: {missing}")

    df = apply_quality_rules(df)
    df = add_derived_columns(df)

    write_silver(df, spark, silver_table)
    logger.info("Transformação Silver/Consumo concluída.")
    return df


if __name__ == "__main__":
    run()