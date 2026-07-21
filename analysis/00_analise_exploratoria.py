"""
Análise Exploratória (EDA)
===========================
Visão geral rápida da tabela de consumo: volumetria, nulos, distribuição
de valores e o efeito das regras de qualidade aplicadas na camada Silver.
Serve como ponto de partida antes de qualquer análise de negócio.

Uso:
    python -m analysis.00_analise_exploratoria
"""

import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import BRONZE_TABLE, SILVER_TABLE  # noqa: E402
from src.utils.spark_session import get_spark  # noqa: E402


def run():
    spark = get_spark()

    bronze = spark.table(BRONZE_TABLE)
    silver = spark.table(SILVER_TABLE)

    n_bronze = bronze.count()
    n_silver = silver.count()

    print("=" * 70)
    print("1) Volumetria: Bronze (bruto) vs Silver (consumo, pós-qualidade)")
    print("=" * 70)
    print(f"Bronze : {n_bronze:,} linhas".replace(",", "."))
    print(f"Silver : {n_silver:,} linhas".replace(",", "."))
    print(f"Removido pelas regras de qualidade: {n_bronze - n_silver:,} linhas "
          f"({100 * (n_bronze - n_silver) / n_bronze:.2f}%)".replace(",", "."))

    print("\n" + "=" * 70)
    print("2) Corridas por mês na camada de consumo")
    print("=" * 70)
    silver.groupBy("pickup_year", "pickup_month").count() \
        .orderBy("pickup_year", "pickup_month").show()

    print("=" * 70)
    print("3) Estatísticas descritivas de total_amount, trip_distance e passenger_count")
    print("=" * 70)
    silver.select("total_amount", "trip_distance", "passenger_count").describe().show()

    print("=" * 70)
    print("4) % de nulos por coluna obrigatória, na camada de consumo")
    print("=" * 70)
    required = ["VendorID", "passenger_count", "total_amount",
                "tpep_pickup_datetime", "tpep_dropoff_datetime"]
    silver.select([
        (100 * F.sum(F.col(c).isNull().cast("int")) / F.count("*")).alias(c)
        for c in required
    ]).show()

    print("=" * 70)
    print("5) Distribuição de payment_type (top 10)")
    print("=" * 70)
    silver.groupBy("payment_type").count().orderBy(F.desc("count")).show(10)

    return {"n_bronze": n_bronze, "n_silver": n_silver}


if __name__ == "__main__":
    run()
