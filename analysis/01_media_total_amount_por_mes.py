"""
Pergunta 1
==========
Qual a média de valor total (total_amount) recebido em um mês,
considerando todos os yellow táxis da frota?

A resposta é dada por mês (Jan-Mai/2023), já que "em um mês" varia mês a
mês -- reportar um único número para o período inteiro esconderia a
sazonalidade. Os dados vêm 100% da camada de CONSUMO (Silver), acessada
aqui via Spark SQL, exatamente como um usuário final consumiria.

Uso:
    python -m analysis.01_media_total_amount_por_mes
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import SILVER_TABLE  # noqa: E402
from src.utils.spark_session import get_spark  # noqa: E402

QUERY = f"""
SELECT
    pickup_year                                AS ano,
    pickup_month                                AS mes,
    COUNT(*)                                    AS qtd_corridas,
    ROUND(AVG(total_amount), 2)                 AS media_total_amount
FROM {SILVER_TABLE}
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month
"""


def run():
    spark = get_spark()
    result = spark.sql(QUERY)
    result.show(truncate=False)
    return result


if __name__ == "__main__":
    run()
