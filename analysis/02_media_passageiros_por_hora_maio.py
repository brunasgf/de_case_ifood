"""
Pergunta 2
==========
Qual a média de passageiros (passenger_count) por cada hora do dia que
pegaram táxi no mês de maio, considerando todos os táxis da frota?

Observação de escopo: o case pede a ingestão apenas dos dados de YELLOW
taxi (ver "Dados Disponíveis" e a lista de colunas exigidas, que é o
schema do dataset yellow). Assim, "todos os táxis da frota" é interpretado
aqui como "toda a frota de yellow taxis ingerida" -- não há dado de green/
FHV/FHVHV no Data Lake deste projeto. Caso o escopo real inclua outras
frotas, basta repetir os passos de ingestão/bronze/silver para os
respectivos datasets e fazer um UNION na consulta abaixo.

"Hora do dia que pegou o táxi" = hora do tpep_pickup_datetime (0-23).

Uso:
    python -m analysis.02_media_passageiros_por_hora_maio
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import SILVER_TABLE, YEAR  # noqa: E402
from src.utils.spark_session import get_spark  # noqa: E402

QUERY = f"""
SELECT
    pickup_hour                                 AS hora_do_dia,
    COUNT(*)                                    AS qtd_corridas,
    ROUND(AVG(passenger_count), 2)              AS media_passenger_count
FROM {SILVER_TABLE}
WHERE pickup_year = {YEAR} AND pickup_month = 5
GROUP BY pickup_hour
ORDER BY pickup_hour
"""


def run():
    spark = get_spark()
    result = spark.sql(QUERY)
    result.show(n=24, truncate=False)
    return result


if __name__ == "__main__":
    run()
