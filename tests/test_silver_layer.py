"""
Testes unitários das transformações da camada Silver.

Não dependem de nenhum dado externo (real ou sintético em disco) -- criam
DataFrames pequenos em memória, controlados linha a linha, para validar
que cada regra de qualidade filtra exatamente o que deveria.

Uso:
    python -m pytest tests/test_silver_layer.py -v
"""

import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.transformation.silver_layer import apply_quality_rules, add_derived_columns  
from src.utils.spark_session import get_spark  


@pytest.fixture(scope="module")
def spark():
    return get_spark(app_name="ifood-case-tests")


SCHEMA_COLS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "total_amount", "trip_distance",
    "PULocationID", "DOLocationID", "payment_type",
]


def make_row(pickup=None, dropoff=None, passenger_count=1, total_amount=10.0):
    return (1, pickup, dropoff, passenger_count, total_amount, 1.0, 100, 200, 1)


def test_drop_null_pickup_or_dropoff(spark):
    rows = [
        make_row(datetime(2023, 1, 1, 10), datetime(2023, 1, 1, 10, 20)),  # válida
        make_row(None, datetime(2023, 1, 1, 10, 20)),                       # pickup nulo
        make_row(datetime(2023, 1, 1, 10), None),                          # dropoff nulo
    ]
    df = spark.createDataFrame(rows, SCHEMA_COLS)
    result = apply_quality_rules(df)
    assert result.count() == 1


def test_drop_dropoff_before_pickup(spark):
    rows = [
        make_row(datetime(2023, 2, 1, 10, 0), datetime(2023, 2, 1, 10, 30)),  # válida
        make_row(datetime(2023, 2, 1, 10, 30), datetime(2023, 2, 1, 10, 0)),  # dropoff < pickup
    ]
    df = spark.createDataFrame(rows, SCHEMA_COLS)
    result = apply_quality_rules(df)
    assert result.count() == 1


def test_drop_pickup_outside_window(spark):
    rows = [
        make_row(datetime(2023, 5, 15, 8), datetime(2023, 5, 15, 8, 15)),  # dentro da janela
        make_row(datetime(2002, 1, 1, 8), datetime(2002, 1, 1, 8, 15)),    # fora da janela (erro TLC)
        make_row(datetime(2023, 6, 1, 0), datetime(2023, 6, 1, 0, 10)),    # limite exclusivo
    ]
    df = spark.createDataFrame(rows, SCHEMA_COLS)
    result = apply_quality_rules(df)
    assert result.count() == 1


def test_negative_total_amount_is_kept(spark):
    """Estornos (total_amount negativo) são um valor de negócio válido e
    não devem ser removidos pela camada de qualidade."""
    rows = [make_row(datetime(2023, 3, 1, 9), datetime(2023, 3, 1, 9, 10), total_amount=-12.5)]
    df = spark.createDataFrame(rows, SCHEMA_COLS)
    result = apply_quality_rules(df)
    assert result.count() == 1
    assert result.collect()[0]["total_amount"] == -12.5


def test_derived_columns(spark):
    rows = [make_row(datetime(2023, 5, 10, 14, 30), datetime(2023, 5, 10, 14, 45))]
    df = spark.createDataFrame(rows, SCHEMA_COLS)
    result = add_derived_columns(df).collect()[0]
    assert result["pickup_year"] == 2023
    assert result["pickup_month"] == 5
    assert result["pickup_hour"] == 14
