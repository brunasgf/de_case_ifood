"""
Cria a SparkSession usada em todo o pipeline.

Detecta automaticamente se está rodando dentro do Databricks (onde a
SparkSession global `spark` já existe e o Delta Lake já vem habilitado)
ou localmente (onde precisamos configurar o pacote `delta-spark` na mão).
Isso permite que o mesmo código dos jobs rode em ambos os ambientes sem
alteração -- só a criação da sessão muda.
"""


def get_spark(app_name: str = "ifood-case-nyc-taxi"):
    from src.config import TABLE_FORMAT

    try:
        from pyspark.sql import SparkSession

        active = SparkSession.getActiveSession()
        if active is not None:
            return active
    except ImportError:
        pass

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
    )

    if TABLE_FORMAT == "delta":
        from delta import configure_spark_with_delta_pip

        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
    
    return builder.enableHiveSupport().getOrCreate()
