from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

def get_spark():
    builder = (
        SparkSession.builder
        .appName(os.getenv("SPARK_APP_NAME", "ml_data_engineer_spark"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.jars", "/app/jars/postgresql-42.7.3.jar")
        .config("spark.driver.extraClassPath", "/app/jars/postgresql-42.7.3.jar")
        .config("spark.executor.extraClassPath", "/app/jars/postgresql-42.7.3.jar")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark