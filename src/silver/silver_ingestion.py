from __future__ import annotations

from typing import Iterable, TYPE_CHECKING

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import upper, regexp_replace, col, expr

from src.utils.utils import add_metadados_cols, empty_as_null

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

def normalizar_empresas_silver(df_bronze: DataFrame, ano_mes: str) -> DataFrame:
    """
    Realiza a normalização dos dados da tabela Bronze de empresas para carregar na Silver.
    Converte tipos de dados, limpa strings e adiciona colunas de metadados.

    Args:
        df_bronze: DataFrame da tabela Bronze de empresas
        ano_mes: String com o ano e mês no formato 'YYYY-MM' para particionar a tabela Delta

    Returns:
        DataFrame pronto para ser salvo na tabela Silver de empresas
    """
    df = (
        df_bronze.select(
            empty_as_null("cnpj").alias("cnpj"),
            upper(regexp_replace(col("razao_social"), r"\s+", " ")).alias("razao_social"),
            expr("try_cast(regexp_replace(natureza_juridica, '[^0-9-]', '') as int)").alias("natureza_juridica"),
            expr("try_cast(regexp_replace(qualificacao_responsavel, '[^0-9-]', '') as int)").alias("qualificacao_responsavel"),
            regexp_replace(regexp_replace(col("capital_social"), r"\.", ""), ",", ".").cast("double").alias("capital_social"),
            empty_as_null("cod_porte").alias("cod_porte"))
    )
    return add_metadados_cols(df, ano_mes)


def normalizar_socios_silver(df_bronze: DataFrame, ano_mes: str) -> DataFrame:
    """
    Realiza a normalização dos dados da tabela Bronze de socios para carregar na Silver.
    Converte tipos de dados, limpa strings e adiciona colunas de metadados.

    Args:
        df_bronze: DataFrame da tabela Bronze de socios
        ano_mes: String com o ano e mês no formato 'YYYY-MM' para particionar a tabela Delta

    Returns:
        DataFrame pronto para ser salvo na tabela Silver de socios
    """
    df = (
        df_bronze.select(
            empty_as_null("cnpj").alias("cnpj"),
            col("tipo_socio").cast("int").alias("tipo_socio"),
            upper(regexp_replace(col("nome_socio"), r"\s+", " ")).alias("nome_socio"),
            regexp_replace(col("documento_socio"), r"[^0-9*]", "").alias("documento_socio"),
            col("codigo_qualificacao_socio").alias("codigo_qualificacao_socio"),
        )
    )
    return add_metadados_cols(df, ano_mes)


def write_silver_merge(spark: SparkSession, df_source: DataFrame, path_silver: str, key_columns: Iterable[str], particao: str = "ano_mes") -> None:
    """
    Realiza um merge (upsert) do DataFrame fonte na tabela Silver de destino, usando as
    colunas chaves para o merge e ano_mes como partição. Se a tabela Delta não existir,
    cria uma nova.
    """
    df_source = df_source.dropDuplicates(list(key_columns))

    if DeltaTable.isDeltaTable(spark, path_silver):
        df_target = DeltaTable.forPath(spark, path_silver)
        cond = " AND ".join([f"t.{k} = s.{k}" for k in key_columns])

        (df_target.alias("t")
            .merge(df_source.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
        
    else:
        df_source.write.format("delta").mode("overwrite").partitionBy(particao).save(path_silver)