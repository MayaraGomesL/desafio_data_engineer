from __future__ import annotations
import config.variables as V
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)
    
def write_gold_to_postgres(df_gold: DataFrame, tabela_final: str):
    """
    Escreve o DataFrame da camada Gold em uma tabela no PostgreSQL.
    """
    try:
        df_gold.write.format("jdbc") \
            .option("url", f"jdbc:postgresql://{V.PG_HOST}:{V.PG_PORT}/{V.PG_DB}") \
            .option("dbtable", tabela_final) \
            .option("user", V.PG_USER) \
            .option("password", V.PG_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .option("truncate", "true") \
            .save()

        logger.info("4) GOLD - Carregada com sucesso: final=%s", tabela_final)
    except Exception as e:
        logger.exception("Erro ao carregar dados para o PostgreSQL: %s", e)
        raise