from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, when, lit, col, regexp_replace, max as max_
from src.utils.utils import read_delta

def criar_gold_flat_emp_socios(spark: SparkSession, path_silver_empresas: str, path_silver_socios: str, path_gold: str) -> str:
    """
    Constrói a tabela Gold com as seguintes colunas:
      - cnpj: chave primária
      - qtde_socios: quantidade de sócios
      - flag_socio_estrangeiro: True se pelo menos um sócio for estrangeiro
      - doc_alvo: True se cod_porte = '03' e qtde_socios > 1, senão False

    Args:
        spark: SparkSession
        path_silver_empresas: Caminho da tabela Silver de empresas (Delta)
        path_silver_socios: Caminho da tabela Silver de sócios (Delta)
        gold_path: Caminho onde a tabela Gold será salva (Delta)

    Returns:
        Caminho onde a tabela Gold foi salva (gold_path)
    """
    df_empresas = read_delta(spark, path_silver_empresas)
    df_socios = read_delta(spark, path_silver_socios)

    # Lógica da flag de sócio estrangeiro
    is_estrangeiro = (
        ((col("tipo_socio") == 2) & regexp_replace(col("documento_socio"), r"[^0-9]", "").rlike(r"9{6}")) | (regexp_replace(col("documento_socio"), r"\D", "").rlike(r"^9{14}$"))
    )

    # Agregação dos sócios por CNPJ
    socios_agg = (df_socios
        .groupBy("cnpj")
        .agg(
            countDistinct(col("documento_socio")).alias("qtde_socios"),
            max_(when(is_estrangeiro, lit(True)).otherwise(lit(False))).alias("flag_socio_estrangeiro")
        )
    )

    # Join e transformações finais
    df_gold = (df_empresas
        .select("cnpj", "cod_porte")
        .join(socios_agg, "cnpj", "left")
        .na.fill({"qtde_socios": 0, "flag_socio_estrangeiro": False})
        .withColumn("doc_alvo", (col("cod_porte") == "03") & (col("qtde_socios") > 1))
        .select("cnpj", "qtde_socios", "flag_socio_estrangeiro", "doc_alvo")
    )
    
    df_gold.write.format("delta").mode("overwrite").save(path_gold)
    
    return path_gold