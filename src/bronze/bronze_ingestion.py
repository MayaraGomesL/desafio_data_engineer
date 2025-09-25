from __future__ import annotations
from typing import Iterable, Optional
from pathlib import Path
from typing import TYPE_CHECKING
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, lit

import config.variables as V
import config.schemas as S

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

def write_bronze_append(spark: SparkSession, csv_paths: Iterable[Path], path_bronze: str, sep: str, encoding: str, schema: Optional["StructType"] = None, ano_mes: Optional[str] = None) -> None:
    """
    Lê os arquivos CSV e faz append na tabela Delta da camada Bronze.
    Adiciona colunas de metadados `dataCarga` e `ano_mes`.
    """
    # Lê e faz union de todos os CSVs
    df_union = None
    for p in csv_paths:
        df = spark.read.csv(str(p), header=False, sep=sep, encoding=encoding, schema=schema)
        df_union = df if df_union is None else df_union.unionByName(df)
    if df_union is None:
        raise ValueError("Nenhum CSV encontrado para realizar a ingestão na Bronze.")
    
    # Adiciona metadados
    df_bronze = (
        df_union
        .withColumn("dataCarga", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))
        .withColumn("ano_mes", lit(ano_mes))
    )

    # writer - partitionBy apenas se o caminho não já tiver ano_mes embutido
    writer = df_bronze.write.format("delta").mode("append")
    caminho_tem_particao_embutida = "ano_mes=" in path_bronze
    if not caminho_tem_particao_embutida:
        writer = writer.partitionBy("ano_mes")

    writer.save(path_bronze)