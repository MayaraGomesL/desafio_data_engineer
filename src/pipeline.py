from __future__ import annotations
import shutil
import logging, os, sys
from pathlib import Path

import config.variables as V
import config.schemas as S

from src.utils.spark_session import get_spark
from src.utils.download_html import download_raw_data
from src.bronze.bronze_ingestion import write_bronze_append
from src.silver.silver_ingestion import normalizar_empresas_silver, normalizar_socios_silver, write_silver_merge
from src.gold.transform_gold import criar_gold_flat_emp_socios
from src.utils.utils import read_delta
from src.gold.load_gold import write_gold_to_postgres

def setup_logging():
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True
    )


def main(ano_mes: str, empresas_id: int, socios_id: int):

    setup_logging()
    log = logging.getLogger("pipeline")
    log.info("Iniciando pipeline para %s (emp_id=%s, soc_id=%s)", ano_mes, empresas_id, socios_id)

    # Criação das pastas do data lake, se não existirem
    for p in [V.DATA_LAKE_PATH, V.BRONZE_PATH, V.SILVER_PATH, V.GOLD_PATH,
              V.WORK_PATH, V.ZIPS_PATH, V.CSV_EMPRESAS_PATH, V.CSV_SOCIOS_PATH]:
        Path(p).mkdir(parents=True, exist_ok=True)

    # 0) SparkSession
    spark = get_spark()

    # 1)  Faz o download dos arquivos brutos e descompacta os CSVs
    try:
        raw_files = download_raw_data(ano_mes=ano_mes, empresas_zip=f"Empresas{empresas_id}.zip", socios_zip=f"Socios{socios_id}.zip")
    except ValueError as e:
        print(e)
        return

    
    # 2) BRONZE — append particionado por ano_mes
    bronze_empresas_path = V.BRONZE_PATH /V.TBL_EMPRESAS
    bronze_socios_path = V.BRONZE_PATH/V.TBL_SOCIOS
    write_bronze_append(spark, raw_files["empresas_csvs"], str(bronze_empresas_path), ';', 'utf-8', S.BRONZE_SCHEMA_EMPRESAS, ano_mes)
    write_bronze_append(spark, raw_files["socios_csvs"], str(bronze_socios_path), ';', 'utf-8', S.BRONZE_SCHEMA_SOCIOS, ano_mes)
    log.info("2) BRONZE - Gravada em %s e %s", bronze_empresas_path, bronze_socios_path)


    # Limpa arquivos CSV temporários após a ingestão. Somente ZIPs serão mantidos.
    shutil.rmtree(V.CSV_EMPRESAS_PATH, ignore_errors=True)
    shutil.rmtree(V.CSV_SOCIOS_PATH, ignore_errors=True)
    print("Arquivos CSV temporários removidos com sucesso.")

    # 3) SILVER — ler Bronze, normalizar e fazer MERGE (upsert)
    df_emp_bronze = read_delta(spark, str(bronze_empresas_path))
    df_soc_bronze = read_delta(spark, str(bronze_socios_path))

    df_emp_silver = normalizar_empresas_silver(df_emp_bronze, ano_mes)
    df_soc_silver = normalizar_socios_silver(df_soc_bronze, ano_mes)

    silver_empresas_dir = V.SILVER_PATH/V.TBL_EMPRESAS
    silver_socios_dir   = V.SILVER_PATH/V.TBL_SOCIOS

    write_silver_merge(spark=spark, df_source=df_emp_silver, path_silver=str(silver_empresas_dir), key_columns=["cnpj"], particao="ano_mes")
    write_silver_merge(spark=spark, df_source=df_soc_silver, path_silver=str(silver_socios_dir), key_columns=["cnpj", "documento_socio", "tipo_socio"], particao="ano_mes")

    log.info("3) SILVER - Atualizada em %s e %s", silver_empresas_dir, silver_socios_dir)

    # 4) GOLD — ler Silver, agregar e exportar para Postgres
    gold_path = V.GOLD_PATH/V.TBL_GOLD_FLAT_EMP_SOCIOS
    gold_delta_path = criar_gold_flat_emp_socios(spark, str(silver_empresas_dir), str(silver_socios_dir), str(gold_path))

    # Export Gold para Postgres
    df_gold = read_delta(spark, gold_delta_path)
    write_gold_to_postgres(df_gold=df_gold, tabela_final=V.PG_GOLD_TABLE)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Uso: python src/pipeline.py <ano_mes:YYYY-MM> <empresas_id:int> <socios_id:int>")
        sys.exit(1)
    ano_mes, emp_id, soc_id = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    main(ano_mes, emp_id, soc_id)