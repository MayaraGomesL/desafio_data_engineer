from pathlib import Path

# =========================
# Variáveis de Ambiente e Caminhos
# =========================

# -------- PostgreSQL --------
PG_HOST = "db"
PG_PORT = 5432
PG_DB = "ml_data_engineer_sql"
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_GOLD_TABLE = "gold_flat_emp_socios"


# -------- Fonte (Receita Federal) --------
RF_BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

# =========================
# Caminhos do Data Lake (Armazenamento Local)
# =========================

DATA_LAKE_PATH = Path("/app/data")

# Camadas do Data Lake
BRONZE_PATH = DATA_LAKE_PATH / "bronze"
SILVER_PATH = DATA_LAKE_PATH / "silver"
GOLD_PATH = DATA_LAKE_PATH / "gold"

# Diretórios temporários para o pipeline
WORK_PATH = DATA_LAKE_PATH / "work"
ZIPS_PATH = WORK_PATH / "zips"
CSV_EMPRESAS_PATH = WORK_PATH / "csv_empresas"
CSV_SOCIOS_PATH = WORK_PATH / "csv_socios"

# Tabelas Delta
TBL_EMPRESAS = "empresas"
TBL_SOCIOS = "socios"
TBL_GOLD_FLAT_EMP_SOCIOS = "gold_flat_emp_socios"

# -------- SparkSession --------
SPARK_APP_NAME = "ML-Data-Engineer-SparkSession"
