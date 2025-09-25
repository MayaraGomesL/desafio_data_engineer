📦 CNPJ Pipeline — Bronze → Silver → Gold (Delta + Spark + Postgres)

Pipeline de ingestão e transformação dos Dados Abertos do CNPJ (Receita Federal) usando PySpark e Delta Lake, orquestrado via Docker Compose, com persistência em camadas (Bronze/Silver/Gold) e carga final no PostgreSQL.

Se a imagem não aparecer, mova o arquivo arquitetura-projeto.png baixado para docs/ e commite. Como fallback, há também um diagrama Mermaid no final do README.

✨ Visão Geral

Fonte: índice público da RFB (dados_abertos_cnpj), varrido via HTTP (requests + BeautifulSoup).

Raw (arquivos): zips baixados e extraídos localmente (arquivos como ...EMPRECSV, ...SOCIOCSV).

Bronze (Delta): ingestão particionada por ano_mes, schema aplicado, armazenamento em /app/data/bronze.

Silver (Delta): normalização (tipos, limpeza, padronizações) e deduplicação simples por chave antes do MERGE.

Gold (Delta): agregação final (gold_flat_emp_socios) e carga no PostgreSQL (staging → upsert na final).

📁 Estrutura de Pastas (essencial)
.
├─ config/
│  ├─ variables.py          # caminhos, JDBC, nomes de tabelas e SQLs (DDL/UPSERT)
│  └─ schemas.py            # schemas das tabelas Bronze
├─ src/
│  ├─ pipeline.py           # orquestra o fluxo end-to-end
│  ├─ utils/
│  │  ├─ spark_session.py   # get_spark() com Delta via pip + driver JDBC
│  │  ├─ utils.py           # helpers (HTTP session, unzip, read_delta, metadados)
│  │  └─ download_html.py   # listar meses/zips, baixar e extrair
│  ├─ bronze/
│  │  └─ bronze_ingestion.py   # write_bronze_append (idempotente por partição)
│  ├─ silver/
│  │  └─ silver_ingestion.py   # normalizações + write_silver_merge (dropDuplicates)
│  └─ gold/
│     ├─ transform_gold.py     # criar_gold_flat_emp_socios(...)
│     └─ load_gold.py          # write_gold_to_postgres(...)
├─ data/                    # gerado em runtime (não versionar)
│  ├─ bronze/
│  ├─ silver/
│  ├─ gold/
│  └─ work/                 # zips, extrações temporárias
├─ jars/
│  └─ postgresql-42.7.3.jar # driver JDBC (recomendado via spark.jars)
├─ Dockerfile
├─ docker-compose.yml
├─ requirements.txt
└─ README.md


Dica: adicione data/ ao .gitignore para não versionar os arquivos.

🧱 Camadas de Dados
RAW (arquivos)

Local: WORK_PATH → /app/data/work (zips em zips/, extrações em csv_empresas/ e csv_socios/).

Arquivos típicos: K3241.K03200Y1.D50913.EMPRECSV, ...SOCIOCSV (sem extensão .csv).

A função de unzip já seleciona/renomeia para .csv ou retorna todos os arquivos para leitura Spark com .format("csv").

Bronze (Delta)

Local: /app/data/bronze/{empresas|socios}.

Particionamento: ano_mes (ex.: ano_mes=2025-09).

Esquemas aplicados a partir de config/schemas.py.

Idempotente por partição: recomendado escrever com

.mode("overwrite").option("replaceWhere", f"ano_mes='{ano_mes}'")


para reprocessamentos do mesmo mês sem duplicar.

Silver (Delta)

Local: /app/data/silver/{empresas|socios}.

Normalizações:

cnpj: limpeza (remover não dígitos), empty_as_null, upper, regexp_replace, casts.

colunas específicas por domínio (empresas/sócios).

Deduplicação simples antes do MERGE:

df_source = df_source.dropDuplicates(key_columns)


Chaves típicas:

Empresas: ["cnpj"]

Sócios: ["cnpj", "documento_socio"]

MERGE (upsert) para garantir atualização incremental.

Gold (Delta)

Local: /app/data/gold/gold_flat_emp_socios (exemplo).

criar_gold_flat_emp_socios(...) combina dimensões/atributos para consumo analítico.

Carga no PostgreSQL:

Staging via JDBC (mode("overwrite").option("truncate","true")).

Upsert na final com psycopg2 (DDL_FINAL + UPSERT_SQL definidos em variables.py).

▶️ Como Rodar
0) Pré-requisitos

Docker + Docker Compose

Rede ativa (para baixar dados da RFB)

Driver JDBC do Postgres disponível:

Baixe para o projeto: ./jars/postgresql-42.7.3.jar

O get_spark() já aponta para /app/jars/postgresql-42.7.3.jar via spark.jars

1) Subir só o banco (opcional)
docker compose up -d db

2) Rodar o pipeline (um-shot)
# sintaxe
docker compose run --rm app python -m src.pipeline <YYYY-MM> <EMP_ID> <SOC_ID>

# exemplo
docker compose run --rm app python -m src.pipeline 2025-09 7 7


Dica: o compose também aceita command com variáveis:
command: ["python", "src/pipeline.py", "${ANO_MES:-2025-09}", "7", "7"]

3) Ver dados rapidamente
# Bronze empresas do mês
docker compose run --rm app python - <<'PY'
from pyspark.sql import SparkSession as S
s=S.builder.getOrCreate()
df=s.read.format("delta").load("/app/data/bronze/empresas")
df.where("ano_mes='2025-09'").show(20, False)
PY

4) Histórico Delta
docker compose run --rm app python - <<'PY'
from delta.tables import DeltaTable
from pyspark.sql import SparkSession as S
s=S.builder.getOrCreate()
DeltaTable.forPath(s, "/app/data/bronze/empresas").history().show(50, False)
PY

⚙️ Configuração
config/variables.py (exemplo)
from pathlib import Path

# Data Lake
DATA_LAKE_PATH = Path("/app/data")
BRONZE_PATH = DATA_LAKE_PATH / "bronze"
SILVER_PATH = DATA_LAKE_PATH / "silver"
GOLD_PATH   = DATA_LAKE_PATH / "gold"

# Temporários
WORK_PATH        = DATA_LAKE_PATH / "work"
ZIPS_PATH        = WORK_PATH / "zips"
CSV_EMPRESAS_PATH= WORK_PATH / "csv_empresas"
CSV_SOCIOS_PATH  = WORK_PATH / "csv_socios"

# Fonte RFB
RF_BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

# JDBC / Postgres
PG_HOST="db"; PG_PORT=5432; PG_DB="ml_data_engineer_sql"
PG_USER="postgres"; PG_PASSWORD="postgres"
PG_GOLD_TABLE="gold_flat_emp_socios"
# opcional: PG_JDBC_URL="jdbc:postgresql://db:5432/ml_data_engineer_sql"

# SQLs (exemplos)
DDL_FINAL = """
CREATE TABLE IF NOT EXISTS gold_flat_emp_socios (
  cnpj TEXT PRIMARY KEY,
  razao_social TEXT,
  tipo_socio INT,
  documento_socio TEXT,
  ano_mes TEXT
);
"""
UPSERT_SQL = """
INSERT INTO gold_flat_emp_socios (cnpj, razao_social, tipo_socio, documento_socio, ano_mes)
SELECT cnpj, razao_social, tipo_socio, documento_socio, ano_mes
FROM gold_flat_emp_socios_stg s
ON CONFLICT (cnpj)
DO UPDATE SET
  razao_social = EXCLUDED.razao_social,
  tipo_socio = EXCLUDED.tipo_socio,
  documento_socio = EXCLUDED.documento_socio,
  ano_mes = EXCLUDED.ano_mes;
"""

src/utils/spark_session.py (trecho chave)
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark():
    builder = (
        SparkSession.builder
        .appName("ml_data_engineer_spark")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        # driver JDBC local (recomendado e determinístico)
        .config("spark.jars", "/app/jars/postgresql-42.7.3.jar")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

🔄 Idempotência e Reprocessamento

Bronze: use mode("overwrite") + replaceWhere por ano_mes para reprocessar o mesmo mês sem duplicar.

Silver: dropDuplicates(key_columns) antes do MERGE para evitar conflitos.

Gold: staging overwrite + truncate, depois UPSERT para final.

🛠️ Problemas Comuns

“No suitable driver” na escrita JDBC
→ Garanta o JAR do Postgres no classpath (/app/jars/postgresql-42.7.3.jar + spark.jars).
Teste rápido:

s._sc._jvm.java.lang.Class.forName("org.postgresql.Driver")


Sem CSVs após unzip
→ Os arquivos da RFB não têm .csv. O unzip já lida com ...EMPRECSV/...SOCIOCSV.
Confira logs de extração.

Merge com múltiplas linhas por chave
→ Ative dropDuplicates por ["cnpj"] (empresas) e ["cnpj","documento_socio"] (sócios).

📝 Licença & Fontes

Dados: Receita Federal do Brasil — Dados Abertos do CNPJ.

Este repositório é apenas um pipeline de ingestão/transformação para fins educacionais e analíticos.