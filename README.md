📦 Dasafio Data Engineer → Pipeline — Bronze → Silver → Gold (Delta + Spark + Postgres)

Pipeline de ingestão e transformação dos Dados Abertos do CNPJ (Receita Federal) usando PySpark e Delta Lake, orquestrado via Docker Compose, com persistência em camadas (Bronze/Silver/Gold) e carga final no PostgreSQL.

✨ Visão Geral

Fonte: índice público da Receita Federal (dados_abertos_cnpj), download via HTTP (requests + BeautifulSoup).

Raw (arquivos): zips baixados e extraídos localmente (arquivos como: ...EMPRECSV, ...SOCIOCSV).

Bronze (Delta): camada de dados bruta, sem transformação. Ingestão particionada por ano_mes, schema aplicado, armazenamento em /app/data/bronze.

Silver (Delta): normalização (tipos, limpeza, padronizações) e deduplicação simples por chave antes do MERGE. Armazenamento em /app/data/silver.

Gold (Delta): agregação final (gold_flat_emp_socios) e carga no PostgreSQL.

📁 Estrutura de Pastas
.
├─ config/
│  ├─ variables.py          # caminhos, JDBC, nomes de tabelas
│  └─ schemas.py            # schemas das tabelas Bronze e Silver
├─ src/
│  ├─ pipeline.py           # orquestra o fluxo end-to-end
│  ├─ utils/
│  │  ├─ spark_session.py   # get_spark() com Delta e driver JDBC
│  │  ├─ utils.py           # helpers (HTTP session, unzip, read_delta, metadados)
│  │  └─ download_html.py   # listar meses/zips, baixar e extrair
│  ├─ bronze/
│  │  └─ bronze_ingestion.py   # write_bronze_append
│  ├─ silver/
│  │  └─ silver_ingestion.py   # normalizações + write_silver_merge (dropDuplicates)
│  └─ gold/
│     ├─ transform_gold.py     # criar_gold_flat_emp_socios
│     └─ load_gold.py          # write_gold_to_postgres
├─ data/                    # Armazenamento dos dados
│  ├─ bronze/
│  ├─ silver/
│  ├─ gold/
│  └─ work/                 # zips, extrações temporárias
├─ jars/
│  └─ postgresql-42.7.3.jar # driver JDBC
├─ Dockerfile
├─ docker-compose.yml
├─ requirements.txt
└─ README.md

🧱 Camadas de Dados
RAW (arquivos)

Arquivos típicos: K3241.K03200Y1.D50913.EMPRECSV, ...SOCIOCSV (sem extensão .csv).

A função de unzip já seleciona/renomeia para .csv ou retorna todos os arquivos para leitura Spark com .format("csv").

Bronze (Delta)
Local: /app/data/bronze/{empresas|socios}.
Particionamento: ano_mes (ex.: ano_mes=2025-09).
Esquemas aplicados a partir de config/schemas.py.

Silver (Delta)
Local: /app/data/silver/{empresas|socios}.
Normalizações:
- cnpj: limpeza (remover não dígitos), empty_as_null, upper, regexp_replace, casts.
- colunas específicas por domínio (empresas/sócios).
Deduplicação simples antes do MERGE:
- df_source = df_source.dropDuplicates(key_columns)

Chaves:
Empresas: ["cnpj"]
Sócios: ["cnpj", "documento_socio","tipo_socio"]

MERGE (upsert) para garantir atualização incremental.

Gold (Delta)
Local: /app/data/gold/gold_flat_emp_socios.

▶️ Como Rodar
0) Pré-requisitos
Docker + Docker Compose
Rede ativa (para baixar dados da RFB)

1) Rodar o pipeline (um-shot)
# sintaxe
docker compose run --rm app python -m src.pipeline <YYYY-MM> <EMP_ID> <SOC_ID>

# exemplo
docker compose run --rm app python -m src.pipeline 2025-09 1 1

2) Ver dados rapidamente
# Bronze empresas do mês
docker compose run --rm app python - <<'PY'
from pyspark.sql import SparkSession as S
s=S.builder.getOrCreate()
df=s.read.format("delta").load("/app/data/bronze/empresas")
df.where("ano_mes='2025-09'").show(20, False)
PY

3) Acessar PostgreSQL
# docker compose exec -it db psql -U postgres -d ml_data_engineer_sql 
SELECT * FROM gold_flat_emp_socios LIMIT 10;


📝 Licença & Fontes
Dados: Receita Federal do Brasil — Dados Abertos do CNPJ.
