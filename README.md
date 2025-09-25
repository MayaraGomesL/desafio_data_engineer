# 📦 Desafio Data Engineer — Pipeline (Bronze → Silver → Gold)

Pipeline de ingestão e transformação dos Dados Abertos do CNPJ (Receita Federal) com PySpark + Delta Lake, orquestrado por Docker Compose e carga final no PostgreSQL.

---

## 🧭 Sumário

- [Visão Geral](#visão-geral)
- [Estrutura de Pastas](#estrutura-de-pastas)
- [Pré-requisitos](#pré-requisitos)
- [Como Rodar](#como-rodar)
- [Comandos Úteis](#comandos-úteis)
- [Configurações Importantes](#configurações-importantes)
- [Limpeza / Reset](#limpeza--reset)
- [Troubleshooting](#troubleshooting)
- [Licença & Fontes](#licença--fontes)

---

## ✨ Visão Geral

- **Fonte:** Índice público da Receita Federal (dados abertos do CNPJ), via HTTP usando `requests` + `BeautifulSoup`.
- **Raw:** Zips baixados e extraídos localmente (arquivos `...EMPRECSV`, `...SOCIOCSV`).
- **Bronze (Delta):** Dados brutos com schema aplicado e partição por `ano_mes`.
- **Silver (Delta):** Normalização (tipos, limpeza, padronizações) e deduplicação antes do MERGE (upsert).
- **Gold:** Agregação final (`gold_flat_emp_socios`) e escrita direta no PostgreSQL.

---

## 📁 Estrutura de Pastas


```
.
├─ config/
│  ├─ variables.py          # caminhos, JDBC, nomes de tabelas
│  └─ schemas.py            # schemas das tabelas Bronze e Silver
├─ src/
│  ├─ pipeline.py           # orquestra o fluxo end-to-end
│  ├─ utils/
│  │  ├─ spark_session.py   # get_spark() com Delta + driver JDBC
│  │  ├─ utils.py           # helpers: http_session, get_html_links, unzip, metadados, read_delta
│  │  └─ download_html.py   # listar meses/zips, validar, baixar e extrair
│  ├─ bronze/
│  │  └─ bronze_ingestion.py   # write_bronze_append (CSV → Delta Bronze)
│  ├─ silver/
│  │  └─ silver_ingestion.py   # normalizações + write_silver_merge (dropDuplicates + merge)
│  └─ gold/
│     ├─ transform_gold.py     # criar_gold_flat_emp_socios
│     └─ load_gold.py          # write_gold_to_postgres
├─ data/                    # Armazenamento local (não versionado)
│  ├─ bronze/
│  ├─ silver/
│  ├─ gold/
│  └─ work/
│     └─ zips/              # zips baixados
├─ Dockerfile
├─ docker-compose.yml
├─ requirements.txt
└─ README.md
```

---

## ✅ Pré-requisitos

- Docker + Docker Compose
- Conexão de rede (para baixar os arquivos da RFB)

---

## ▶️ Como Rodar

### 1) Build (opcional) e execução do pipeline

**Build da imagem:**
```sh
docker compose build
```

**Rodar pipeline (one-shot):**
```sh
# Sintaxe:
docker compose run --rm app python -m src.pipeline <YYYY-MM> <EMP_ID> <SOC_ID>

# Exemplo (baixa Empresas1.zip e Socios1.zip do mês 2025-09):
docker compose run --rm app python -m src.pipeline 2025-09 1 1
```
> `EMP_ID` / `SOC_ID` correspondem aos sufixos dos zips listados no índice (ex.: Empresas1.zip / Socios1.zip). Caso o ano-mes solicitado ou Empresas/Socios não seja localizado lista pastas disponíveis para download.

---

## 🔎 Comandos Úteis

### Ver Bronze (exemplo empresas do mês)
```sh
docker compose run --rm app python - <<'PY'
from pyspark.sql import SparkSession as S
s = S.builder.getOrCreate()
df = s.read.format("delta").load("/app/data/bronze/empresas")
df.where("ano_mes='2025-09'").show(20, False)
PY
```

### Exportar amostra da Silver (500 linhas) para CSV
```sh
docker compose run --rm app python - <<'PY'
import os, glob, shutil
from pyspark.sql import SparkSession as S
s = S.builder.getOrCreate()
df = s.read.format("delta").load("/app/data/silver/socios").limit(500)
tmp = "/app/data/samples/_silver_socios_head500"
out = "/app/data/samples/silver_socios_head500.csv"
(df.coalesce(1).write.mode("overwrite").option("header","true").csv(tmp))
os.makedirs("/app/data/samples", exist_ok=True)
part = glob.glob(os.path.join(tmp,"part-*.csv"))[0]
shutil.move(part, out)
shutil.rmtree(tmp)
print("Arquivo gerado:", out)
PY
```

### Consultar GOLD no Postgres
```sh
docker compose exec -it db psql -U postgres -d ml_data_engineer_sql -c "SELECT * FROM gold_flat_emp_socios LIMIT 10;"
```

---

## 🧹 Limpeza / Reset

### Limpar dados do data lake (ATENÇÃO: remove bronze/silver/gold e temporários)
```sh
docker compose run --rm app bash -lc "rm -rf /app/data/bronze /app/data/silver /app/data/gold /app/data/work/* && mkdir -p /app/data/work/zips"
```

### Resetar tabela GOLD no Postgres
```sh
docker compose exec -it db psql -U postgres -d ml_data_engineer_sql -c "DROP TABLE IF EXISTS gold_flat_emp_socios;"
```

---

## 🛠️ Troubleshooting

- **Failed to find the data source: delta**  
  Falta o pacote do Delta no Spark. Garanta que o `spark.jars.packages` inclui `io.delta:delta-spark_2.13:4.0.0` e que a `SparkSession` habilita a extensão `DeltaSparkSessionExtension`.

- **java.lang.ClassNotFoundException: org.postgresql.Driver**  
  Inclua `org.postgresql:postgresql:42.7.3` em `spark.jars.packages` ou adicione o JAR ao classpath.

---

## 📝 Licença & Fontes

**Dados:** Receita Federal do Brasil — Dados Abertos do CNPJ.
Este repositório tem caráter não oficial.
