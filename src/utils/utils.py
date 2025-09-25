from __future__ import annotations

from typing import TYPE_CHECKING
import requests
import zipfile
from pathlib import Path
from typing import List, Optional
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
from pyspark.sql.functions import (from_utc_timestamp, current_timestamp, lit, when, length, col, trim)


import config.variables as V

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

def bronze_path(tabela: str, ano_mes: str) -> str:
    """
    Constrói o caminho para a tabela Delta da camada Bronze.
    """
    return f"{V.BRONZE_PATH}/{tabela}/ano_mes={ano_mes}"
    
def silver_path(tabela: str) -> str:
    """
    Constrói o caminho para a tabela Delta da camada Silver.
    """
    return f"{V.SILVER_PATH}/{tabela}"

def gold_path(tabela: str) -> str:
    """
    Constrói o caminho para a tabela Delta da camada Gold.
    """
    return f"{V.GOLD_PATH}/{tabela}"

# =========================
# HTTP / HTML
# =========================

def http_session(timeout: int = 60) -> requests.Session:
    """Cria e retorna uma sessão HTTP com retry e timeout configurados."""
    s = requests.Session()
    retries = Retry(
        total=4, backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.request_timeout = timeout
    return s


def get_html_links(url: str, s: Optional[requests.Session] = None) -> List[str]:
    """Faz um GET na 'url' e retorna todos os 'href' encontrados."""
    sess = s or http_session()
    r = sess.get(url, timeout=getattr(sess, "request_timeout", 60))
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    return [
        urljoin(url, a["href"])
        for a in soup.find_all("a")
        if a.has_attr("href") and a["href"] != "../"
    ]

# =========================
# Arquivos / Delta
# =========================

def unzip_csvs(zip_file: Path, unzip_path: Path) -> list[Path]:
    """Descompacta o arquivo .zip e retorna a lista de arquivos .csv extraídos."""
    unzip_path.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_file, "r") as z:
        z.extractall(unzip_path)
    return [p for p in unzip_path.rglob("*") if p.is_file()]


def read_delta(spark: 'SparkSession', path: str) -> DataFrame:
    """
    Lê e retorna um DataFrame a partir de um caminho Delta.
    
    Args:
        spark: Sessão Spark
        path: Path do diretório Delta a ser lido.
    """
    return spark.read.format("delta").load(path)


# =========================
# Transforms
# =========================

def add_metadados_cols(df: DataFrame, ano_mes: str) -> DataFrame:
    """
    Adiciona metadados padrão a qualquer DataFrame:
      - dataCarga: timestamp do processamento
      - ano_mes : string 'YYYY-MM' (usada para partição no write)

    Args:
        df: DataFrame de entrada
        ano_mes: String com o mês e ano no formato 'YYYY-MM' para particionar a tabela Delta

    Returns:
        DataFrame com as colunas adicionais
    """
    
    return (
        df.withColumn("dataCarga", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))
          .withColumn("ano_mes", lit(ano_mes))
    )

def empty_as_null(c: str):
    """
    Converte string vazia em NULL após trim
    """
    return when(length(trim(col(c))) == 0, None).otherwise(trim(col(c)))