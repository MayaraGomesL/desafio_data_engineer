from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin, urlparse

import config.variables as V
from src.utils import utils as H

logger = logging.getLogger(__name__)

def listar_meses(s: Optional[object] = None) -> List[str]:
    """Lista as pastas 'YYYY-MM/' existentes no índice da Receita Federal."""
    sess = s or H.http_session()
    links = H.get_html_links(V.RF_BASE_URL, sess)
    meses = []
    for href in links:
        m = re.search(r"(\d{4}-\d{2})/?$", href.rstrip("/"))
        if m:
            meses.append(m.group(1))
    return sorted(set(meses))


def listar_zips_por_mes(ano_mes: str, s: Optional[object] = None) -> Tuple[List[str], List[str]]:
    """Lista os nomes dos arquivos .zip e os separa em empresas e sócios."""
    sess = s or H.http_session()
    mes_url = urljoin(V.RF_BASE_URL, f"{ano_mes}/")
    logger.info("Listando zips em: %s", mes_url)
    links = H.get_html_links(mes_url, sess)

    nomes = []
    for l in links:
        p = urlparse(l)
        nome = Path(p.path).name
        if nome:
            nomes.append(nome)

    empresas = [n for n in nomes if re.fullmatch(r"(?i)Empresas[\w-]*\.zip", n)]
    socios = [n for n in nomes if re.fullmatch(r"(?i)Socios[\w-]*\.zip", n)]

    logger.info("Empresas disponíveis (%d): %s", len(empresas), ", ".join(empresas) or "nenhum")
    logger.info("Sócios disponíveis (%d): %s",   len(socios),   ", ".join(socios)   or "nenhum")
    return empresas, socios


def download_arquivo_local(url: str, output_path: Path, s: Optional[object] = None) -> Path:
    """
    Baixa arquivo da web para o sistema de arquivos local.
    """
    sess = s or H.http_session()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Baixando: %s -> %s", url, output_path)

    with sess.get(url, timeout=getattr(sess, "request_timeout", 60), stream=True) as r:
        r.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    f.write(chunk)

    logger.info("Download concluído: %s", output_path)
    return output_path


def download_raw_data(ano_mes: str, empresas_zip: str, socios_zip: str) -> Dict[str, list[Path]]:
    """
    Orquestra o download e a descompactação dos arquivos brutos para o disco local.
    Realiza a validação de caminhos e retorna erros claros.
    """
    sess = H.http_session()

    # 1) valida mês
    meses_disponiveis = listar_meses(sess)
    if ano_mes not in meses_disponiveis:
        raise ValueError(
            f"ERRO: A pasta '{ano_mes}' não foi encontrada. "
            f"Pastas disponíveis: {', '.join(meses_disponiveis)}"
        )

    # 2) valida zips
    empresas_disp, socios_disp = listar_zips_por_mes(ano_mes, sess)
    if empresas_zip not in empresas_disp:
        raise ValueError(
            f"ERRO: O arquivo '{empresas_zip}' não foi encontrado. "
            f"Arquivos de Empresas disponíveis: {', '.join(empresas_disp) or 'nenhum encontrado'}"
        )
    if socios_zip not in socios_disp:
        raise ValueError(
            f"ERRO: O arquivo '{socios_zip}' não foi encontrado. "
            f"Arquivos de Sócios disponíveis: {', '.join(socios_disp) or 'nenhum encontrado'}"
        )

    # 3) URLs finais
    mes_url = urljoin(V.RF_BASE_URL, f"{ano_mes}/")
    emp_url = urljoin(mes_url, empresas_zip)
    soc_url = urljoin(mes_url, socios_zip)
    logger.info("URLs finais: Empresas=%s | Sócios=%s", emp_url, soc_url)


    # 5) download + unzip
    emp_zip_local = download_arquivo_local(emp_url, V.ZIPS_PATH/empresas_zip, sess)
    soc_zip_local = download_arquivo_local(soc_url, V.ZIPS_PATH/socios_zip, sess)

    logger.info("Descompactando: %s", emp_zip_local)
    emp_csvs = H.unzip_csvs(emp_zip_local, V.CSV_EMPRESAS_PATH)
    logger.info("CSV Empresas extraídos (%d) em %s", len(emp_csvs), V.CSV_EMPRESAS_PATH)

    logger.info("Descompactando: %s", soc_zip_local)
    soc_csvs = H.unzip_csvs(soc_zip_local, V.CSV_SOCIOS_PATH)
    logger.info("CSV Sócios extraídos (%d) em %s", len(soc_csvs), V.CSV_SOCIOS_PATH)

    return {"empresas_csvs": emp_csvs, "socios_csvs": soc_csvs}