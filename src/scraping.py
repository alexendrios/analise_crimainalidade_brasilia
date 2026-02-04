from util.log import logs
from util.config_loader import get_config

import re
import requests
import pandas as pd

from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logs()
config = get_config()

# =========================
# Constantes
# =========================
URL_LISTA_RA: str = config["coleta"]["fontes"]["dados_wikpedia"]["url_1"]
URLS_RA_INDIVIDUAIS: list[str] = config["coleta"]["fontes"]["dados_wikpedia"][
    "urls_individuais"
]

COL_RA = "região administrativa"
COL_POP = "população"
OUTPUT_PATH = "./data/csv/ra_df_populacao.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9",
}


# =========================
# Sessão HTTP com retry
# =========================
def criar_sessao() -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.headers.update(HEADERS)
    session.mount("https://", adapter)

    return session


SESSION = criar_sessao()


# =========================
# Scraping lista de RAs
# =========================
def obter_tabela_ra_populacao(url: str) -> pd.DataFrame:
    logger.info("Iniciando scraping da lista de RAs", extra={"url": url})

    response = SESSION.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    tabela = soup.find("table", class_="wikitable")
    if not tabela:
        raise RuntimeError("Tabela wikitable não encontrada")

    linhas = tabela.find_all("tr")

    cabecalho = [th.get_text(strip=True).lower() for th in linhas[0].find_all("th")]
    dados: list[list[str]] = []

    for linha in linhas[1:]:
        colunas = [
            td.get_text(strip=True).replace("\u00a0", "") for td in linha.find_all("td")
        ]
        if len(colunas) == len(cabecalho):
            dados.append(colunas)

    df = pd.DataFrame(dados, columns=cabecalho)

    logger.info("Total de registros extraídos (lista)", extra={"total": len(df)})
    return df


# =========================
# Scraping RA individual
# =========================
def obter_populacao_ra_individual(url: str) -> pd.DataFrame:
    logger.info("Iniciando scraping de RA individual", extra={"url": url})

    response = SESSION.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Nome da RA
    titulo = soup.select_one("h1#firstHeading")
    nome_ra = (
        re.sub(r"\s*\(.*?\)", "", titulo.get_text(strip=True)).strip()
        if titulo
        else "Desconhecida"
    )

    # Rótulo "População"
    pop_label = soup.find(string=re.compile(r"^\s*População\s*$"))
    if not pop_label:
        raise RuntimeError("Rótulo 'População' não encontrado")

    linha_rotulo = pop_label.find_parent("tr")
    if not linha_rotulo:
        raise RuntimeError("Linha do rótulo 'População' não encontrada")

    # Valor está na linha seguinte (- Total)
    linha_valor = linha_rotulo.find_next_sibling("tr")
    if not linha_valor:
        raise RuntimeError("Linha do valor da população não encontrada")

    texto_pop = linha_valor.get_text(" ", strip=True)

    if "- Total" not in texto_pop:
        logger.warning(
            "Linha inesperada para população",
            extra={"ra": nome_ra, "texto": texto_pop, "url": url},
        )

    # Regex tolerante
    match = re.search(r"\d[\d\. ]*", texto_pop)

    if not match:
        logger.warning(
            "População não numérica encontrada",
            extra={"ra": nome_ra, "texto": texto_pop, "url": url},
        )
        populacao = None
    else:
        valor = re.sub(r"[^\d]", "", match.group())
        populacao = valor if valor else None

    logger.info(
        "Resultado scraping RA",
        extra={"ra": nome_ra, "populacao": populacao, "url": url},
    )

    return pd.DataFrame(
        [[nome_ra, populacao]],
        columns=[COL_RA, COL_POP],
    )


# =========================
# Normalização
# =========================
def normalizar_df(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Normalizando DataFrame")

    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    colunas_esperadas = {COL_RA, COL_POP}
    
    if not colunas_esperadas.issubset(df.columns):
        raise ValueError(f"Colunas inválidas encontradas: {df.columns}")

    df = df[list(colunas_esperadas)]


    df[COL_RA] = df[COL_RA].astype(str).str.strip()


    df[COL_POP] = (
        df[COL_POP]
        .astype(str)
        .str.replace(r"[^\d]", "", regex=True)
        .replace("", pd.NA)
        .astype("Int64")
    )

    return df


# =========================
# Pipeline principal
# =========================
def obter_dados_ra_populacao() -> None:
    logger.info("===== INÍCIO DO PROCESSO =====")

    df_lista = normalizar_df(obter_tabela_ra_populacao(URL_LISTA_RA))

    dfs_individuais: list[pd.DataFrame] = []

    for url in URLS_RA_INDIVIDUAIS:
        try:
            df_tmp = normalizar_df(obter_populacao_ra_individual(url))
            dfs_individuais.append(df_tmp)

            ra = df_tmp[COL_RA].iloc[0]
            if ra not in df_lista[COL_RA].values:
                logger.warning(
                    "RA individual não encontrada na lista oficial",
                    extra={"ra": ra},
                )

        except Exception:
            logger.exception(
                "Erro ao processar RA individual",
                extra={"url": url},
            )

    df_individual = (
        pd.concat(dfs_individuais, ignore_index=True)
        if dfs_individuais
        else pd.DataFrame(columns=[COL_RA, COL_POP])
    )

    df_final = (
        pd.concat([df_lista, df_individual], ignore_index=True)
        .drop_duplicates(subset=[COL_RA])
        .sort_values(COL_RA)
        .reset_index(drop=True)
    )

    df_final.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    logger.info(
        "Processo finalizado com sucesso",
        extra={"arquivo": OUTPUT_PATH, "total": len(df_final)},
    )

    logger.info("===== FIM DO PROCESSO =====")
