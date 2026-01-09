import os
import re
import pandas as pd
from pathlib import Path
from typing import Iterable, Sequence
from util.log import logs

logger = logs()

def extrair_ano(nome_arquivo: str) -> int:
    """
    Extrai o ano do nome do arquivo.

    Regras:
    - POP-00 -> 2001
    - Ano explícito (19xx / 20xx) -> usa o ano
    - Não encontrado -> assume 2010
    """
    nome = nome_arquivo.lower()

    if "pop-00" in nome:
        return 2001

    match = re.search(r"(19|20)\d{2}", nome)
    if match:
        return int(match.group())

    logger.warning(f"Ano não identificado no arquivo: {nome_arquivo}. Assumindo 2010.")
    return 2010

def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    return df


def filtrar_distrito_federal(df: pd.DataFrame) -> pd.DataFrame:
    valores_df = {"distrito federal", "df", "brasília", "brasilia"}

    for col in df.columns:
        if df[col].dtype == object:
            serie = df[col].astype(str).str.lower().str.strip()
            mask = serie.isin(valores_df)
            if mask.any():
                return df.loc[mask]

    logger.warning("Distrito Federal não encontrado")
    return df.iloc[0:0]


def encontrar_coluna_populacao(df: pd.DataFrame) -> str:
    # 1️⃣ Nome explícito
    for col in df.columns:
        if "popul" in col.lower():
            return col

    # 2️⃣ Fallback: maior número plausível
    for col in df.columns:
        serie = df[col].astype(str).str.replace(r"[^\d]", "", regex=True)
        nums = pd.to_numeric(serie, errors="coerce")

        if nums.notna().any() and nums.max() > 100_000:
            return col

    raise ValueError("Coluna de população não encontrada")


def limpar_populacao(valor) -> int | None:
    if pd.isna(valor):
        return None

    valor = re.sub(r"[^\d]", "", str(valor))
    return int(valor) if valor else None

def listar_arquivos_por_padrao(
    diretorio: str,
    padroes: Iterable[str],
    extensoes: Sequence[str] = (".xls", ".xlsx"),
) -> list[str]:
    """
    Lista arquivos em um diretório com base em padrões no nome e extensões permitidas.
    """
    caminho = Path(diretorio)

    if not caminho.exists() or not caminho.is_dir():
        return []

    arquivos: list[str] = []

    for path in caminho.iterdir():
        if not path.is_file():
            continue

        nome = path.name.lower()

        if path.suffix.lower() not in extensoes:
            continue

        if not any(p in nome for p in padroes):
            continue

        arquivos.append(str(path))

    return sorted(arquivos)

def listar_arquivos_populacao(diretorio: str) -> list[str]:
    padroes = ("pop", "populacao", "estimativa", "uf_")
    return listar_arquivos_por_padrao(diretorio, padroes)


def listar_arquivos_crimes(diretorio: str) -> list[str]:
    padroes = (
        "roubo",
        "racismo",
        "lesao",
        "latro",
        "injuria",
        "homi",
        "furto",
        "feminicidio",
        "crime",
    )
    return listar_arquivos_por_padrao(diretorio, padroes)


def processar_arquivo(caminho: str) -> pd.DataFrame:
    nome = os.path.basename(caminho)
    ano = extrair_ano(nome)

    df = pd.read_excel(caminho, header=2)
    df = normalizar_colunas(df)
    df = filtrar_distrito_federal(df)

    if df.empty:
        return df

    col_pop = encontrar_coluna_populacao(df)

    return pd.DataFrame(
        {
            "ano": [ano],
            "uf": ["DF"],
            "localidade": ["Distrito Federal"],
            "populacao": [limpar_populacao(df[col_pop].iloc[0])],
            "arquivo": [nome],
        }
    )


def processar_dados_crimes(caminho: str) -> pd.DataFrame:
    logger.info("Processando arquivo de crimes: %s", caminho)

    try:
        xls = pd.ExcelFile(caminho)

       
        sheet_index = 1 if len(xls.sheet_names) > 1 else 0
        header = 2 if sheet_index > 0 else 0

        df = pd.read_excel(
            xls,
            sheet_name=sheet_index,
            header=header,
        )

        df = normalizar_colunas(df)

        if df.empty:
            logger.warning("Arquivo de crimes sem dados válidos: %s", caminho)
            return pd.DataFrame()

        df["arquivo"] = os.path.basename(caminho)

        logger.info(
            "Arquivo de crimes processado com sucesso: %s (%d registros)",
            caminho,
            len(df),
        )

        return df

    except Exception as exc:
        logger.exception("Erro ao processar arquivo de crimes: %s", caminho)
        raise exc


def consolidar_historico(lista_arquivos: list[str]) -> pd.DataFrame:
    historico = []

    for arquivo in lista_arquivos:
        try:
            logger.info(f"Processando: {arquivo}")
            df = processar_arquivo(arquivo)

            if not df.empty:
                historico.append(df)

        except Exception as e:
            logger.error(f"Erro ao processar {arquivo}: {e}", exc_info=True)

    if not historico:
        return pd.DataFrame()

    return (
        pd.concat(historico, ignore_index=True)
        .sort_values("ano")
        .reset_index(drop=True)
    )
    
    
def salvar_historico_csv(
    df: pd.DataFrame,
    caminho: str ,
    separador: str = ";",
    encoding: str = "utf-8-sig",
) -> None:
    """
    Salva o DataFrame histórico em CSV.

    - Cria o diretório automaticamente, se não existir
    - Usa encoding UTF-8 com BOM (compatível com Excel)
    - Não salva o índice
    """
    if df.empty:
        logger.warning("DataFrame vazio. CSV não será gerado.")
        return

    path = Path(caminho)
    path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(
        path,
        sep=separador,
        encoding=encoding,
        index=False,
    )

    logger.info(f"Arquivo CSV salvo com sucesso em: {path}")
    
def processar_populacao():
    arquivos = listar_arquivos_populacao("./data/planilha")
    df_historico = consolidar_historico(arquivos)
    salvar_historico_csv(df_historico, "./data/csv/populacao_df_historico.csv")


def processar_crimes(caminho_planilhas: Path, caminho_saida: Path):
    arquivos = listar_arquivos_crimes(caminho_planilhas)
    arquivos = [a for a in arquivos if not Path(a).name.startswith("~$")]

    for arquivo in arquivos:
        nome_csv = Path(arquivo).stem + ".csv"
        df = processar_dados_crimes(arquivo)
        salvar_historico_csv(df, caminho_saida / nome_csv)   