from util.log import logs
import pandas as pd
import unicodedata
from itertools import combinations

logger = logs()

def renomear_linha(dataset, coluna, registro_principal, registro_nomeado):
    logger.info("Iniciando renomeação de registros")

    if coluna not in dataset.columns:
        logger.error(f"Coluna '{coluna}' não encontrada no dataset")
        raise ValueError(f"Coluna '{coluna}' não existe")

    total_registros = dataset.shape[0]
    logger.info(f"Total de registros no dataset: {total_registros}")

    filtro = dataset[coluna] == registro_principal
    quantidade_afetada = filtro.sum()

    logger.info(
        f"Quantidade de registros encontrados com valor '{registro_principal}': {quantidade_afetada}"
    )

    if quantidade_afetada == 0:
        logger.warning("Nenhum registro encontrado para renomeação")
        return dataset

    dataset.loc[filtro, coluna] = registro_nomeado

    logger.info(f"Renomeação concluída: '{registro_principal}' → '{registro_nomeado}'")

    return dataset

def recriar_regiao_com_valor(
    df: pd.DataFrame,
    nome_regiao: str,
    coluna_regiao: str = "região_administrativa",
    coluna_ano: str = "ano",
    coluna_valor: str = "crimes_contra_mulher",
    valor_padrao: int | float = 0,
) -> pd.DataFrame:
    """
    Remove registros de uma região específica e recria
    a região para todos os anos existentes no dataset,
    preenchendo com um valor padrão.
    """

    logger.info("Iniciando recriação da região")
    logger.info(f"Região alvo: {nome_regiao}")

    for coluna in [coluna_regiao, coluna_ano, coluna_valor]:
        if coluna not in df.columns:
            logger.error(f"Coluna '{coluna}' não encontrada no DataFrame")
            raise ValueError(f"Coluna '{coluna}' não existe")

    total_inicial = df.shape[0]
    logger.info(f"Total de registros antes do processamento: {total_inicial}")

    anos = df[coluna_ano].unique()
    logger.info(f"Quantidade de anos únicos encontrados: {len(anos)}")

    registros_regiao = (df[coluna_regiao] == nome_regiao).sum()
    logger.info(f"Registros existentes da região '{nome_regiao}': {registros_regiao}")

    df_base = pd.DataFrame(
        {coluna_ano: anos, coluna_regiao: nome_regiao, coluna_valor: valor_padrao}
    )

    logger.info(
        f"Novo DataFrame base criado com {df_base.shape[0]} registros "
        f"(valor padrão = {valor_padrao})"
    )

    df_filtrado = df[df[coluna_regiao] != nome_regiao]
    logger.info(
        f"Registros após remoção da região '{nome_regiao}': {df_filtrado.shape[0]}"
    )

    df_final = pd.concat([df_base, df_filtrado], ignore_index=True)

    total_final = df_final.shape[0]
    logger.info(f"Total de registros após processamento: {total_final}")

    if total_final != df_filtrado.shape[0] + df_base.shape[0]:
        logger.warning(
            "Possível inconsistência no número de registros após concatenação"
        )

    logger.info("Recriação da região concluída com sucesso")
    return df_final

def remover_acentos(texto):
    try:
        if pd.isna(texto):
            logger.debug("Valor nulo encontrado ao remover acentos")
            return texto

        resultado = (
            unicodedata.normalize("NFKD", texto)
            .encode("ASCII", "ignore")
            .decode("utf-8")
        )

        logger.debug(f"Texto original: {texto} | Sem acento: {resultado}")
        return resultado

    except Exception:
        logger.error(f"Erro ao remover acentos do texto: {texto}", exc_info=True)
        raise

def normalizar_colunas(df):
    try:
        colunas_originais = list(df.columns)

        novas_colunas = [
            remover_acentos(str(col))
            .strip()
            .lower()
            for col in df.columns
        ]

        # limpeza adicional
        novas_colunas = [
            col.replace(" ", "_")
               .replace("-", "_")
            for col in novas_colunas
        ]

        # regex para limpeza mais robusta
        import re
        novas_colunas = [
            re.sub(r"[^\w]", "_", col) for col in novas_colunas
        ]

        novas_colunas = [
            re.sub(r"_+", "_", col).strip("_") for col in novas_colunas
        ]

        df.columns = novas_colunas

        logger.info("Colunas normalizadas com sucesso")
        logger.debug(f"Antes: {colunas_originais}")
        logger.debug(f"Depois: {novas_colunas}")

        return df

    except Exception:
        logger.exception("Erro ao normalizar nomes das colunas")
        raise
    
def padronizar_regiao(df, coluna):
    logger.info(f"Iniciando padronização da coluna: {coluna}")
    logger.debug(f"Shape inicial: {df.shape}")

    try:
        if coluna not in df.columns:
            logger.error(f"Coluna '{coluna}' não encontrada no DataFrame")
            raise ValueError(f"Coluna '{coluna}' não existe")

        nulos_antes = df[coluna].isna().sum()

        # 🔥 EVITA SettingWithCopyWarning
        df.loc[:, coluna] = (
            df[coluna]
            .astype("string")  # melhor que str (preserva NA)
            .str.strip()
            .str.upper()
            .apply(remover_acentos)
        )

        # Corrige "NAN" gerado na conversão
        df.loc[:, coluna] = df[coluna].replace("NAN", None)

        nulos_depois = df[coluna].isna().sum()

        logger.info(f"Padronização concluída para coluna: {coluna}")
        logger.debug(f"Nulos antes: {nulos_antes} | Nulos depois: {nulos_depois}")
        logger.debug(f"Valores únicos (amostra): {df[coluna].dropna().unique()[:10]}")
        logger.debug(f"Shape final: {df.shape}")

        return df

    except Exception:
        logger.exception(f"Erro ao padronizar coluna: {coluna}")
        raise

def transformar_wide_para_long(df, coluna_regiao, coluna_valor):
    logger.info("Iniciando transformação wide → long")
    logger.debug(f"Shape inicial: {df.shape}")

    try:
        df = df.melt(id_vars=coluna_regiao, var_name="ano", value_name=coluna_valor)
        logger.debug(f"Após melt: {df.shape}")

        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
        logger.debug(f"Valores nulos em 'ano': {df['ano'].isna().sum()}")

        df[coluna_valor] = (
            pd.to_numeric(df[coluna_valor], errors="coerce").fillna(0).astype("Int64")
        )
        logger.debug(
            f"Valores nulos em '{coluna_valor}' após tratamento: {df[coluna_valor].isna().sum()}"
        )


        df[coluna_regiao] = df[coluna_regiao].astype(str).str.strip().str.upper()

        linhas_antes = df.shape[0]
        df = df[df[coluna_regiao] != "DISTRITO FEDERAL"]
        linhas_depois = df.shape[0]

        logger.info(
            f"Removidas {linhas_antes - linhas_depois} linhas da região DISTRITO FEDERAL"
        )

        logger.info("Transformação concluída com sucesso")
        logger.debug(f"Shape final: {df.shape}")

        return df

    except Exception as e:
        logger.error("Erro durante transformação wide → long", exc_info=True)
        raise
    
def ordenar_por_ano(df, coluna="ano"):
    logger.info(f"Iniciando ordenação pelo campo: {coluna}")
    logger.debug(f"Shape inicial: {df.shape}")

    try:
        if coluna not in df.columns:
            logger.error(f"Coluna '{coluna}' não encontrada no DataFrame")
            raise ValueError(f"Coluna '{coluna}' não existe")

        # Verifica nulos antes
        nulos_antes = df[coluna].isna().sum()
        logger.debug(f"Nulos na coluna '{coluna}' antes da ordenação: {nulos_antes}")

        df_ordenado = df.sort_values(by=coluna).reset_index(drop=True)

        # Verifica nulos depois
        nulos_depois = df_ordenado[coluna].isna().sum()

        logger.info(f"Ordenação concluída pela coluna: {coluna}")
        logger.debug(f"Nulos após ordenação: {nulos_depois}")
        logger.debug(f"Shape final: {df_ordenado.shape}")

        return df_ordenado

    except Exception:
        logger.exception(f"Erro ao ordenar DataFrame pela coluna: {coluna}")
        raise

def comparar_datasets(m_datasets: dict, exibir=True):
    """
    Recebe um dicionário no formato:
    {
        "nome_dataset": dataframe,
        ...
    }
    """

    logger.info("[START] Comparação de datasets")

    try:
        resumo = []

        for nome, df in m_datasets.items():
            logger.info(f"Analisando dataset: {nome}")

            linhas, colunas = df.shape
            nulos_total = df.isnull().sum().sum()
            colunas_com_nulos = df.isnull().sum().gt(0).sum()

            logger.debug(f"{nome} | shape={df.shape}")
            logger.debug(f"{nome} | nulos_total={nulos_total}")

            resumo.append(
                {
                    "dataset": nome,
                    "linhas": linhas,
                    "colunas": colunas,
                    "nulos_total": int(nulos_total),
                    "colunas_com_nulos": int(colunas_com_nulos),
                }
            )

            if exibir:
                logger.info(f"📊 Dataset: {nome}")
                logger.info(
                    f"Linhas: {linhas} | Colunas: {colunas} | Nulos: {nulos_total}"
                )

                # Evita log gigante
                logger.debug(f"Head:\n{df.head()}")

        logger.info("[END] Comparação concluída com sucesso")

        return pd.DataFrame(resumo)

    except Exception:
        logger.exception("[ERROR] Erro ao comparar datasets")
        raise
    
def comparar_coluna_entre_datasets(datasets: dict, coluna: str):
    """
    Compara valores de uma coluna entre múltiplos DataFrames.

    :param datasets: dict -> {"nome_dataset": dataframe}
    :param coluna: str -> nome da coluna a ser comparada
    :return: dict com diferenças entre os datasets
    """

    try:
        logger.info("🔍 Iniciando comparação entre datasets")
        logger.info(f"📌 Coluna analisada: {coluna}")
        logger.info(f"📦 Total de datasets recebidos: {len(datasets)}")

        # Criar conjuntos
        conjuntos = {}
        for nome, df in datasets.items():
            if coluna not in df.columns:
                raise ValueError(f"Coluna '{coluna}' não encontrada em {nome}")

            conjuntos[nome] = set(df[coluna].dropna())
            logger.info(f"✅ {nome}: {len(conjuntos[nome])} valores únicos")

        resultados = {}

        # Comparação par a par (combinações)
        for (nome_a, set_a), (nome_b, set_b) in combinations(conjuntos.items(), 2):
            somente_a = set_a - set_b
            somente_b = set_b - set_a

            chave = f"{nome_a} vs {nome_b}"

            resultados[chave] = {
                f"somente_{nome_a}": somente_a,
                f"somente_{nome_b}": somente_b,
            }

            logger.info(f"🔄 Comparação: {nome_a} vs {nome_b}")
            logger.info(f"➡️ {nome_a} exclusivo: {len(somente_a)}")
            logger.info(f"➡️ {nome_b} exclusivo: {len(somente_b)}")

            logger.debug(f"{nome_a} exclusivo: {somente_a}")
            logger.debug(f"{nome_b} exclusivo: {somente_b}")

        logger.info("🏁 Comparação finalizada com sucesso")

        return resultados

    except Exception as e:
        logger.error("❌ Erro na comparação entre datasets", exc_info=True)
        raise
    
