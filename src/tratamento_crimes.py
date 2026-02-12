# src/tratamento_crimes.py
import pandas as pd
import unicodedata
from io import StringIO
from util.log import logs

logger = logs()


def normalizar_texto(texto: str) -> str:
    if pd.isna(texto):
        logger.debug("Texto recebido é NaN/None – retornando sem alteração")
        return texto

    texto_original = str(texto)
    logger.debug(
        "Normalizando texto (tamanho=%d): %s",
        len(texto_original),
        texto_original[:50],
    )

    texto_normalizado = unicodedata.normalize("NFKD", texto_original)
    texto_normalizado = (
        texto_normalizado.encode("ascii", "ignore").decode("utf-8").upper().strip()
    )

    logger.debug(
        "Texto normalizado (tamanho=%d): %s",
        len(texto_normalizado),
        texto_normalizado[:50],
    )

    return texto_normalizado

def padronizando_colunas(df):
    logger.info("Padronizando colunas do DataFrame")

    colunas_antes = df.columns.tolist()
    logger.debug("Colunas antes da padronização: %s", colunas_antes)

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    colunas_depois = df.columns.tolist()
    logger.debug("Colunas após a padronização: %s", colunas_depois)

    return df

def tratar_crimes_contra_mulher(arquivo_entrada, arquivo_saida):
    logger.info("Iniciando tratamento de crimes contra a mulher")
    logger.info("Arquivo de entrada: %s", arquivo_entrada)

    df = pd.read_csv(arquivo_entrada, sep=";")
    logger.debug("CSV carregado | linhas=%d | colunas=%d", df.shape[0], df.shape[1])

    df = padronizando_colunas(df)

    if "arquivo" in df.columns:
        logger.debug("Removendo coluna 'arquivo'")
    df = df.drop(columns=["arquivo"], errors="ignore")

    logger.debug("Estrutura final | linhas=%d | colunas=%d", df.shape[0], df.shape[1])
    logger.debug("Colunas finais: %s", df.columns.tolist())

    df.to_csv(arquivo_saida, sep=";", index=False)
    logger.info("Arquivo tratado salvo com sucesso: %s", arquivo_saida)

def tratar_feminicidio(arquivo_entr_feminicidio, arquivo_saida_feminicidio):
    logger.info("Iniciando tratamento de dados de feminicídio")
    logger.info("Arquivo de entrada: %s", arquivo_entr_feminicidio)

    df = pd.read_csv(arquivo_entr_feminicidio, sep=";")
    logger.debug(
        "CSV carregado | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )

    df = padronizando_colunas(df)

    colunas_remover = [
        "unnamed:_0",
        "2015_a_2024",
        "2023_a_2024",
        "arquivo",
    ]

    colunas_existentes = [c for c in colunas_remover if c in df.columns]
    if colunas_existentes:
        logger.debug("Removendo colunas: %s", colunas_existentes)

    df = df.drop(columns=colunas_remover, errors="ignore")

    filtro_invalido = "* valor não divisivel por zero"
    if "região_administrativa" in df.columns:
        qtd_removidas = (df["região_administrativa"] == filtro_invalido).sum()
        if qtd_removidas > 0:
            logger.debug(
                "Removendo %d linhas inválidas de região administrativa",
                qtd_removidas,
            )
        df = df[df["região_administrativa"] != filtro_invalido]

    logger.debug(
        "Estrutura final | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )
    logger.debug("Colunas finais: %s", df.columns.tolist())

    df.to_csv(arquivo_saida_feminicidio, sep=";", index=False)
    logger.info(
        "Arquivo de feminicídio tratado e salvo em: %s", arquivo_saida_feminicidio
    )

def tratar_desaparecidos_idade_sexo(arquivo_entrada, arquivo_saida):
    logger.info("Iniciando tratamento de desaparecidos por idade e sexo")
    logger.info("Arquivo de entrada: %s", arquivo_entrada)

    df_raw = pd.read_csv(
        arquivo_entrada,
        sep=";",
        encoding="latin1",
        header=None,
        engine="python",
    )
    logger.debug(
        "CSV carregado | linhas=%d | colunas=%d",
        df_raw.shape[0],
        df_raw.shape[1],
    )

    # Remove linhas totalmente vazias
    linhas_antes = df_raw.shape[0]
    df_raw = df_raw.dropna(how="all")
    logger.debug(
        "Linhas totalmente vazias removidas: %d",
        linhas_antes - df_raw.shape[0],
    )

    # Extração e propagação do ano
    df_raw["ano"] = df_raw[0].astype(str).str.extract(r"(20\d{2})")
    df_raw["ano"] = df_raw["ano"].ffill()
    logger.debug("Ano extraído e propagado com forward fill")

    # Remove linhas de cabeçalho e fonte
    linhas_antes = df_raw.shape[0]
    df_raw = df_raw[
        ~df_raw[0]
        .astype(str)
        .str.contains("DESAPARECIDOS|Tabela|Fonte", case=False, na=False)
    ]
    logger.debug(
        "Linhas de cabeçalho/fonte removidas: %d",
        linhas_antes - df_raw.shape[0],
    )

    # Identificação das linhas de dados
    mask_dados = (
        df_raw[0].astype(str).str.contains("ANOS|NÃO INFORMADO|TOTAL", na=False)
    )
    logger.debug("Linhas identificadas como dados: %d", mask_dados.sum())

    # Colunas de interesse (mantendo o ano)
    colunas_base = df_raw.columns[:7].tolist()
    colunas_base.append("ano")

    df_dados = df_raw.loc[mask_dados, colunas_base].copy()
    df_dados.columns = [
        "faixa_etaria",
        "total",
        "total_pct",
        "masc",
        "masc_pct",
        "fem",
        "fem_pct",
        "ano",
    ]

    # Remove totais gerais
    linhas_antes = df_dados.shape[0]
    df_dados = df_dados[df_dados["faixa_etaria"] != "TOTAL"]
    logger.debug(
        "Linhas de TOTAL removidas: %d",
        linhas_antes - df_dados.shape[0],
    )

    # Padronização de texto
    df_dados["faixa_etaria"] = df_dados["faixa_etaria"].str.strip().str.upper()

    # Conversão numérica
    for col in ["total", "masc", "fem"]:
        df_dados[col] = pd.to_numeric(df_dados[col], errors="coerce")

    for col in ["total_pct", "masc_pct", "fem_pct"]:
        df_dados[col] = (
            df_dados[col]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", ".", regex=False)
            .astype(float)
        )

    # Ano como inteiro
    df_dados["ano"] = df_dados["ano"].astype(int)

    logger.debug(
        "Dataset estruturado | linhas=%d | colunas=%d",
        df_dados.shape[0],
        df_dados.shape[1],
    )

    # Normalização para formato longo
    df_final = pd.melt(
        df_dados,
        id_vars=["ano", "faixa_etaria"],
        value_vars=["masc", "fem"],
        var_name="sexo",
        value_name="quantidade",
    )

    df_final["sexo"] = df_final["sexo"].map({"masc": "MASCULINO", "fem": "FEMININO"})

    logger.debug(
        "Dataset final | linhas=%d | colunas=%d",
        df_final.shape[0],
        df_final.shape[1],
    )

    df_final.to_csv(arquivo_saida, index=False, sep=";", encoding="utf-8")
    logger.info(
        "Desaparecidos por idade e sexo tratados e salvos em: %s",
        arquivo_saida,
    )

def tratar_desaparecidos_localizados(arquivo_entrada, arquivo_saida):
    ANO = 2021
    logger.info("Iniciando tratamento de desaparecidos localizados")
    logger.info("Arquivo de entrada: %s | Ano fixo: %s", arquivo_entrada, ANO)

    df = pd.read_csv(
        arquivo_entrada,
        sep=";",
        encoding="latin1",
        header=None,
        skip_blank_lines=True,
    )
    logger.debug(
        "CSV carregado | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )

    # Remoção de linhas irrelevantes
    linhas_antes = df.shape[0]
    df = df[
        ~df[0]
        .astype(str)
        .str.contains(
            "DESAPARECIDOS|Tabela|Fonte|FAIXA|RESULTADO",
            case=False,
            na=False,
        )
    ]
    logger.debug(
        "Linhas de cabeçalho/fonte removidas: %d",
        linhas_antes - df.shape[0],
    )

    # Remove nulos e TOTAL
    linhas_antes = df.shape[0]
    df = df[df[0].notna()]
    df = df[df[0] != "TOTAL"]
    logger.debug(
        "Linhas nulas/TOTAL removidas: %d",
        linhas_antes - df.shape[0],
    )

    # Seleção e renomeação de colunas
    df = df.iloc[:, :5]
    df.columns = [
        "faixa_etaria",
        "ainda_desaparecidos",
        "ainda_desaparecidos_pct",
        "localizados",
        "localizados_pct",
    ]
    logger.debug("Colunas padronizadas")

    # Padronização de texto
    df["faixa_etaria"] = (
        df["faixa_etaria"]
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace("�", "Á", regex=False)
    )

    # Conversão numérica (quantidades)
    for col in ["ainda_desaparecidos", "localizados"]:
        df[col] = (
            df[col].astype(str).str.replace(".", "", regex=False).replace("nan", None)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Conversão percentual
    for col in ["ainda_desaparecidos_pct", "localizados_pct"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", ".", regex=False)
            .replace("nan", None)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["ano"] = ANO
    logger.debug(
        "Dataset estruturado | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )

    # Normalização para formato longo
    df_final = pd.melt(
        df,
        id_vars=["ano", "faixa_etaria"],
        value_vars=["ainda_desaparecidos", "localizados"],
        var_name="status",
        value_name="quantidade",
    )

    df_final["status"] = df_final["status"].map(
        {
            "ainda_desaparecidos": "AINDA DESAPARECIDOS",
            "localizados": "LOCALIZADOS",
        }
    )

    logger.debug(
        "Dataset final | linhas=%d | colunas=%d",
        df_final.shape[0],
        df_final.shape[1],
    )

    df_final.to_csv(arquivo_saida, index=False, sep=";", encoding="utf-8")
    logger.info(
        "Desaparecidos localizados tratados e salvos em: %s",
        arquivo_saida,
    )

def tratar_desaparecidos_regiao(arquivo_entrada, arquivo_saida):
    logger.info("Iniciando tratamento de desaparecidos por região")
    logger.info("Arquivo de entrada: %s", arquivo_entrada)

    df = pd.read_csv(
        arquivo_entrada,
        sep=";",
        encoding="latin1",
        header=None,
        skip_blank_lines=True,
    )
    logger.debug(
        "CSV carregado | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )

    # Remoção de cabeçalhos, fontes e observações
    linhas_antes = df.shape[0]
    df = df[
        ~df[0]
        .astype(str)
        .str.contains(
            "DESAPARECIMENTO|TABEL|FONTE|OBS|VARIACAO",
            case=False,
            na=False,
        )
    ]
    logger.debug(
        "Linhas removidas (cabeçalho/fonte/obs): %d",
        linhas_antes - df.shape[0],
    )

    # Remove nulos e TOTAL
    linhas_antes = df.shape[0]
    df = df[df[0].notna()]
    df = df[df[0] != "TOTAL"]
    logger.debug(
        "Linhas removidas (nulos/TOTAL): %d",
        linhas_antes - df.shape[0],
    )

    # Seleção e renomeação de colunas
    df = df.iloc[:, :6]
    df.columns = [
        "ordem",
        "regiao_administrativa",
        "ocorrencias_2020",
        "ocorrencias_2021",
        "variacao_absoluta",
        "participacao_percentual_2021",
    ]
    logger.debug("Colunas selecionadas e renomeadas")

    # Normalização de texto
    df["regiao_administrativa"] = df["regiao_administrativa"].apply(normalizar_texto)
    logger.debug("Texto de região administrativa normalizado")

    # Conversão de colunas inteiras
    colunas_int = [
        "ordem",
        "ocorrencias_2020",
        "ocorrencias_2021",
        "variacao_absoluta",
    ]
    for col in colunas_int:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Conversão percentual
    df["participacao_percentual_2021"] = (
        df["participacao_percentual_2021"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df["participacao_percentual_2021"] = pd.to_numeric(
        df["participacao_percentual_2021"],
        errors="coerce",
    ).fillna(0.0)

    # Seleção final de colunas
    df = df[
        [
            "regiao_administrativa",
            "ocorrencias_2020",
            "ocorrencias_2021",
            "variacao_absoluta",
            "participacao_percentual_2021",
        ]
    ]

    # Limpeza final
    linhas_antes = df.shape[0]
    df = df.dropna(subset=["regiao_administrativa"])
    df = df[df["regiao_administrativa"] != "REGIAO ADMINISTRATIVA"]
    logger.debug(
        "Linhas removidas após limpeza final: %d",
        linhas_antes - df.shape[0],
    )

    df.reset_index(drop=True, inplace=True)
    logger.debug(
        "Dataset final | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )

    df.to_csv(arquivo_saida, index=False, sep=";", encoding="utf-8")
    logger.info(
        "Desaparecidos por região tratados e salvos em: %s",
        arquivo_saida,
    )

def tratar_furto_veiculo(arquivo_entrada, arquivo_saida):
    logger.info("Iniciando tratamento de furto em veículo")
    logger.info("Arquivo de entrada: %s", arquivo_entrada)

    df = pd.read_csv(arquivo_entrada, sep=";")
    logger.debug("CSV carregado | linhas=%d | colunas=%d", df.shape[0], df.shape[1])

    # Remover colunas irrelevantes
    colunas_remover = ["unnamed:_0", "arquivo"]
    colunas_existentes = [c for c in colunas_remover if c in df.columns]
    if colunas_existentes:
        logger.debug("Removendo colunas: %s", colunas_existentes)
    df = df.drop(columns=colunas_remover, errors="ignore")

    # Linha 1 é o header real
    df.columns = df.iloc[1].astype(str).str.replace(".0", "", regex=False).str.strip()
    logger.debug("Header ajustado com base na segunda linha do CSV")

    # Remover linhas acima do header
    df = df.iloc[2:].reset_index(drop=True)
    df = df.rename(columns={df.columns[0]: "Região Administrativa"})
    logger.debug("Linhas acima do header removidas | linhas atuais: %d", df.shape[0])

    # Remover nulos e totais/fonte
    linhas_antes = df.shape[0]
    df = df[df["Região Administrativa"].notna()]
    df = df[
        ~df["Região Administrativa"].str.contains(
            "Distrito Federal|TOTAL|Fonte", case=False, na=False
        )
    ]
    logger.debug(
        "Linhas removidas por nulo ou total/fonte: %d",
        linhas_antes - df.shape[0],
    )

    # Selecionar apenas anos 2015 a 2024
    colunas_anos = [c for c in df.columns if c.isdigit() and 2015 <= int(c) <= 2024]
    df = df[["Região Administrativa"] + colunas_anos]
    logger.debug("Colunas de anos selecionadas: %s", colunas_anos)

    # Conversão numérica
    for col in colunas_anos:
        df[col] = df[col].fillna(0).astype(float).astype(int)
    logger.debug("Conversão de colunas numéricas concluída")

    # Salvando CSV final
    df.to_csv(arquivo_saida, sep=";", index=False)
    logger.info("Furto em veículo tratado e salvo em: %s", arquivo_saida)

def tratar_homicidio(arquivo_entrada, arquivo_saida):
    logger.info("Iniciando tratamento de homicídio")
    logger.info("Arquivo de entrada: %s", arquivo_entrada)

    df = pd.read_csv(arquivo_entrada, sep=";", dtype=str)
    logger.debug("CSV carregado | linhas=%d | colunas=%d", df.shape[0], df.shape[1])

    # Identifica header
    idx_header = df[
        df.iloc[:, 1].str.contains("Região Administrativa", na=False)
    ].index[0]
    logger.debug("Header encontrado na linha: %d", idx_header)

    # Remover colunas irrelevantes
    colunas_remover = ["unnamed:_0", "arquivo"]
    colunas_existentes = [c for c in colunas_remover if c in df.columns]
    if colunas_existentes:
        logger.debug("Removendo colunas: %s", colunas_existentes)
    df.drop(columns=colunas_remover, errors="ignore", inplace=True)

    # Ajuste do header real
    df.columns = df.iloc[idx_header]
    df = df.iloc[idx_header + 1 :].reset_index(drop=True)
    logger.debug("Linhas acima do header removidas | linhas atuais: %d", df.shape[0])

    # Renomear colunas críticas
    df = df.rename(
        columns={
            "Região Administrativa": "regiao_administrativa",
            "2015 a 2024": "variacao_2015_2024",
            "2023 a 2024": "variacao_2023_2024",
        }
    )

    # Remover nulos e linhas irrelevantes
    linhas_antes = df.shape[0]
    df = df[df["regiao_administrativa"].notna()]
    df = df[~df["regiao_administrativa"].str.contains("Gráfico", na=False)]
    logger.debug("Linhas removidas (nulos/Gráfico): %d", linhas_antes - df.shape[0])

    # Conversão numérica
    colunas_numericas = df.columns.drop("regiao_administrativa")
    df[colunas_numericas] = df[colunas_numericas].apply(pd.to_numeric, errors="coerce")
    logger.debug("Conversão numérica aplicada nas colunas: %s", list(colunas_numericas))

    # Seleção e renomeação final de colunas
    colunas_final = [
        "regiao_administrativa",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023.0",
        "2024.0",
    ]
    df = df[colunas_final]
    df = df.rename(columns={"2023.0": "2023", "2024.0": "2024"})
    logger.debug("Colunas finalizadas e renomeadas: %s", list(df.columns))

    # Salvar CSV
    df.to_csv(arquivo_saida, sep=";", index=False, encoding="utf-8")
    logger.info("Homicídio tratado e salvo em: %s", arquivo_saida)

def tratar_violencia_idosos(caminho_arquivo, arquivo_saida):
    logger.info("Iniciando tratamento de violência contra idosos")
    logger.info("Arquivo de entrada: %s", caminho_arquivo)

    # Ler arquivo
    with open(caminho_arquivo, encoding="latin1") as f:
        linhas = f.readlines()
    logger.debug("Arquivo lido | linhas totais: %d", len(linhas))

    texto = "".join(linhas)

    # Dividir em partes (Tabelas 4 e 5)
    partes = texto.split("Tabela 5:")
    if len(partes) < 2:
        logger.warning("Não foi possível localizar Tabela 5 no arquivo")
        raise ValueError("Tabela 5 não encontrada no arquivo")

    tabela4_txt = partes[0]
    tabela5_txt = "Tabela 5:" + partes[1]
    logger.debug(
        "Tabelas separadas: Tabela4 (%d caracteres), Tabela5 (%d caracteres)",
        len(tabela4_txt),
        len(tabela5_txt),
    )

    # Processar tabelas
    df_t4 = _processar_tabela4(tabela4_txt)
    df_t5 = _processar_tabela5(tabela5_txt)
    logger.debug(
        "Tabelas processadas | Tabela4 linhas=%d | Tabela5 linhas=%d",
        df_t4.shape[0],
        df_t5.shape[0],
    )

    # Salvar CSVs
    if isinstance(arquivo_saida, (list, tuple)) and len(arquivo_saida) == 2:
        df_t4.to_csv(arquivo_saida[0], sep=";", index=False, encoding="utf-8")
        df_t5.to_csv(arquivo_saida[1], sep=";", index=False, encoding="utf-8")
        logger.info("Tabelas salvas em: %s e %s", arquivo_saida[0], arquivo_saida[1])
    else:
        logger.error("arquivo_saida deve ser lista/tupla com 2 caminhos")
        raise ValueError("arquivo_saida deve ser lista/tupla com 2 caminhos")

    return df_t4, df_t5

def _processar_tabela4(texto):
    logger.info("Iniciando processamento da Tabela 4 de violência contra idosos")
    linhas = [
        l.strip()
        for l in texto.splitlines()
        if l.strip() and (l.startswith("ANO") or l.strip()[0].isdigit())
    ]

    logger.debug("Linhas filtradas para processamento: %d", len(linhas))

    if not linhas:
        logger.warning("Nenhuma linha encontrada para Tabela 4")
        raise ValueError("Tabela 4 não contém dados válidos")

    # ✅ Validação de header obrigatória
    if not any(l.startswith("ANO") for l in linhas):
        logger.warning("Header ANO não encontrado na Tabela 4")
        raise ValueError("Tabela 4 sem header válido")

    csv_like = "\n".join(linhas)
    df = pd.read_csv(StringIO(csv_like), sep=";", usecols=[0, 1, 2])

    df.columns = ["ano", "ocorrencias", "violencia_dentro_de_casa"]

    df = df.astype({"ano": int, "ocorrencias": int, "violencia_dentro_de_casa": int})

    logger.info("Tabela 4 processada com sucesso | linhas=%d", df.shape[0])
    return df

def _processar_tabela5(texto):
    logger.info("Iniciando processamento da Tabela 5 de violência contra idosos")

    linhas = []
    for l in texto.splitlines():
        l = l.strip()
        if not l:
            continue

        # aceita header ou linhas iniciadas por ano numérico
        if l.startswith("ANO") or l.split(";")[0].isdigit():
            linhas.append(l)

    logger.debug("Linhas filtradas para processamento: %d", len(linhas))

    if not linhas:
        logger.warning("Nenhuma linha encontrada para Tabela 5")
        raise ValueError("Tabela 5 não contém dados válidos")

    # 🔒 valida header obrigatório
    if not any(l.startswith("ANO") for l in linhas):
        logger.warning("Header ANO não encontrado na Tabela 5")
        raise ValueError("Tabela 5 sem header válido")

    csv_like = "\n".join(linhas)

    df = pd.read_csv(StringIO(csv_like), sep=";")

    logger.debug(
        "CSV lido em DataFrame | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )

    # valida número de colunas esperado
    if df.shape[1] != 4:
        raise ValueError("Tabela 5 com número inesperado de colunas")

    df.columns = ["ano", "masculino", "feminino", "total"]

    try:
        df = df.astype(int)
    except ValueError:
        raise ValueError("Tabela 5 contém valores não numéricos")

    logger.info("Tabela 5 processada com sucesso | linhas=%d", df.shape[0])
    return df

def calcular_variacao_percentual(base, delta):
    logger.info(
        "Iniciando cálculo de variação percentual | base=%s, delta=%s", base, delta
    )

    if base > 0:
        resultado = round((delta / base) * 100, 2)
        logger.debug("Base > 0 | variação calculada: %.2f%%", resultado)
        return resultado

    if base == 0 and delta > 0:
        resultado = round(delta * 100, 2)
        logger.debug("Base = 0 e delta > 0 | variação calculada: %.2f%%", resultado)
        return resultado

    if base == 0 and delta < 0:
        logger.debug("Base = 0 e delta < 0 | variação definida como -100%%")
        return -100.0

    logger.info(
        "Base e delta não indicam aumento ou redução percentual clara, retornando 0.0"
    )
    return 0.0

def tratar_crimes_idosos_ranking(caminho_arquivo, arquivo_saida):
    logger.info(
        "Iniciando tratamento do ranking de crimes contra idosos | arquivo: %s",
        caminho_arquivo,
    )

    # Leitura do CSV
    df = pd.read_csv(caminho_arquivo, sep=";", encoding="latin1", dtype=str)
    logger.debug("Arquivo lido | linhas=%d | colunas=%d", df.shape[0], df.shape[1])

    df = df.dropna(axis=1, how="all")
    logger.debug(
        "Colunas totalmente vazias removidas | colunas restantes=%d", df.shape[1]
    )

    # Renomear colunas
    df.columns = [
        "ranking",
        "regiao_administrativa",
        "jan_ago_2016",
        "jan_ago_2017",
        "variacao_percentual",
        "variacao_absoluta",
    ]

    # Filtrar apenas rankings válidos
    df = df[df["ranking"].str.match(r"^\d+[ªº]?$", na=False)]
    logger.debug(
        "Linhas filtradas por ranking válido | linhas restantes=%d", df.shape[0]
    )

    # Limpar e converter colunas
    df["ranking"] = df["ranking"].str.replace("ª", "", regex=False).astype(int)

    for col in ["jan_ago_2016", "jan_ago_2017", "variacao_absoluta"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    df["variacao_percentual"] = (
        df["variacao_percentual"]
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df["variacao_percentual"] = pd.to_numeric(
        df["variacao_percentual"], errors="coerce"
    ).fillna(0.0)

    # Recalcular variação absoluta e percentual
    df["variacao_absoluta"] = df["jan_ago_2017"] - df["jan_ago_2016"]
    df["variacao_percentual"] = df.apply(
        lambda x: calcular_variacao_percentual(
            x["jan_ago_2016"], x["variacao_absoluta"]
        ),
        axis=1,
    )
    df["variacao_percentual"] = df["variacao_percentual"].round(2)

    logger.debug("Cálculo de variação concluído | amostra:\n%s", df.head())

    # Salvar CSV final
    df.to_csv(arquivo_saida, sep=";", index=False, encoding="utf-8")
    logger.info("Ranking de crimes contra idosos tratado e salvo em: %s", arquivo_saida)

def tratar_crimes_idosos_por_mes(caminho_arquivo, tipo):
    logger.info(
        "Iniciando tratamento de crimes contra idosos por mês | arquivo=%s | tipo=%s",
        caminho_arquivo,
        tipo,
    )

    df_raw = pd.read_csv(
        caminho_arquivo, sep=";", encoding="latin1", header=None, dtype=str
    )
    logger.debug(
        "Arquivo carregado com %d linhas e %d colunas", df_raw.shape[0], df_raw.shape[1]
    )

    # Identifica início e fim da tabela
    if tipo == "registro":
        inicio = df_raw[df_raw[0].str.contains("Tabela 2", na=False)].index[0]
        fim = df_raw[df_raw[0].str.contains("Tabela 3", na=False)].index[0]
    elif tipo == "fato":
        inicio = df_raw[df_raw[0].str.contains("Tabela 3", na=False)].index[0]
        fim = len(df_raw)
    else:
        logger.error("Tipo inválido informado: %s", tipo)
        raise ValueError("tipo deve ser 'registro' ou 'fato'")

    logger.info("Intervalo identificado | inicio=%d | fim=%d", inicio, fim)

    df = df_raw.iloc[inicio:fim].copy()
    logger.debug("Recorte da tabela aplicado | linhas=%d", len(df))

    # Limpeza de dados
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")
    logger.debug("Após remoção de linhas/colunas vazias | shape=%s", df.shape)

    meses_validos = [
        "JAN",
        "FEV",
        "MAR",
        "ABR",
        "MAI",
        "JUN",
        "JUL",
        "AGO",
        "SET",
        "OUT",
        "NOV",
        "DEZ",
    ]

    linhas_antes = len(df)
    df = df[df.iloc[:, 0].isin(meses_validos)]
    logger.debug(
        "Filtro de meses aplicado | antes=%d | depois=%d",
        linhas_antes,
        len(df),
    )

    df.columns = ["mes", "2016", "2017"]
    logger.debug("Colunas renomeadas: %s", df.columns.tolist())

    df = df.melt(id_vars="mes", var_name="ano", value_name="ocorrencias")
    logger.debug("Transformação melt aplicada | linhas=%d", len(df))

    # Conversões
    df["ano"] = df["ano"].astype(int)
    df["ocorrencias"] = (
        pd.to_numeric(df["ocorrencias"], errors="coerce").fillna(0).astype(int)
    )

    df["tipo"] = tipo

    logger.info(
        "Tratamento finalizado com sucesso | registros=%d | tipo=%s",
        len(df),
        tipo,
    )
    return df

def crimes_idosos_por_mes(arquivo_entrada, tipos, arquivo_saida):
    logger.info(
        "Iniciando consolidação de crimes contra idosos por mês | entrada=%s | tipos=%s | saida=%s",
        arquivo_entrada,
        tipos,
        arquivo_saida,
    )

    dfs = []
    for t in tipos:
        logger.info("Processando tipo: %s", t)
        df_tipo = tratar_crimes_idosos_por_mes(arquivo_entrada, t)
        logger.debug(
            "Tipo %s processado | registros=%d",
            t,
            len(df_tipo),
        )
        dfs.append(df_tipo)

    df_final = pd.concat(dfs, ignore_index=True)
    logger.info("Concatenação finalizada | total_registros=%d", len(df_final))

    mapa_meses = {
        "JAN": 1,
        "FEV": 2,
        "MAR": 3,
        "ABR": 4,
        "MAI": 5,
        "JUN": 6,
        "JUL": 7,
        "AGO": 8,
        "SET": 9,
        "OUT": 10,
        "NOV": 11,
        "DEZ": 12,
    }

    df_final["mes_num"] = df_final["mes"].map(mapa_meses)
    logger.debug("Mapeamento de meses aplicado")

    df_pivot = df_final.pivot_table(
        index=["ano", "mes", "mes_num"],
        columns="tipo",
        values="ocorrencias",
        aggfunc="sum",
    ).reset_index()

    logger.info(
        "Pivot table criada | linhas=%d | colunas=%d",
        df_pivot.shape[0],
        df_pivot.shape[1],
    )

    df_pivot["subnotificacao"] = df_pivot["registro"] - df_pivot["fato"]
    logger.debug("Campo subnotificacao calculado")

    df_pivot.to_csv(arquivo_saida, sep=";", index=False, encoding="utf-8")
    logger.info("Arquivo gerado com sucesso | caminho=%s", arquivo_saida)

def tratar_injuria_racial_por_regiao(
    caminho_arquivo_entrada: str, caminho_arquivo_saida: str
):
    logger.info(
        "Iniciando tratamento de injúria racial por região | entrada=%s | saida=%s",
        caminho_arquivo_entrada,
        caminho_arquivo_saida,
    )

    df_raw = pd.read_csv(
        caminho_arquivo_entrada, sep=";", encoding="latin1", header=None, dtype=str
    )
    logger.debug(
        "Arquivo carregado | linhas=%d | colunas=%d",
        df_raw.shape[0],
        df_raw.shape[1],
    )

    df_raw = df_raw.drop(columns=[0], errors="ignore")
    logger.debug("Coluna 0 removida (se existente)")

    linha_header = None
    for i, row in df_raw.iterrows():
        valores = row.astype(str).tolist()
        if any("2015" in v for v in valores) and any("2024" in v for v in valores):
            linha_header = i
            logger.info("Header identificado na linha %d", i)
            break

    if linha_header is None:
        logger.error("Header não encontrado no arquivo %s", caminho_arquivo_entrada)
        raise ValueError("❌ Header não encontrado no CSV")

    df = df_raw.iloc[linha_header + 1 :].copy()
    df = df.dropna(how="all")
    logger.debug("Dados abaixo do header extraídos | linhas=%d", len(df))

    df = df.iloc[:, :12]
    logger.debug("Colunas limitadas às 12 primeiras | colunas=%d", df.shape[1])

    df.columns = [
        "regiao",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
        "variacao_2015_2024",
    ]
    logger.debug("Cabeçalhos padronizados")

    df = df.drop(columns=["variacao_2015_2024"], errors="ignore")
    logger.debug("Coluna de variação removida")

    linhas_antes = len(df)
    df = df[df["regiao"].notna()]
    df = df[~df["regiao"].str.contains("Total|Fonte", case=False, na=False)]
    logger.debug(
        "Filtro de regiões aplicado | antes=%d | depois=%d",
        linhas_antes,
        len(df),
    )

    df["regiao"] = (
        df["regiao"]
        .str.encode("latin1")
        .str.decode("utf-8", errors="ignore")
        .str.strip()
    )
    logger.debug("Normalização de encoding aplicada na coluna regiao")

    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    logger.debug("Conversão numérica aplicada às colunas de anos")

    df = df.fillna(0)
    logger.debug("Valores nulos substituídos por zero")

    df.to_csv(caminho_arquivo_saida, sep=";", index=False, encoding="utf-8")
    logger.info("Arquivo final gerado com sucesso | caminho=%s", caminho_arquivo_saida)

def tratar_latrocinio_por_regiao(
    caminho_arquivo_entrada: str, caminho_arquivo_saida: str
):
    logger.info(
        "Iniciando tratamento de latrocínio por região | entrada=%s | saida=%s",
        caminho_arquivo_entrada,
        caminho_arquivo_saida,
    )

    df_raw = pd.read_csv(
        caminho_arquivo_entrada, sep=";", encoding="latin1", header=None, dtype=str
    )
    logger.debug(
        "Arquivo carregado | linhas=%d | colunas=%d",
        df_raw.shape[0],
        df_raw.shape[1],
    )

    df_raw = df_raw.dropna(axis=1, how="all")
    logger.debug("Colunas totalmente vazias removidas")

    df_raw = df_raw.drop(columns=[0, 12, 13, 14], errors="ignore")
    logger.debug("Colunas irrelevantes removidas (0, 12, 13, 14 se existentes)")

    linha_header = None
    for i, row in df_raw.iterrows():
        valores = row.astype(str).tolist()
        if any("2015" in v for v in valores) and any("2024" in v for v in valores):
            linha_header = i
            logger.info("Header identificado na linha %d", i)
            break

    if linha_header is None:
        logger.error("Header não encontrado no arquivo %s", caminho_arquivo_entrada)
        raise ValueError("❌ Header não encontrado no CSV")

    df = df_raw.iloc[linha_header + 1 :].copy()
    logger.debug(
        "Dados abaixo do header extraídos | linhas=%d",
        len(df),
    )

    df.columns = [
        "regiao",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
    ]
    logger.debug("Cabeçalhos definidos para regiões e anos")

    linhas_antes = len(df)
    df = df[df["regiao"].notna()]
    logger.debug(
        "Filtro de regiões não nulas aplicado | antes=%d | depois=%d",
        linhas_antes,
        len(df),
    )

    df.replace(["-", "", "nan", "None"], 0, inplace=True)
    df = df.fillna(0)
    logger.debug("Valores inválidos substituídos por zero")

    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    logger.debug("Conversão numérica aplicada às colunas de anos")

    df["regiao"] = (
        df["regiao"]
        .str.encode("latin1")
        .str.decode("utf-8", errors="ignore")
        .str.strip()
    )
    logger.debug("Normalização de encoding aplicada na coluna regiao")

    df.to_csv(caminho_arquivo_saida, sep=";", index=False, encoding="utf-8")
    logger.info(
        "Arquivo final de latrocínio por região gerado com sucesso | caminho=%s",
        caminho_arquivo_saida,
    )

def tratar_lesao_corporal_morte_por_regiao(caminho_entrada: str, caminho_saida: str):
    logger.info(
        "Iniciando tratamento de lesão corporal seguida de morte por região | entrada=%s | saida=%s",
        caminho_entrada,
        caminho_saida,
    )

    df_raw = pd.read_csv(caminho_entrada, sep=";", encoding="latin1", dtype=str)
    logger.debug(
        "Arquivo carregado | linhas=%d | colunas=%d",
        df_raw.shape[0],
        df_raw.shape[1],
    )

    colunas_remover = [
        "ï»¿unnamed:_0",
        "unnamed:_12",
        "unnamed:_13",
        "arquivo",
    ]
    df_raw = df_raw.drop(columns=colunas_remover, errors="ignore")
    logger.debug("Colunas irrelevantes removidas (se existentes)")

    linha_header = None
    for i, row in df_raw.iterrows():
        valores = row.astype(str).tolist()
        if any("2015" in v for v in valores) and any("2024" in v for v in valores):
            linha_header = i
            logger.info("Header identificado na linha %d", i)
            break

    if linha_header is None:
        logger.error("Header não encontrado no arquivo %s", caminho_entrada)
        raise ValueError("❌ Header não encontrado no CSV")

    df = df_raw.iloc[linha_header + 1 :].copy()
    logger.debug(
        "Dados abaixo do header extraídos | linhas=%d",
        len(df),
    )

    df.columns = [
        "regiao",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
    ]
    logger.debug("Cabeçalhos definidos")

    linhas_antes = len(df)
    df = df[df["regiao"].notna()]
    df = df[df["regiao"] != "Região Administrativa"]
    logger.debug(
        "Filtro de regiões aplicado | antes=%d | depois=%d",
        linhas_antes,
        len(df),
    )

    df.replace(["-", "", "nan", "None"], 0, inplace=True)
    df = df.fillna(0)
    logger.debug("Valores inválidos e nulos substituídos por zero")

    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    logger.debug("Conversão numérica aplicada às colunas de anos")

    df["regiao"] = (
        df["regiao"]
        .str.encode("latin1")
        .str.decode("utf-8", errors="ignore")
        .str.strip()
    )
    logger.debug("Normalização de encoding aplicada na coluna regiao")

    df.to_csv(caminho_saida, index=False, sep=";", encoding="utf-8")
    logger.info(
        "Arquivo final de lesão corporal seguida de morte por região gerado com sucesso | caminho=%s",
        caminho_saida,
    )

def tratar_lesao_corporal_morte(caminho_entrada: str, caminho_saida: str):
    logger.info(
        "Iniciando tratamento de lesão corporal seguida de morte | entrada=%s | saida=%s",
        caminho_entrada,
        caminho_saida,
    )

    df_raw = pd.read_csv(
        caminho_entrada, sep=";", encoding="latin1", header=None, dtype=str
    )
    logger.debug(
        "Arquivo carregado | linhas=%d | colunas=%d",
        df_raw.shape[0],
        df_raw.shape[1],
    )

    df_raw = df_raw.dropna(axis=1, how="all")
    logger.debug("Colunas totalmente vazias removidas")

    colunas_remover = [0, 12, 13, 14]
    df_raw = df_raw.drop(columns=colunas_remover, errors="ignore")
    logger.debug("Colunas irrelevantes removidas (0, 12, 13, 14 se existentes)")

    linha_header = None
    for i, row in df_raw.iterrows():
        valores = row.astype(str).tolist()
        if any("2015" in v for v in valores) and any("2024" in v for v in valores):
            linha_header = i
            logger.info("Header identificado na linha %d", i)
            break

    if linha_header is None:
        logger.error("Header não encontrado no arquivo %s", caminho_entrada)
        raise ValueError("❌ Header não encontrado no CSV")

    df = df_raw.iloc[linha_header + 1 :].copy()
    logger.debug(
        "Dados abaixo do header extraídos | linhas=%d",
        len(df),
    )

    df.columns = [
        "regiao",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
    ]
    logger.debug("Cabeçalhos definidos")

    linhas_antes = len(df)
    df = df[df["regiao"].notna()]
    df = df[df["regiao"] != "Região Administrativa"]
    logger.debug(
        "Filtro de regiões aplicado | antes=%d | depois=%d",
        linhas_antes,
        len(df),
    )

    df.replace(["-", "", "nan", "None"], 0, inplace=True)
    df = df.fillna(0)
    logger.debug("Valores inválidos e nulos substituídos por zero")

    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    logger.debug("Conversão numérica aplicada às colunas de anos")

    df["regiao"] = (
        df["regiao"]
        .str.encode("latin1")
        .str.decode("utf-8", errors="ignore")
        .str.strip()
    )
    logger.debug("Normalização de encoding aplicada na coluna regiao")

    df.to_csv(caminho_saida, index=False, sep=";", encoding="utf-8")
    logger.info(
        "Arquivo final de lesão corporal seguida de morte gerado com sucesso | caminho=%s",
        caminho_saida,
    )

def tratar_racismo(caminho_entrada: str, caminho_saida: str):
    logger.info(
        "Iniciando tratamento de racismo por região | entrada=%s | saida=%s",
        caminho_entrada,
        caminho_saida,
    )

    df_raw = pd.read_csv(
        caminho_entrada, sep=";", encoding="latin1", header=None, dtype=str
    )
    logger.debug(
        "Arquivo carregado | linhas=%d | colunas=%d",
        df_raw.shape[0],
        df_raw.shape[1],
    )

    df_raw = df_raw.dropna(axis=1, how="all")
    logger.debug("Colunas totalmente vazias removidas")

    df_raw = df_raw.drop(columns=[0, 17, 18, 19], errors="ignore")
    logger.debug("Colunas irrelevantes removidas (0, 17, 18, 19 se existentes)")

    linha_header = None
    for i, row in df_raw.iterrows():
        valores = row.astype(str).tolist()
        if any("2015" in v for v in valores) and any("2024" in v for v in valores):
            linha_header = i
            logger.info("Header identificado na linha %d", i)
            break

    if linha_header is None:
        logger.error("Header não encontrado no arquivo %s", caminho_entrada)
        raise ValueError("❌ Header não encontrado no CSV")

    header = df_raw.iloc[linha_header].tolist()
    idx_2024 = None
    for i, col in enumerate(header):
        if "2024" in str(col):
            idx_2024 = i
            logger.info("Coluna 2024 identificada no índice %d", i)
            break

    if idx_2024 is None:
        logger.error(
            "Coluna 2024 não encontrada no header do arquivo %s", caminho_entrada
        )
        raise ValueError("❌ Coluna 2024 não encontrada")

    header = header[: idx_2024 + 1]
    logger.debug("Header truncado até a coluna 2024")

    df = df_raw.iloc[linha_header + 1 :, : idx_2024 + 1].copy()
    df.columns = header
    logger.debug(
        "Dados extraídos com base no header | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )

    df.columns = [
        "regiao",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
    ]
    logger.debug("Cabeçalhos padronizados")

    linhas_antes = len(df)
    df = df[df["regiao"].notna()]
    df = df[df["regiao"].str.len() > 2]
    logger.debug(
        "Filtro de regiões válidas aplicado | antes=%d | depois=%d",
        linhas_antes,
        len(df),
    )

    df.replace(["-", "*", "", "nan", "None"], 0, inplace=True)
    df = df.fillna(0)
    logger.debug("Valores inválidos e nulos substituídos por zero")

    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    logger.debug("Conversão numérica aplicada às colunas de anos")

    df["regiao"] = (
        df["regiao"]
        .str.encode("latin1")
        .str.decode("utf-8", errors="ignore")
        .str.strip()
    )
    logger.debug("Normalização de encoding aplicada na coluna regiao")

    df = df[df["regiao"] != "Região Administrativa"]
    logger.debug("Região Administrativa removida do dataset")

    df.to_csv(caminho_saida, index=False, sep=";", encoding="utf-8")
    logger.info(
        "Arquivo final de racismo por região gerado com sucesso | caminho=%s",
        caminho_saida,
    )

def tratar_roubo_pedestre(caminho_entrada: str, caminho_saida: str):
    logger.info(
        "Iniciando tratamento de roubo a pedestre | entrada=%s | saida=%s",
        caminho_entrada,
        caminho_saida,
    )

    df = pd.read_csv(caminho_entrada, sep=";", header=0)
    logger.debug(
        "Arquivo carregado | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )

    df = df.dropna(axis=1, how="all")
    logger.debug("Colunas totalmente vazias removidas")

    df = df.drop(
        columns=["unnamed:_0", "unnamed:_12", "unnamed:_13", "arquivo"],
        errors="ignore",
    )
    logger.debug("Colunas irrelevantes removidas (se existentes)")

    linha_header = None
    for i, row in df.iterrows():
        valores = row.astype(str).tolist()
        if any("2015" in v for v in valores) and any("2024" in v for v in valores):
            linha_header = i
            logger.info("Header identificado na linha %d", i)
            break

    if linha_header is None:
        logger.error("Header não encontrado no arquivo %s", caminho_entrada)
        raise ValueError("❌ Header não encontrado no CSV")

    header = df.iloc[linha_header].astype(str).str.strip().tolist()
    logger.debug("Header extraído e normalizado")

    df = df.iloc[linha_header + 1 :].copy()
    df.columns = header
    logger.debug(
        "Dados abaixo do header extraídos | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )

    df.columns = [
        "Região Administrativa",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
    ]
    logger.debug("Cabeçalhos padronizados")

    linhas_antes = len(df)
    df = df[df["Região Administrativa"].notna()]
    df = df[df["Região Administrativa"].str.len() > 2]
    logger.debug(
        "Filtro de regiões válidas aplicado | antes=%d | depois=%d",
        linhas_antes,
        len(df),
    )

    df.replace(["-", "*", "", "nan", "None"], 0, inplace=True)
    logger.debug("Valores inválidos substituídos por zero")

    colunas_anos = [c for c in df.columns if c.replace(".", "").isdigit()]
    logger.debug("Colunas numéricas identificadas: %s", colunas_anos)

    df[colunas_anos] = df[colunas_anos].apply(pd.to_numeric, errors="coerce")
    df[colunas_anos] = df[colunas_anos].fillna(0)
    logger.debug("Conversão numérica aplicada às colunas de anos")

    df.to_csv(caminho_saida, index=False, sep=";", encoding="utf-8")
    logger.info(
        "Arquivo final de roubo a pedestre gerado com sucesso | caminho=%s",
        caminho_saida,
    )

def tratar_roubo_veiculo(caminho_entrada: str, caminho_saida: str):
    logger.info(
        "Iniciando tratamento de roubo de veículo | entrada=%s | saida=%s",
        caminho_entrada,
        caminho_saida,
    )

    df = pd.read_csv(caminho_entrada, sep=";", header=0)
    logger.debug(
        "Arquivo carregado | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )

    df = df.dropna(axis=1, how="all")
    logger.debug("Colunas totalmente vazias removidas")

    df = df.drop(
        columns=["unnamed:_12", "unnamed:_13", "unnamed:_17", "unnamed:_18", "arquivo"],
        errors="ignore",
    )
    logger.debug("Colunas irrelevantes removidas (se existentes)")

    linha_header = None
    for i, row in df.iterrows():
        valores = row.astype(str).tolist()
        if any("2015" in v for v in valores) and any("2024" in v for v in valores):
            linha_header = i
            logger.info("Header identificado na linha %d", i)
            break

    if linha_header is None:
        logger.error("Header não encontrado no arquivo %s", caminho_entrada)
        raise ValueError("❌ Header não encontrado no CSV")

    header = df.iloc[linha_header].astype(str).str.strip().tolist()
    logger.debug("Header extraído e normalizado")

    df = df.iloc[linha_header + 1 :].copy()
    df.columns = header
    logger.debug(
        "Dados abaixo do header extraídos | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )

    colunas_validas = []
    for col in df.columns:
        colunas_validas.append(col)
        if str(col).strip() in ("2024", "2024.0"):
            logger.info("Última coluna de ano (2024) identificada")
            break

    df = df[colunas_validas].copy()
    logger.debug("Colunas válidas recortadas | colunas=%d", df.shape[1])

    df.columns = [
        "Região Administrativa",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
    ]
    logger.debug("Cabeçalhos padronizados")

    linhas_antes = len(df)
    df = df[df["Região Administrativa"].notna()]
    df = df[df["Região Administrativa"].str.len() > 2]
    logger.debug(
        "Filtro de regiões válidas aplicado | antes=%d | depois=%d",
        linhas_antes,
        len(df),
    )

    df.replace(["-", "*", "", "nan", "None"], 0, inplace=True)
    logger.debug("Valores inválidos substituídos por zero")

    colunas_anos = [c for c in df.columns if c.isdigit()]
    logger.debug("Colunas numéricas identificadas: %s", colunas_anos)

    df[colunas_anos] = df[colunas_anos].apply(pd.to_numeric, errors="coerce")
    df[colunas_anos] = df[colunas_anos].fillna(0)
    logger.debug("Conversão numérica aplicada às colunas de anos")

    df.to_csv(caminho_saida, index=False, sep=";", encoding="utf-8")
    logger.info(
        "Arquivo final de roubo de veículo gerado com sucesso | caminho=%s",
        caminho_saida,
    )

def roubo_comercio(caminho_entrada: str, caminho_saida: str):
    logger.info(
        "Iniciando tratamento de roubo a comércio | entrada=%s | saida=%s",
        caminho_entrada,
        caminho_saida,
    )

    df = pd.read_csv(caminho_entrada, sep=";", header=0)
    logger.debug(
        "Arquivo carregado | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )

    df = df.dropna(axis=1, how="all")
    logger.debug("Colunas totalmente vazias removidas")

    df = df.drop(
        columns=["unnamed:_12", "unnamed:_13", "unnamed:_17", "unnamed:_18", "arquivo"],
        errors="ignore",
    )
    logger.debug("Colunas irrelevantes removidas (se existentes)")

    linha_header = None
    for i, row in df.iterrows():
        valores = row.astype(str).tolist()
        if any("2015" in v for v in valores) and any("2024" in v for v in valores):
            linha_header = i
            logger.info("Header identificado na linha %d", i)
            break

    if linha_header is None:
        logger.error("Header não encontrado no arquivo %s", caminho_entrada)
        raise ValueError("❌ Header não encontrado no CSV")

    header = df.iloc[linha_header].astype(str).str.strip().tolist()
    logger.debug("Header extraído e normalizado")

    df = df.iloc[linha_header + 1 :].copy()
    df.columns = header
    logger.debug(
        "Dados abaixo do header extraídos | linhas=%d | colunas=%d",
        df.shape[0],
        df.shape[1],
    )

    colunas_validas = []
    for col in df.columns:
        colunas_validas.append(col)
        if str(col).strip() in ("2024", "2024.0"):
            logger.info("Última coluna de ano (2024) identificada")
            break

    df = df[colunas_validas].copy()
    logger.debug("Colunas válidas recortadas | colunas=%d", df.shape[1])

    df.columns = [
        "Região Administrativa",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
    ]
    logger.debug("Cabeçalhos padronizados")

    linhas_antes = len(df)
    df = df[df["Região Administrativa"].notna()]
    df = df[df["Região Administrativa"].str.len() > 2]
    logger.debug(
        "Filtro de regiões válidas aplicado | antes=%d | depois=%d",
        linhas_antes,
        len(df),
    )

    df.replace(["-", "*", "", "nan", "None"], 0, inplace=True)
    logger.debug("Valores inválidos substituídos por zero")

    colunas_anos = [c for c in df.columns if c.isdigit()]
    logger.debug("Colunas numéricas identificadas: %s", colunas_anos)

    df[colunas_anos] = df[colunas_anos].apply(pd.to_numeric, errors="coerce")
    df[colunas_anos] = df[colunas_anos].fillna(0)
    logger.debug("Conversão numérica aplicada às colunas de anos")

    df.to_csv(caminho_saida, index=False, sep=";", encoding="utf-8")
    logger.info(
        "Arquivo final de roubo a comércio gerado com sucesso | caminho=%s",
        caminho_saida,
    )

def roubo_transporte_coletivo(caminho_entrada: str, caminho_saida: str):
    logger.info("Iniciando processamento do arquivo: %s", caminho_entrada)

    df = pd.read_csv(caminho_entrada, sep=";", header=0)
    logger.info(
        "Arquivo carregado com %d linhas e %d colunas", df.shape[0], df.shape[1]
    )

    # Remover colunas totalmente vazias
    df = df.dropna(axis=1, how="all")
    logger.debug("Após dropna(all): %d colunas restantes", df.shape[1])

    # Remover colunas indesejadas
    colunas_remover = [
        "unnamed:_12",
        "unnamed:_13",
        "unnamed:_17",
        "unnamed:_18",
        "arquivo",
    ]
    df = df.drop(columns=colunas_remover, errors="ignore")
    logger.debug("Colunas removidas (se existiam): %s", colunas_remover)

    # Encontrar linha do header real
    linha_header = None
    for i, row in df.iterrows():
        valores = row.astype(str).tolist()
        if any("2015" in v for v in valores) and any("2024" in v for v in valores):
            linha_header = i
            logger.info("Header encontrado na linha %d", i)
            break

    if linha_header is None:
        logger.error("❌ Header não encontrado no CSV")
        raise ValueError("❌ Header não encontrado no CSV")

    # Ajustar header
    header = df.iloc[linha_header].astype(str).str.strip().tolist()
    df = df.iloc[linha_header + 1 :].copy()
    df.columns = header
    logger.debug("Header aplicado com %d colunas", len(header))

    # Manter apenas Região + anos até 2024
    colunas_validas = []
    for col in df.columns:
        colunas_validas.append(col)
        if str(col).strip() in ("2024.0", "2024"):
            break

    df = df[colunas_validas].copy()
    logger.info("Colunas mantidas até 2024: %s", colunas_validas)

    # Renomear colunas
    df.columns = [
        "Região Administrativa",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
    ]
    logger.debug("Colunas renomeadas com sucesso")

    # Limpeza de registros inválidos
    linhas_antes = df.shape[0]
    df = df[df["Região Administrativa"].notna()]
    df = df[df["Região Administrativa"].str.len() > 2]
    logger.info(
        "Registros filtrados por Região Administrativa: %d → %d",
        linhas_antes,
        df.shape[0],
    )

    # Substituição de valores inválidos
    df.replace(["-", "*", "", "nan", "None"], 0, inplace=True)
    logger.debug("Valores inválidos substituídos por 0")

    # Conversão para numérico
    colunas_anos = [c for c in df.columns if c.isdigit()]
    df[colunas_anos] = df[colunas_anos].apply(pd.to_numeric, errors="coerce").fillna(0)
    logger.info("Conversão para numérico aplicada nas colunas: %s", colunas_anos)

    # Salvar resultado
    df.to_csv(caminho_saida, index=False, sep=";", encoding="utf-8")
    logger.info("Arquivo processado salvo em: %s", caminho_saida)
