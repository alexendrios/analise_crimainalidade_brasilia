import re
import pandas as pd
import numpy as np
from pathlib import Path
import unicodedata    
from io import StringIO
                        
def normalizar_texto(texto: str) -> str:
    if pd.isna(texto):
        return texto
    texto = unicodedata.normalize("NFKD", texto)
    texto = texto.encode("ascii", "ignore").decode("utf-8")
    return texto.upper().strip()

def padronizando_colunas(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

def tratar_crimes_contra_mulher(arquivo_entrada, arquivo_saida):
    df = pd.read_csv(arquivo_entrada, sep=";")
    df = padronizando_colunas(df)
    df = df.drop(columns=["arquivo"], errors="ignore")
    df.to_csv(arquivo_saida, sep=';', index=False)

def tratar_feminicidio(arquivo_entr_feminicidio, arquivo_saida_feminicidio):
    

    df = pd.read_csv(arquivo_entr_feminicidio, sep=';')
    df = padronizando_colunas(df)

    col = [
        "unnamed:_0",
        "2015_a_2024",
        "2023_a_2024",  
        "arquivo",
    ]


    df = df.drop(columns=col, errors="ignore")

    df = df[df["região_administrativa"] != "* valor não divisivel por zero"]
    df.to_csv(arquivo_saida_feminicidio, sep=';', index=False)

def tratar_desaparecidos_idade_sexo(arquivo_entrada, arquivo_saida):
    df_raw = pd.read_csv(
        arquivo_entrada, sep=";", encoding="latin1", header=None, engine="python"
    )
   
    # Remove linhas totalmente vazias
    df_raw = df_raw.dropna(how="all")
    df_raw["ano"] = df_raw[0].astype(str).str.extract(r"(20\d{2})")

    df_raw["ano"] = df_raw["ano"].ffill()
    df_raw = df_raw[
        ~df_raw[0]
        .astype(str)
        .str.contains("DESAPARECIDOS|Tabela|Fonte", case=False, na=False)
    ]

    mask_dados = df_raw[0].astype(str).str.contains("ANOS|NÃO INFORMADO|TOTAL", na=False)

    # Colunas de interesse (mantendo o ANO!)
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
    df_dados = df_dados[df_dados["faixa_etaria"] != "TOTAL"]

    # Padroniza texto
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

    df_final = pd.melt(
        df_dados,
        id_vars=["ano", "faixa_etaria"],
        value_vars=["masc", "fem"],
        var_name="sexo",
        value_name="quantidade",
    )

    df_final["sexo"] = df_final["sexo"].map({"masc": "MASCULINO", "fem": "FEMININO"})
    df_final.to_csv(arquivo_saida, index=False, encoding="utf-8")

def tratar_desaparecidos_localizados(arquivo_entrada, arquivo_saida):
    ANO = 2021

    df = pd.read_csv(
        arquivo_entrada, sep=";", encoding="latin1", header=None, skip_blank_lines=True
    )

    df = df[
        ~df[0]
        .astype(str)
        .str.contains("DESAPARECIDOS|Tabela|Fonte|FAIXA|RESULTADO", case=False, na=False)
    ]

    df = df[df[0].notna()]
    df = df[df[0] != "TOTAL"]


    df = df.iloc[:, :5]
    df.columns = [
        "faixa_etaria",
        "ainda_desaparecidos",
        "ainda_desaparecidos_pct",
        "localizados",
        "localizados_pct",
    ]

    df["faixa_etaria"] = (
        df["faixa_etaria"]
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace("�", "Á", regex=False)
    )


    for col in ["ainda_desaparecidos", "localizados"]:
        df[col] = df[col].astype(str).str.replace(".", "", regex=False).replace("nan", None)
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)


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

    df_final = pd.melt(
        df,
        id_vars=["ano", "faixa_etaria"],
        value_vars=["ainda_desaparecidos", "localizados"],
        var_name="status",
        value_name="quantidade",
    )

    df_final["status"] = df_final["status"].map(
        {"ainda_desaparecidos": "AINDA DESAPARECIDOS", "localizados": "LOCALIZADOS"}
    )

    df_final.to_csv(arquivo_saida, index=False, encoding="utf-8")

def tratar_desaparecidos_regiao(arquivo_entrada, arquivo_saida):

    df = pd.read_csv(
        arquivo_entrada, sep=";", encoding="latin1", header=None, skip_blank_lines=True
    )

    df = df[
        ~df[0]
        .astype(str)
        .str.contains("DESAPARECIMENTO|TABEL|FONTE|OBS|VARIACAO", case=False, na=False)
    ]

    df = df[df[0].notna()]
    df = df[df[0] != "TOTAL"]

    df = df.iloc[:, :6]
    df.columns = [
        "ordem",
        "regiao_administrativa",
        "ocorrencias_2020",
        "ocorrencias_2021",
        "variacao_absoluta",
        "participacao_percentual_2021",
    ]

    df["regiao_administrativa"] = df["regiao_administrativa"].apply(normalizar_texto)
    colunas_int = ["ordem", "ocorrencias_2020", "ocorrencias_2021", "variacao_absoluta"]

    for col in colunas_int:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)


    df["participacao_percentual_2021"] = (
        df["participacao_percentual_2021"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    df["participacao_percentual_2021"] = pd.to_numeric(
        df["participacao_percentual_2021"], errors="coerce"
    ).fillna(0.0)

    df = df[
        [
            "regiao_administrativa",
            "ocorrencias_2020",
            "ocorrencias_2021",
            "variacao_absoluta",
            "participacao_percentual_2021",
        ]
    ]

    df = df.dropna(subset=["regiao_administrativa"])
    df = df[df["regiao_administrativa"] != "REGIAO ADMINISTRATIVA"]

    df.reset_index(drop=True, inplace=True)

    df.to_csv(arquivo_saida, index=False, encoding="utf-8")

def tratar_furto_veiculo(arquivo_entrada, arquivo_saida):
    df = pd.read_csv(arquivo_entrada, sep=";")                                              

    col = [
        "unnamed:_0",
        "arquivo",
    ]
    df = df.drop(columns=col, errors="ignore")

    # linha 1 é o header real
    df.columns = df.iloc[1].astype(str).str.replace(".0", "", regex=False).str.strip()

    # remover linhas acima do header
    df = df.iloc[2:].reset_index(drop=True)
    df = df.rename(columns={df.columns[0]: "Região Administrativa"})

    df = df[df["Região Administrativa"].notna()]

    df = df[
        ~df["Região Administrativa"].str.contains(
            "Distrito Federal|TOTAL|Fonte", case=False, na=False
        )
    ]

    colunas_anos = [c for c in df.columns if c.isdigit() and 2015 <= int(c) <= 2024]

    df = df[["Região Administrativa"] + colunas_anos]

    for col in colunas_anos:
        df[col] = df[col].fillna(0).astype(float).astype(int)

    df.to_csv(arquivo_saida, sep=';', index=False)

def tratar_homicidio(arquivo_entrada, arquivo_saida):
    df = pd.read_csv(arquivo_entrada, sep=";", dtype=str)

    idx_header = df[
        df.iloc[:, 1].str.contains("Região Administrativa", na=False)
    ].index[0]
    df.drop(columns=["unnamed:_0", "arquivo"], errors="ignore", inplace=True)

    df.columns = df.iloc[idx_header]
    df = df.iloc[idx_header + 1 :].reset_index(drop=True)
    df = df.rename(
        columns={
            "Região Administrativa": "regiao_administrativa",
            "2015 a 2024": "variacao_2015_2024",
            "2023 a 2024": "variacao_2023_2024",
        }
    )

    df = df[df["regiao_administrativa"].notna()]
    df = df[~df["regiao_administrativa"].str.contains("Gráfico", na=False)]
    
    colunas_numericas = df.columns.drop("regiao_administrativa")
    df[colunas_numericas] = df[colunas_numericas].apply(pd.to_numeric, errors="coerce")
    col = [
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
    df = df[col]
    df = df.rename(columns={"2023.0": "2023", "2024.0": "2024"})
    df.to_csv(arquivo_saida, index=False)

def tratar_violencia_idosos(caminho_arquivo, arquivo_saida):
    with open(caminho_arquivo, encoding="latin1") as f:
        linhas = f.readlines()

    texto = "".join(linhas)

    partes = texto.split("Tabela 5:")

    tabela4_txt = partes[0]
    tabela5_txt = "Tabela 5:" + partes[1]

    df_t4 = _processar_tabela4(tabela4_txt)
    df_t5 = _processar_tabela5(tabela5_txt)
    arquivo_saida[0]
    df_t4.to_csv(arquivo_saida[0], index=False)
    df_t5.to_csv(arquivo_saida[1], index=False)         
    return df_t4, df_t5

def _processar_tabela4(texto):
    linhas = [
        l for l in texto.splitlines() if l.strip() and l.startswith(("ANO", "201"))
    ]

    csv_like = "\n".join(linhas)

    df = pd.read_csv(StringIO(csv_like), sep=";", usecols=[0, 1, 2])
    

    df.columns = ["ano", "ocorrencias", "violencia_dentro_de_casa"]

    df = df.astype({"ano": int, "ocorrencias": int, "violencia_dentro_de_casa": int})
    return df

def _processar_tabela5(texto):
    linhas = [
        l for l in texto.splitlines() if l.strip() and l.startswith(("ANO", "201"))
    ]

    csv_like = "\n".join(linhas)

    df = pd.read_csv(StringIO(csv_like), sep=";")

    df.columns = ["ano", "masculino", "feminino", "total"]

    df = df.astype(int)
    return df

def calcular_variacao_percentual(base, delta):
    if base > 0:
        return round((delta / base) * 100, 2)
    if base == 0 and delta > 0:
        return round(delta * 100, 2)
    if base == 0 and delta < 0:
        return -100.0
    return 0.0

def tratar_crimes_idosos_ranking(caminho_arquivo, arquivo_saida):
    df = pd.read_csv(caminho_arquivo, sep=";", encoding="latin1", dtype=str)
    df = df.dropna(axis=1, how="all")

    df.columns = [
        "ranking",
        "regiao_administrativa",
        "jan_ago_2016",
        "jan_ago_2017",
        "variacao_percentual",
        "variacao_absoluta",
    ]

    df = df[df["ranking"].str.match(r"^\d+[ªº]?$", na=False)]

    df["ranking"] = df["ranking"].str.replace("ª", "", regex=False).astype(int)
    df["variacao_percentual"] = (
        df["variacao_percentual"]
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    
    df["variacao_percentual"] = pd.to_numeric(
        df["variacao_percentual"], errors="coerce"
    )
    df["variacao_absoluta"] = pd.to_numeric(df["variacao_absoluta"], errors="coerce")

    df["jan_ago_2016"] = pd.to_numeric(df["jan_ago_2016"], errors="coerce")
    df["jan_ago_2017"] = pd.to_numeric(df["jan_ago_2017"], errors="coerce")

    df["variacao_absoluta"] = df["jan_ago_2017"] - df["jan_ago_2016"]

    df["variacao_percentual"] = df.apply(
        lambda x: calcular_variacao_percentual(
            x["jan_ago_2016"], x["variacao_absoluta"]
        ),
        axis=1,
    )

    df["variacao_percentual"] = df["variacao_percentual"].round(2)

    df.to_csv(arquivo_saida, index=False) 

def tratar_crimes_idosos_por_mes(caminho_arquivo, tipo):
    df_raw = pd.read_csv(
        caminho_arquivo, sep=";", encoding="latin1", header=None, dtype=str
    )

    # Identifica início e fim da tabela
    if tipo == "registro":
        inicio = df_raw[df_raw[0].str.contains("Tabela 2", na=False)].index[0]
        fim = df_raw[df_raw[0].str.contains("Tabela 3", na=False)].index[0]
    elif tipo == "fato":
        inicio = df_raw[df_raw[0].str.contains("Tabela 3", na=False)].index[0]
        fim = len(df_raw)
    else:
        raise ValueError("tipo deve ser 'registro' ou 'fato'")

    df = df_raw.iloc[inicio:fim].copy()

    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")

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
    df = df[df.iloc[:, 0].isin(meses_validos)]

    df.columns = ["mes", "2016", "2017"]

    df = df.melt(id_vars="mes", var_name="ano", value_name="ocorrencias")

    df["ano"] = df["ano"].astype(int)
    df["ocorrencias"] = (
        pd.to_numeric(df["ocorrencias"], errors="coerce").fillna(0).astype(int)
    )

    df["tipo"] = tipo
    
    return df

def crimes_idosos_por_mes(arquivo_entrada, tipos, arquivo_saida):
    dfs = [tratar_crimes_idosos_por_mes(arquivo_entrada, t) for t in tipos]

    df_final = pd.concat(dfs, ignore_index=True)

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

    df_pivot = df_final.pivot_table(
        index=["ano", "mes", "mes_num"],
        columns="tipo",
        values="ocorrencias",
        aggfunc="sum",
    ).reset_index()

    df_pivot["subnotificacao"] = df_pivot["registro"] - df_pivot["fato"]

    df_pivot.to_csv(arquivo_saida, index=False)
