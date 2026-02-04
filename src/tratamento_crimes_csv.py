import re
import pandas as pd
from pathlib import Path
import unicodedata    

                        
def normalizar_texto(texto: str) -> str:
    if pd.isna(texto):
        return texto
    texto = unicodedata.normalize("NFKD", texto)
    texto = texto.encode("ascii", "ignore").decode("utf-8")
    return texto.upper().strip()

def padronizando_colunas(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

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
