import pandas as pd
import pytest

from src.tratamento_crimes import (
    tratar_crimes_contra_mulher,
    tratar_feminicidio,
    tratar_furto_veiculo,
    tratar_homicidio,
    tratar_desaparecidos_idade_sexo,
    tratar_desaparecidos_localizados,
    tratar_desaparecidos_regiao,
    tratar_crimes_idosos_ranking,
    tratar_crimes_idosos_por_mes,
    crimes_idosos_por_mes,
    tratar_injuria_racial_por_regiao,
    tratar_latrocinio_por_regiao,
    tratar_lesao_corporal_morte_por_regiao,
    tratar_lesao_corporal_morte,
    tratar_racismo,
    tratar_roubo_pedestre,
    tratar_roubo_veiculo,
    roubo_comercio,
    roubo_transporte_coletivo,
)


# ============================================================
# tratar_crimes_contra_mulher
# ============================================================
def test_tratar_crimes_contra_mulher_remove_coluna_arquivo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    pd.DataFrame(
        {
            "Região Administrativa": ["Ceilandia", "Taguatinga"],
            "arquivo": ["a.xlsx", "b.xlsx"],
            "2020": [10, 20],
        }
    ).to_csv(entrada, sep=";", index=False)

    tratar_crimes_contra_mulher(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")
    assert "arquivo" not in resultado.columns
    assert list(resultado.columns) == ["região_administrativa", "2020"]
    assert resultado["2020"].tolist() == [10, 20]


def test_tratar_crimes_contra_mulher_sem_coluna_arquivo_nao_quebra(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    pd.DataFrame({"Região Administrativa": ["Ceilandia"], "2020": [5]}).to_csv(
        entrada, sep=";", index=False
    )

    tratar_crimes_contra_mulher(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")
    assert resultado["2020"].tolist() == [5]


# ============================================================
# tratar_feminicidio
# ============================================================
def test_tratar_feminicidio_remove_colunas_e_linha_invalida(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    pd.DataFrame(
        {
            "Unnamed: 0": [0, 1, 2],
            "região_administrativa": [
                "Ceilandia",
                "* valor não divisivel por zero",
                "Taguatinga",
            ],
            "2015 a 2024": [1, 2, 3],
            "2023 a 2024": [1, 2, 3],
            "arquivo": ["a", "b", "c"],
            "2020": [10, 20, 30],
        }
    ).to_csv(entrada, sep=";", index=False)

    tratar_feminicidio(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    # colunas irrelevantes removidas
    for col in ["unnamed:_0", "2015_a_2024", "2023_a_2024", "arquivo"]:
        assert col not in resultado.columns

    # linha inválida removida
    assert "* valor não divisivel por zero" not in resultado["região_administrativa"].tolist()
    assert len(resultado) == 2
    assert resultado["2020"].tolist() == [10, 30]


def test_tratar_feminicidio_sem_linha_invalida_mantem_todas(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    pd.DataFrame(
        {"região_administrativa": ["Ceilandia", "Taguatinga"], "2020": [1, 2]}
    ).to_csv(entrada, sep=";", index=False)

    tratar_feminicidio(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")
    assert len(resultado) == 2


# ============================================================
# tratar_furto_veiculo
# ============================================================
def test_tratar_furto_veiculo_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "x0;x1;x2;x3",  # header bruto do CSV (descartado)
        "ignora0;ignora1;ignora2;ignora3",  # linha índice 0 (descartada)
        "RegiaoCol;2020;2021;2022",  # linha índice 1 -> vira o header real
        "Ceilandia;10;20;30",
        "Taguatinga;5;15;25",
        "Distrito Federal;999;999;999",  # deve ser removida (totalizador)
    ]
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    tratar_furto_veiculo(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    assert list(resultado.columns) == ["Região Administrativa", "2020", "2021", "2022"]
    assert "Distrito Federal" not in resultado["Região Administrativa"].tolist()
    assert len(resultado) == 2
    assert resultado.set_index("Região Administrativa").loc["Ceilandia"].tolist() == [10, 20, 30]


def test_tratar_furto_veiculo_remove_totais_e_fonte(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "x0;x1;x2",
        "ignora0;ignora1;ignora2",
        "RegiaoCol;2020;2021",
        "Ceilandia;10;20",
        "TOTAL;999;999",
        "Fonte: SSP-DF;0;0",
    ]
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    tratar_furto_veiculo(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")
    assert len(resultado) == 1
    assert resultado["Região Administrativa"].tolist() == ["Ceilandia"]


def test_tratar_furto_veiculo_remove_colunas_irrelevantes_se_presentes(tmp_path):
    """Cobre o ramo que loga quando 'unnamed:_0'/'arquivo' realmente existem no CSV."""
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "unnamed:_0;arquivo;x2;x3",
        "0;a.xlsx;ignora1;ignora2",
        "1;b.xlsx;2020;2021",
        "2;c.xlsx;10;20",
    ]
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    tratar_furto_veiculo(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")
    assert "unnamed:_0" not in resultado.columns
    assert "arquivo" not in resultado.columns


# ============================================================
# tratar_homicidio
# ============================================================
def test_tratar_homicidio_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    colunas_raw = ["unnamed:_0", "arquivo"] + [f"c{i}" for i in range(2, 13)]
    linhas = [
        ";".join(colunas_raw),  # header bruto (descartado)
        "junk0;valor_qualquer;j;j;j;j;j;j;j;j;j;j;j",  # linha índice 0 (não é o header real)
        # linha índice 1: coluna "arquivo" contém o marcador buscado pelo código
        "junk1;Região Administrativa;Região Administrativa;2015;2016;2017;2018;2019;2020;2021;2022;2023.0;2024.0",
        # dados reais (a partir daqui)
        "junk2;valor2;Ceilandia;10;11;12;13;14;15;16;17;18;19",
        "junk3;valor3;Taguatinga;20;21;22;23;24;25;26;27;28;29",
    ]
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    tratar_homicidio(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    colunas_esperadas = [
        "regiao_administrativa",
        "2015", "2016", "2017", "2018", "2019",
        "2020", "2021", "2022", "2023", "2024",
    ]
    assert list(resultado.columns) == colunas_esperadas
    assert len(resultado) == 2
    assert resultado["regiao_administrativa"].tolist() == ["Ceilandia", "Taguatinga"]
    assert resultado["2015"].tolist() == [10.0, 20.0]
    assert resultado["2024"].tolist() == [19.0, 29.0]


def test_tratar_homicidio_remove_linhas_com_grafico(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    colunas_raw = ["unnamed:_0", "arquivo"] + [f"c{i}" for i in range(2, 13)]
    linhas = [
        ";".join(colunas_raw),
        "junk0;valor_qualquer;j;j;j;j;j;j;j;j;j;j;j",
        "junk1;Região Administrativa;Região Administrativa;2015;2016;2017;2018;2019;2020;2021;2022;2023.0;2024.0",
        "junk2;valor2;Ceilandia;10;11;12;13;14;15;16;17;18;19",
        "junk3;valor3;Gráfico ilustrativo;0;0;0;0;0;0;0;0;0;0",
    ]
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    tratar_homicidio(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")
    assert len(resultado) == 1
    assert resultado["regiao_administrativa"].tolist() == ["Ceilandia"]


# ============================================================
# tratar_desaparecidos_idade_sexo
# ============================================================
def _linha_desaparecidos(*valores):
    """Preenche até 7 colunas (o restante fica vazio, como no arquivo real)."""
    valores = list(valores) + [""] * (7 - len(valores))
    return ";".join(str(v) for v in valores[:7])


def test_tratar_desaparecidos_idade_sexo_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        _linha_desaparecidos("DESAPARECIDOS BOLETIM 2020"),
        _linha_desaparecidos("Tabela 1 - algo"),
        _linha_desaparecidos("0 A 17 ANOS", "10", "10,0%", "6", "60,0%", "4", "40,0%"),
        _linha_desaparecidos("NÃO INFORMADO", "5", "5,0%", "2", "20,0%", "3", "30,0%"),
        _linha_desaparecidos("TOTAL", "15", "100,0%", "8", "80,0%", "7", "70,0%"),
        _linha_desaparecidos("Fonte: SSP-DF"),
    ]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    tratar_desaparecidos_idade_sexo(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    assert list(resultado.columns) == ["ano", "faixa_etaria", "sexo", "quantidade"]
    # TOTAL foi removido, sobram 2 faixas etárias x 2 sexos = 4 linhas
    assert len(resultado) == 4
    assert set(resultado["ano"].unique()) == {2020}
    assert set(resultado["sexo"].unique()) == {"MASCULINO", "FEMININO"}
    assert set(resultado["faixa_etaria"].unique()) == {"0 A 17 ANOS", "NÃO INFORMADO"}

    linha_masc = resultado[
        (resultado["faixa_etaria"] == "0 A 17 ANOS") & (resultado["sexo"] == "MASCULINO")
    ]
    assert linha_masc["quantidade"].iloc[0] == 6


def test_tratar_desaparecidos_idade_sexo_remove_linhas_vazias(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        _linha_desaparecidos("DESAPARECIDOS BOLETIM 2021"),
        _linha_desaparecidos(),  # linha totalmente vazia -> descartada
        _linha_desaparecidos("18 A 29 ANOS", "8", "8,0%", "5", "50,0%", "3", "30,0%"),
    ]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    tratar_desaparecidos_idade_sexo(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")
    assert set(resultado["ano"].unique()) == {2021}
    assert len(resultado) == 2  # 1 faixa etária x 2 sexos


# ============================================================
# tratar_desaparecidos_localizados
# ============================================================
def test_tratar_desaparecidos_localizados_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "Tabela 1 - Desaparecidos localizados;;;;",
        "FAIXA ETARIA;AINDA DESAPARECIDOS;%;LOCALIZADOS;%",
        "0 A 17 ANOS;1.234;50,0%;617;30,0%",
        "TOTAL;2000;100%;1000;50%",
        "Fonte: SSP-DF;;;;",
    ]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    tratar_desaparecidos_localizados(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    assert list(resultado.columns) == ["ano", "faixa_etaria", "status", "quantidade"]
    assert len(resultado) == 2  # 1 faixa etária x 2 status
    assert set(resultado["ano"].unique()) == {2021}
    assert set(resultado["status"].unique()) == {"AINDA DESAPARECIDOS", "LOCALIZADOS"}

    linha_ainda = resultado[resultado["status"] == "AINDA DESAPARECIDOS"]
    assert linha_ainda["quantidade"].iloc[0] == 1234  # "1.234" -> 1234 (ponto removido)

    linha_localizados = resultado[resultado["status"] == "LOCALIZADOS"]
    assert linha_localizados["quantidade"].iloc[0] == 617


def test_tratar_desaparecidos_localizados_remove_total_e_cabecalhos(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "RESULTADO GERAL 2021;;;;",
        "18 A 29 ANOS;500;25,0%;300;15,0%",
        "TOTAL;9999;100%;9999;100%",
    ]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    tratar_desaparecidos_localizados(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")
    assert "TOTAL" not in resultado["faixa_etaria"].tolist()
    assert len(resultado) == 2


# ============================================================
# tratar_desaparecidos_regiao
# ============================================================
def test_tratar_desaparecidos_regiao_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "TABELA 1 - Desaparecidos por regiao;;;;;",
        "1;Ceilândia;100;120;20;15,5%",
        "2;REGIAO ADMINISTRATIVA;0;0;0;0%",
        "TOTAL;Todos;500;600;100;100%",
        "Fonte: SSP-DF;;;;;",
    ]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    tratar_desaparecidos_regiao(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    assert list(resultado.columns) == [
        "regiao_administrativa",
        "ocorrencias_2020",
        "ocorrencias_2021",
        "variacao_absoluta",
        "participacao_percentual_2021",
    ]
    assert len(resultado) == 1
    linha = resultado.iloc[0]
    assert linha["regiao_administrativa"] == "CEILANDIA"
    assert linha["ocorrencias_2020"] == 100
    assert linha["ocorrencias_2021"] == 120
    assert linha["variacao_absoluta"] == 20
    assert linha["participacao_percentual_2021"] == 15.5


def test_tratar_desaparecidos_regiao_remove_obs_e_variacao(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "OBS: dados sujeitos a revisão;;;;;",
        "VARIACAO calculada com base em;;;;;",
        "1;Taguatinga;50;60;10;10,0%",
    ]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    tratar_desaparecidos_regiao(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")
    assert len(resultado) == 1
    assert resultado.iloc[0]["regiao_administrativa"] == "TAGUATINGA"


# ============================================================
# tratar_crimes_idosos_ranking
# ============================================================
def test_tratar_crimes_idosos_ranking_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "Ranking;Regiao;jan-ago 2016;jan-ago 2017;Variacao %;Variacao absoluta",
        "1ª;Ceilândia;100;120;20,0%;20",
        "2ª;Taguatinga;50;80;60,0%;30",
    ]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    tratar_crimes_idosos_ranking(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    assert len(resultado) == 2  # header não numérico foi filtrado
    assert resultado["ranking"].tolist() == [1, 2]
    assert resultado["variacao_absoluta"].tolist() == [20, 30]
    assert resultado["variacao_percentual"].tolist() == [20.0, 60.0]


# ============================================================
# tratar_crimes_idosos_por_mes
# ============================================================
def _csv_idosos_por_mes(tmp_path):
    entrada = tmp_path / "entrada.csv"
    linhas = [
        "Tabela 2 - Crimes Registrados;;",
        "JAN;10;15",
        "FEV;5;8",
        "Tabela 3 - Crimes Fato;;",
        "JAN;20;25",
        "FEV;12;18",
    ]
    entrada.write_text("\n".join(linhas), encoding="latin1")
    return entrada


def test_tratar_crimes_idosos_por_mes_tipo_registro(tmp_path):
    entrada = _csv_idosos_por_mes(tmp_path)

    resultado = tratar_crimes_idosos_por_mes(str(entrada), "registro")

    assert set(resultado["mes"].unique()) == {"JAN", "FEV"}
    assert set(resultado["ano"].unique()) == {2016, 2017}
    assert (resultado["tipo"] == "registro").all()
    assert len(resultado) == 4  # 2 meses x 2 anos

    linha_jan_2016 = resultado[(resultado["mes"] == "JAN") & (resultado["ano"] == 2016)]
    assert linha_jan_2016["ocorrencias"].iloc[0] == 10


def test_tratar_crimes_idosos_por_mes_tipo_fato(tmp_path):
    entrada = _csv_idosos_por_mes(tmp_path)

    resultado = tratar_crimes_idosos_por_mes(str(entrada), "fato")

    assert (resultado["tipo"] == "fato").all()
    assert len(resultado) == 4

    linha_jan_2016 = resultado[(resultado["mes"] == "JAN") & (resultado["ano"] == 2016)]
    assert linha_jan_2016["ocorrencias"].iloc[0] == 20


def test_tratar_crimes_idosos_por_mes_tipo_invalido_levanta_erro(tmp_path):
    entrada = _csv_idosos_por_mes(tmp_path)

    with pytest.raises(ValueError, match="registro' ou 'fato'"):
        tratar_crimes_idosos_por_mes(str(entrada), "invalido")


# ============================================================
# crimes_idosos_por_mes
# ============================================================
def test_crimes_idosos_por_mes_fluxo_completo(tmp_path):
    entrada = _csv_idosos_por_mes(tmp_path)
    saida = tmp_path / "saida.csv"

    crimes_idosos_por_mes(str(entrada), ["registro", "fato"], str(saida))

    resultado = pd.read_csv(saida, sep=";")

    assert "subnotificacao" in resultado.columns
    assert set(resultado.columns) >= {"ano", "mes", "mes_num", "registro", "fato", "subnotificacao"}
    assert len(resultado) == 4  # 2 meses x 2 anos

    linha_jan_2016 = resultado[(resultado["mes"] == "JAN") & (resultado["ano"] == 2016)]
    assert linha_jan_2016["registro"].iloc[0] == 10
    assert linha_jan_2016["fato"].iloc[0] == 20
    assert linha_jan_2016["subnotificacao"].iloc[0] == -10


# ============================================================
# tratar_injuria_racial_por_regiao
# ============================================================
def test_tratar_injuria_racial_por_regiao_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "x0;titulo;;;;;;;;;;;",
        "x1;Regiao;2015;2016;2017;2018;2019;2020;2021;2022;2023;2024;Variacao 2015-2024",
        "x2;Ceilandia;10;11;12;13;14;15;16;17;18;19;999",
        "x3;Total Geral;999;999;999;999;999;999;999;999;999;999;999",
        "x4;Fonte: SSP-DF;;;;;;;;;;;",
    ]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    tratar_injuria_racial_por_regiao(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    assert "variacao_2015_2024" not in resultado.columns
    assert len(resultado) == 1
    assert resultado["regiao"].tolist() == ["Ceilandia"]
    assert resultado["2015"].iloc[0] == 10
    assert resultado["2024"].iloc[0] == 19


def test_tratar_injuria_racial_por_regiao_sem_header_levanta_erro(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = ["x0;dados sem ano nenhum;a;b;c"]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    with pytest.raises(ValueError, match="Header não encontrado"):
        tratar_injuria_racial_por_regiao(str(entrada), str(saida))


# ============================================================
# tratar_latrocinio_por_regiao
# ============================================================
def test_tratar_latrocinio_por_regiao_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "x0;titulo;;;;;;;;;;",
        "x1;Regiao;2015;2016;2017;2018;2019;2020;2021;2022;2023;2024",
        "x2;Ceilandia;1;2;3;4;5;6;7;8;9;10",
        "x3;Taguatinga;-;0;nan;None;1;2;3;4;5;6",
    ]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    tratar_latrocinio_por_regiao(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    assert len(resultado) == 2
    linha_taguatinga = resultado[resultado["regiao"] == "Taguatinga"]
    # valores inválidos ("-", "nan", "None") viram 0
    assert linha_taguatinga["2015"].iloc[0] == 0
    assert linha_taguatinga["2017"].iloc[0] == 0
    assert linha_taguatinga["2018"].iloc[0] == 0
    assert linha_taguatinga["2019"].iloc[0] == 1


def test_tratar_latrocinio_por_regiao_sem_header_levanta_erro(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = ["x0;dados sem ano nenhum;a;b;c"]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    with pytest.raises(ValueError, match="Header não encontrado"):
        tratar_latrocinio_por_regiao(str(entrada), str(saida))


# ============================================================
# tratar_lesao_corporal_morte_por_regiao
# ============================================================
def test_tratar_lesao_corporal_morte_por_regiao_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "c0;c1;c2;c3;c4;c5;c6;c7;c8;c9;c10",
        "Regiao;2015;2016;2017;2018;2019;2020;2021;2022;2023;2024",
        "Ceilandia;1;2;3;4;5;6;7;8;9;10",
        "Região Administrativa;1;1;1;1;1;1;1;1;1;1",
        "Taguatinga;-;0;nan;None;1;2;3;4;5;6",
    ]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    tratar_lesao_corporal_morte_por_regiao(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    assert "Região Administrativa" not in resultado["regiao"].tolist()
    assert len(resultado) == 2
    assert resultado["regiao"].tolist() == ["Ceilandia", "Taguatinga"]

    linha_taguatinga = resultado[resultado["regiao"] == "Taguatinga"]
    assert linha_taguatinga["2015"].iloc[0] == 0
    assert linha_taguatinga["2017"].iloc[0] == 0
    assert linha_taguatinga["2018"].iloc[0] == 0


def test_tratar_lesao_corporal_morte_por_regiao_sem_header_levanta_erro(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "c0;c1;c2",
        "dados;sem;ano_nenhum",
    ]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    with pytest.raises(ValueError, match="Header não encontrado"):
        tratar_lesao_corporal_morte_por_regiao(str(entrada), str(saida))


# ============================================================
# tratar_lesao_corporal_morte
# ============================================================
def test_tratar_lesao_corporal_morte_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "x0;titulo;;;;;;;;;;",
        "x1;Regiao;2015;2016;2017;2018;2019;2020;2021;2022;2023;2024",
        "x2;Ceilandia;1;2;3;4;5;6;7;8;9;10",
        "x3;Região Administrativa;1;1;1;1;1;1;1;1;1;1",
        "x4;Taguatinga;-;0;nan;None;1;2;3;4;5;6",
    ]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    tratar_lesao_corporal_morte(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    assert "Região Administrativa" not in resultado["regiao"].tolist()
    assert len(resultado) == 2
    linha_taguatinga = resultado[resultado["regiao"] == "Taguatinga"]
    assert linha_taguatinga["2015"].iloc[0] == 0
    assert linha_taguatinga["2017"].iloc[0] == 0


def test_tratar_lesao_corporal_morte_sem_header_levanta_erro(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = ["x0;dados sem ano nenhum;a;b;c"]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    with pytest.raises(ValueError, match="Header não encontrado"):
        tratar_lesao_corporal_morte(str(entrada), str(saida))


# ============================================================
# tratar_racismo
# ============================================================
def test_tratar_racismo_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "x0;titulo;;;;;;;;;;",
        "x1;Regiao;2015;2016;2017;2018;2019;2020;2021;2022;2023;2024",
        "x2;Ceilandia;1;2;3;4;5;6;7;8;9;10",
        "x3;AB;1;1;1;1;1;1;1;1;1;1",
        "x4;Região Administrativa;1;1;1;1;1;1;1;1;1;1",
        "x5;Taguatinga;-;*;;nan;None;1;2;3;4",
    ]
    # Escrito em utf-8: a função lê com encoding="latin1" (simula o mojibake
    # real que o encode('latin1').decode('utf-8') do código foi feito para
    # corrigir) — por isso "Região Administrativa" chega intacta ao filtro.
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    tratar_racismo(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    assert len(resultado) == 2  # "AB" (len<=2) e "Região Administrativa" removidos
    assert set(resultado["regiao"]) == {"Ceilandia", "Taguatinga"}

    linha_taguatinga = resultado[resultado["regiao"] == "Taguatinga"]
    assert linha_taguatinga["2015"].iloc[0] == 0
    assert linha_taguatinga["2016"].iloc[0] == 0


def test_tratar_racismo_sem_header_levanta_erro(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = ["x0;dados sem ano nenhum;a;b;c"]
    entrada.write_text("\n".join(linhas), encoding="latin1")

    with pytest.raises(ValueError, match="Header não encontrado"):
        tratar_racismo(str(entrada), str(saida))


# ============================================================
# tratar_roubo_pedestre
# ============================================================
def test_tratar_roubo_pedestre_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "h0;h1;h2;h3;h4;h5;h6;h7;h8;h9;h10",
        "Regiao;2015;2016;2017;2018;2019;2020;2021;2022;2023;2024",
        "Ceilandia;1;2;3;4;5;6;7;8;9;10",
        "AB;1;1;1;1;1;1;1;1;1;1",
        "Taguatinga;-;*;1;2;3;4;5;6;7",
    ]
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    tratar_roubo_pedestre(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    assert len(resultado) == 2  # "AB" (len<=2) removida
    assert set(resultado["Região Administrativa"]) == {"Ceilandia", "Taguatinga"}

    linha_taguatinga = resultado[resultado["Região Administrativa"] == "Taguatinga"]
    assert linha_taguatinga["2015"].iloc[0] == 0
    assert linha_taguatinga["2016"].iloc[0] == 0


def test_tratar_roubo_pedestre_sem_header_levanta_erro(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = ["h0;h1;h2", "dados;sem;ano"]
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    with pytest.raises(ValueError, match="Header não encontrado"):
        tratar_roubo_pedestre(str(entrada), str(saida))


# ============================================================
# tratar_roubo_veiculo (com coluna extra além de 2024, testa o corte)
# ============================================================
def test_tratar_roubo_veiculo_fluxo_completo_e_corta_coluna_extra(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "h0;h1;h2;h3;h4;h5;h6;h7;h8;h9;h10;h11",
        "Regiao;2015;2016;2017;2018;2019;2020;2021;2022;2023;2024;2025",
        "Ceilandia;1;2;3;4;5;6;7;8;9;10;999",
        "AB;1;1;1;1;1;1;1;1;1;1;999",
        "Taguatinga;-;*;1;2;3;4;5;6;7;8;999",
    ]
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    tratar_roubo_veiculo(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")

    assert "2025" not in resultado.columns  # coluna extra além de 2024 foi cortada
    assert len(resultado) == 2
    linha_taguatinga = resultado[resultado["Região Administrativa"] == "Taguatinga"]
    assert linha_taguatinga["2015"].iloc[0] == 0
    assert linha_taguatinga["2024"].iloc[0] == 8


def test_tratar_roubo_veiculo_sem_header_levanta_erro(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = ["h0;h1;h2", "dados;sem;ano"]
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    with pytest.raises(ValueError, match="Header não encontrado"):
        tratar_roubo_veiculo(str(entrada), str(saida))


# ============================================================
# roubo_comercio
# ============================================================
def test_roubo_comercio_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "h0;h1;h2;h3;h4;h5;h6;h7;h8;h9;h10",
        "Regiao;2015;2016;2017;2018;2019;2020;2021;2022;2023;2024",
        "Ceilandia;1;2;3;4;5;6;7;8;9;10",
        "AB;1;1;1;1;1;1;1;1;1;1",
        "Taguatinga;-;*;1;2;3;4;5;6;7",
    ]
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    roubo_comercio(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")
    assert len(resultado) == 2
    assert set(resultado["Região Administrativa"]) == {"Ceilandia", "Taguatinga"}


def test_roubo_comercio_sem_header_levanta_erro(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = ["h0;h1;h2", "dados;sem;ano"]
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    with pytest.raises(ValueError, match="Header não encontrado"):
        roubo_comercio(str(entrada), str(saida))


# ============================================================
# roubo_transporte_coletivo
# ============================================================
def test_roubo_transporte_coletivo_fluxo_completo(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = [
        "h0;h1;h2;h3;h4;h5;h6;h7;h8;h9;h10",
        "Regiao;2015;2016;2017;2018;2019;2020;2021;2022;2023;2024",
        "Ceilandia;1;2;3;4;5;6;7;8;9;10",
        "AB;1;1;1;1;1;1;1;1;1;1",
        "Taguatinga;-;*;1;2;3;4;5;6;7",
    ]
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    roubo_transporte_coletivo(str(entrada), str(saida))

    resultado = pd.read_csv(saida, sep=";")
    assert len(resultado) == 2
    assert set(resultado["Região Administrativa"]) == {"Ceilandia", "Taguatinga"}


def test_roubo_transporte_coletivo_sem_header_levanta_erro(tmp_path):
    entrada = tmp_path / "entrada.csv"
    saida = tmp_path / "saida.csv"

    linhas = ["h0;h1;h2", "dados;sem;ano"]
    entrada.write_text("\n".join(linhas), encoding="utf-8")

    with pytest.raises(ValueError, match="Header não encontrado"):
        roubo_transporte_coletivo(str(entrada), str(saida))
