import pandas as pd
import pytest

from validation.schema import (
    DATA,
    NUMERICO,
    TEXTO,
    ErroSchema,
    EsquemaTabela,
    resumo_esquemas,
    validador_de_esquema,
    validador_multi,
    validar_schema,
)
from validation.esquemas import (
    GOLD,
    SILVER,
    SAIDAS_SILVER_CSV,
    validador_silver,
    validar_saida_silver,
)

MODULO = "validation.schema"


@pytest.fixture
def pasta_silver_tmp(monkeypatch, tmp_path):
    """Redireciona a pasta silver para tmp (testes não leem o disco real)."""
    import validation.esquemas as modulo_esquemas

    monkeypatch.setattr(modulo_esquemas, "PASTA_SILVER_OUTPUT", tmp_path)
    return tmp_path


def _esquema():
    return EsquemaTabela(
        nome="teste",
        colunas={"nome": TEXTO, "ano": NUMERICO, "data": DATA},
        chaves=("nome",),
    )


def _df_valido():
    return pd.DataFrame(
        {
            "nome": ["A", "B"],
            "ano": [2020, 2021],
            "data": pd.to_datetime(["2020-01-01", "2021-01-01"]),
        }
    )


# ============================================================
# validar_schema
# ============================================================
def test_schema_valido_passa():
    validar_schema(_df_valido(), _esquema())


def test_coluna_obrigatoria_ausente_levanta_erro_schema():
    df = _df_valido().drop(columns=["ano"])
    with pytest.raises(ErroSchema, match="ausentes"):
        validar_schema(df, _esquema())


def test_tipo_incompativel_levanta_erro_schema():
    df = _df_valido()
    df["ano"] = ["dois mil", "dois mil e um"]
    with pytest.raises(ErroSchema, match="deveria ser 'numerico'"):
        validar_schema(df, _esquema())


def test_nulos_em_obrigatorias_apenas_alertam():
    from unittest.mock import patch

    df = _df_valido()
    df.loc[0, "ano"] = None

    with patch(f"{MODULO}.logger.warning") as mock_warning:
        validar_schema(df, _esquema())

    mock_warning.assert_called_once()


def test_chave_duplicada_propaga_valueerror():
    df = pd.concat([_df_valido(), _df_valido().head(1)], ignore_index=True)
    with pytest.raises(ValueError, match="Duplicidade"):
        validar_schema(df, _esquema())


def test_esquema_com_tipo_invalido_rejeitado_na_criacao():
    with pytest.raises(ValueError, match="tipos inválidos"):
        EsquemaTabela(nome="quebrado", colunas={"a": "flutuante"})


def test_df_que_nao_e_dataframe_levanta_erro_schema():
    with pytest.raises(ErroSchema, match="esperado DataFrame"):
        validar_schema({"a": [1]}, _esquema())


# ============================================================
# Hooks
# ============================================================
def test_validador_de_esquema_valida_o_retorno_do_step():
    hook = validador_de_esquema(_esquema())
    hook(_df_valido())


def test_validador_de_esquema_aceita_retorno_none_com_aviso():
    from unittest.mock import patch

    hook = validador_de_esquema(_esquema())
    with patch(f"{MODULO}.logger.warning") as mock_warning:
        hook(None)
    mock_warning.assert_called_once()


def test_validador_multi_aplica_por_posicao():
    outro = EsquemaTabela(nome="outro", colunas={"x": NUMERICO})
    hook = validador_multi(_esquema(), outro)
    hook((_df_valido(), pd.DataFrame({"x": [1.5]})))

    with pytest.raises(ErroSchema):
        hook((_df_valido(), pd.DataFrame({"x": ["a"]})))


def test_resumo_esquemas_documenta_os_contratos():
    resumo = resumo_esquemas([_esquema()])
    assert resumo.iloc[0]["nome"] == "teste"
    assert resumo.iloc[0]["colunas_obrigatorias"] == 3


# ============================================================
# Registro de schemas (silver + gold)
# ============================================================
def test_registros_principais_estao_definidos():
    for chave in (
        "crimes_contra_mulher",
        "desaparecidos_regiao",
        "roubo_pedestre",
        "idosos_tabela4",
        "idosos_tabela5",
    ):
        assert chave in SILVER

    for tabela in (
        "violencia_contra_mulher_gold",
        "crimes_letais_gold",
        "violencia_idosos_mensais_gold",
    ):
        assert tabela in GOLD


def test_todos_os_csvs_mapeados_tem_schema():
    sem_schema = set(SAIDAS_SILVER_CSV) - set(SILVER)
    assert not sem_schema, f"CSVs sem schema: {sem_schema}"


def test_validador_silver_valida_dataframe_retornado(pasta_silver_tmp):
    validador = validador_silver("desaparecidos_idade_sexo")
    validador(
        pd.DataFrame(
            {"ano": [2020], "faixa_etaria": ["0 A 17"], "sexo": ["M"], "quantidade": [3]}
        )
    )

    with pytest.raises(ErroSchema):
        validador(pd.DataFrame({"coluna_errada": [1]}))


def test_validar_saida_silver_relendo_csv_gravado(pasta_silver_tmp):
    csv = pasta_silver_tmp / "desaparecidos_idade_sexo_tratado.csv"
    pd.DataFrame(
        {"ano": [2020], "faixa_etaria": ["0 A 17 ANOS"], "sexo": ["MASCULINO"], "quantidade": [6]}
    ).to_csv(csv, sep=";", index=False)

    validar_saida_silver("desaparecidos_idade_sexo", None)


def test_validar_saida_silver_arquivo_ausente_levanta_erro(pasta_silver_tmp):
    with pytest.raises(ErroSchema, match="não encontrado"):
        validar_saida_silver("homicidio", None)


def test_validar_saida_silver_step_sem_registro_apenas_avisa():
    from unittest.mock import patch

    with patch(f"{MODULO}.logger.warning") as mock_warning:
        validar_saida_silver("step_inexistente", None)
    mock_warning.assert_called_once()


def test_validar_saida_silver_violencia_idosos_valida_a_tupla():
    df_t4 = pd.DataFrame(
        {"ano": [2010], "ocorrencias": [55], "violencia_dentro_de_casa": [22]}
    )
    df_t5 = pd.DataFrame({"ano": [2010], "masculino": [36], "feminino": [34], "total": [70]})

    validar_saida_silver("violencia_idosos", (df_t4, df_t5))

    df_t4_quebrado = df_t4.drop(columns=["ocorrencias"])
    with pytest.raises(ErroSchema):
        validar_saida_silver("violencia_idosos", (df_t4_quebrado, df_t5))
