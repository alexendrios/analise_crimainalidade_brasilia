from unittest.mock import patch

import pandas as pd
import pytest

from validation.validator import validar_chaves, validar_colunas


@pytest.fixture
def df_valido():
    return pd.DataFrame(
        {
            "ano": [2020, 2021, 2020],
            "regiao_administrativa": ["Taguatinga", "Taguatinga", "Ceilândia"],
            "crimes": [10, 20, 30],
        }
    )


# ============================================================
# validar_chaves
# ============================================================
def test_validar_chaves_sem_chaves_apenas_avisa(df_valido):
    with patch("validation.validator.logger") as mock_logger:
        validar_chaves(df_valido, [])

    mock_logger.warning.assert_called_once()
    mock_logger.error.assert_not_called()


def test_validar_chaves_sem_duplicidade_nao_levanta_erro(df_valido):
    with patch("validation.validator.logger") as mock_logger:
        validar_chaves(df_valido, ["ano", "regiao_administrativa"])

    mock_logger.info.assert_called()
    mock_logger.error.assert_not_called()


def test_validar_chaves_com_duplicidade_levanta_value_error(df_valido):
    df = pd.concat([df_valido, df_valido.iloc[[0]]], ignore_index=True)

    with patch("validation.validator.logger") as mock_logger:
        with pytest.raises(ValueError, match="Duplicidade encontrada nas chaves"):
            validar_chaves(df, ["ano", "regiao_administrativa"])

    mock_logger.error.assert_called_once()
    extra = mock_logger.error.call_args.kwargs["extra"]
    assert extra["keys"] == ["ano", "regiao_administrativa"]
    assert extra["quantidade_duplicados"] == 2
    assert len(extra["exemplo"]) == 2


def test_validar_chaves_duplicidade_em_subconjunto_de_chaves_levanta_erro(df_valido):
    df = pd.concat([df_valido, df_valido.iloc[[0]]], ignore_index=True)

    with patch("validation.validator.logger"):
        with pytest.raises(ValueError, match="Quantidade: 3"):
            validar_chaves(df, ["regiao_administrativa"])


# ============================================================
# validar_colunas
# ============================================================
def test_validar_colunas_sem_colunas_apenas_avisa(df_valido):
    with patch("validation.validator.logger") as mock_logger:
        validar_colunas(df_valido, [])

    mock_logger.warning.assert_called_once()
    mock_logger.error.assert_not_called()


def test_validar_colunas_todas_presentes_nao_levanta_erro(df_valido):
    with patch("validation.validator.logger") as mock_logger:
        validar_colunas(df_valido, ["ano", "crimes"])

    mock_logger.info.assert_called()
    mock_logger.error.assert_not_called()


def test_validar_colunas_faltando_levanta_value_error(df_valido):
    with patch("validation.validator.logger") as mock_logger:
        with pytest.raises(ValueError, match="Colunas faltando"):
            validar_colunas(df_valido, ["ano", "coluna_inexistente"])

    mock_logger.error.assert_called_once()
    extra = mock_logger.error.call_args.kwargs["extra"]
    assert extra["faltando"] == ["coluna_inexistente"]


def test_validar_colunas_faltando_multiplas_reporta_todas(df_valido):
    with patch("validation.validator.logger") as mock_logger:
        with pytest.raises(ValueError) as excinfo:
            validar_colunas(df_valido, ["a", "b", "c"])

    mensagem = str(excinfo.value)
    assert "'a'" in mensagem and "'b'" in mensagem and "'c'" in mensagem
    extra = mock_logger.error.call_args.kwargs["extra"]
    assert set(extra["faltando"]) == {"a", "b", "c"}
