from unittest.mock import patch


# ============================================================
# TESTE PRINCIPAL — fluxo completo
# ============================================================


@patch("src.busca.coleta_dados")
@patch("src.busca.limpar_diretorios")
@patch("src.busca.logger")
@patch("src.busca.config")
def test_coletar_dados_fluxo_completo(
    mock_config,
    mock_logger,
    mock_limpar,
    mock_coleta,
):
    # Config fake
    mock_config.__getitem__.side_effect = lambda key: {
        "rotas": {"r1": {}},
        "rotas_ibge": {"r2": {}},
        "coleta": {
            "fontes": {
                "dados_abertos": {"url": "http://abertos"},
                "dados_ibge": {"url": "http://ibge"},
            }
        },
    }[key]

    from src.busca import coletar_dados_

    coletar_dados_()

    # Asserções
    mock_limpar.assert_called_once()
    assert mock_coleta.call_count == 2

    chamadas = mock_coleta.call_args_list

    assert chamadas[0].args[0] == "http://abertos"
    assert chamadas[1].args[0] == "http://ibge"

    assert mock_logger.info.call_count > 0

# ============================================================
# TESTE — ordem de execução
# ============================================================
@patch("src.busca.coleta_dados")
@patch("src.busca.limpar_diretorios")
@patch("src.busca.logger")
@patch("src.busca.config")
def test_coletar_dados_ordem_execucao(
    mock_config,
    mock_logger,
    mock_limpar,
    mock_coleta,
):
    # Config mínima necessária para não quebrar
    mock_config.__getitem__.side_effect = lambda key: {
        "rotas": {},
        "rotas_ibge": {},
        "coleta": {
            "fontes": {
                "dados_abertos": {"url": "u1"},
                "dados_ibge": {"url": "u2"},
            }
        },
    }[key]

    from src.busca import coletar_dados_

    coletar_dados_()

    assert mock_limpar.call_count == 1
    assert mock_coleta.call_count == 2


# ============================================================
# TESTE — logs principais
# ============================================================
@patch("src.busca.coleta_dados")
@patch("src.busca.limpar_diretorios")
@patch("src.busca.logger")
@patch("src.busca.config")
def test_coletar_dados_logs(
    mock_config,
    mock_logger,
    mock_limpar,
    mock_coleta,
):
    mock_config.__getitem__.side_effect = lambda key: {
        "rotas": {},
        "rotas_ibge": {},
        "coleta": {
            "fontes": {
                "dados_abertos": {"url": "u1"},
                "dados_ibge": {"url": "u2"},
            }
        },
    }[key]

    from src.busca import coletar_dados_

    coletar_dados_()

    mensagens = [c.args[0] for c in mock_logger.info.call_args_list]

    assert any("Processo geral iniciado" in m for m in mensagens)
    assert any("Todas as coletas de dados concluídas" in m for m in mensagens)
