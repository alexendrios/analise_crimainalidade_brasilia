# tests/test_config_loader.py

import os
import pytest
from unittest.mock import patch, mock_open
import builtins
import util.config_loader as cfg

# -----------------------------
# TESTES load_yaml
# -----------------------------

def test_load_yaml_sucesso():
    conteudo_yaml = "chave: valor"
    m = mock_open(read_data=conteudo_yaml)
    with patch("builtins.open", m):
        with patch("os.path.exists", return_value=True):
            resultado = cfg.load_yaml("fake_path.yaml")
            import yaml
            assert resultado == yaml.safe_load(conteudo_yaml)

def test_load_yaml_arquivo_inexistente():
    with patch("os.path.exists", return_value=False):
        with pytest.raises(FileNotFoundError):
            cfg.load_yaml("nao_existe.yaml")

# -----------------------------
# TESTES override_with_env
# -----------------------------

def test_override_with_env_sobrescreve():
    cfg_dict = {"database": {"host": "localhost", "port": "5432"}}
    with patch.dict(os.environ, {"DATABASE_HOST": "env_host"}):
        cfg.override_with_env(cfg_dict)
        assert cfg_dict["database"]["host"] == "env_host"
        assert cfg_dict["database"]["port"] == "5432"

def test_override_with_env_multinivel():
    cfg_dict = {"a": {"b": {"c": "valor"}}}
    with patch.dict(os.environ, {"A_B_C": "novo_valor"}):
        cfg.override_with_env(cfg_dict)
        assert cfg_dict["a"]["b"]["c"] == "novo_valor"

def test_override_with_env_sem_env():
    cfg_dict = {"key": "valor"}
    cfg.override_with_env(cfg_dict)
    assert cfg_dict["key"] == "valor"

# -----------------------------
# TESTES load_config
# -----------------------------

@patch("util.config_loader.load_yaml")
@patch("util.config_loader.override_with_env")
@patch("util.config_loader.load_dotenv")
def test_load_config_completo(mock_dotenv, mock_override, mock_load_yaml):
    # Mock para os YAMLs
    mock_load_yaml.side_effect = [
        {"app": "teste"},  # config.yaml
        {"rotas": {"r1": "v1"}}  # rotas.yaml
    ]

    resultado = cfg.load_config()

    mock_dotenv.assert_called_once()
    assert resultado["app"] == "teste"
    assert resultado["rotas"] == {"r1": "v1"}
    mock_override.assert_called_once_with(resultado)

# -----------------------------
# TESTE get_config
# -----------------------------

def test_get_config_retorna_instancia_global():
    cfg_global = cfg.get_config()
    assert isinstance(cfg_global, dict)
    assert "rotas" in cfg_global
