import os
import pytest
from unittest.mock import patch, mock_open
import importlib
import util.config_loader as cfg


# -----------------------------
# TESTES load_yaml
# -----------------------------


def test_load_yaml_sucesso():
    conteudo_yaml = "chave: valor"

    with patch("os.path.exists", return_value=True):
        with patch("builtins.open", mock_open(read_data=conteudo_yaml)):
            resultado = cfg.load_yaml("fake.yaml")

    assert resultado == {"chave": "valor"}


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


@patch("util.config_loader.load_dotenv")
@patch("util.config_loader.override_with_env")
@patch("util.config_loader.load_yaml")
def test_load_config_completo(mock_load_yaml, mock_override, mock_dotenv):
    mock_load_yaml.side_effect = [
        {"app": "teste"},  # config.yaml
        {"rotas": {"r1": "v1"}},  # rotas.yaml
        {"rotas_ibge": {"ib1": "v2"}},  # ibge_rotas.yaml
    ]

    resultado = cfg.load_config()

    mock_dotenv.assert_called_once()
    mock_override.assert_called_once_with(resultado)

    assert resultado["app"] == "teste"
    assert resultado["rotas"] == {"r1": "v1"}
    assert resultado["rotas_ibge"] == {"ib1": "v2"}


# -----------------------------
# TESTES get_config
# -----------------------------


def test_get_config_retorna_dict():
    cfg_global = cfg.get_config()
    assert isinstance(cfg_global, dict)


def test_get_config_cache_estatico():
    cfg1 = cfg.get_config()
    cfg2 = cfg.get_config()

    assert cfg1 is cfg2  # mesma instância (cache real)

def test_get_config_reload_modulo():
    cfg1 = cfg.get_config()

    importlib.reload(cfg)
    cfg2 = cfg.get_config()

    assert cfg1 is not cfg2
