import yaml
import os
from dotenv import load_dotenv


def load_yaml(path: str):
    """
    Carrega um arquivo YAML com segurança.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def override_with_env(cfg, prefix=""):
    """
    Percorre recursivamente o dicionário do YAML e
    sobrescreve valores com variáveis de ambiente (.env).
    
    Regra:
    - CHAVE no YAML → procuramos ENV com nome UPPERCASE
      exemplo:
         database:
            host → DATABASE_HOST
            password → DATABASE_PASSWORD
    """
    for key, value in cfg.items():
        env_key = f"{prefix}{key}".upper()

        if isinstance(value, dict):
            override_with_env(value, prefix=f"{env_key}_")
        else:
            env_value = os.getenv(env_key)
            if env_value is not None:
                cfg[key] = env_value


def load_config():
    """
    Carrega:
      ✓ config.yaml
      ✓ rotas.yaml
      ✓ variáveis do .env
    e unifica em um único dicionário.
    """
    # Carrega variáveis de ambiente primeiro
    load_dotenv()

    # Carrega YAML principal
    config = load_yaml("config.yaml")

    # Carrega YAML secundário de rotas
    rotas_yaml = load_yaml("rotas.yaml")
    config["rotas"] = rotas_yaml.get("rotas", {})

    # Sobrepor com valores do .env
    override_with_env(config)

    return config


# Instância global (evita ficar lendo YAML toda execução)
config = load_config()


def get_config():
    return config
