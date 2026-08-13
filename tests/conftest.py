import pytest
import pandas
from unittest.mock import MagicMock, patch, Mock, mock_open
import requests


@pytest.fixture
def mock_requests_get():
    with patch("requests.get") as mock:
        yield mock


@pytest.fixture
def mock_session_factory():
    """
    Fixture para simular `requests.Session()` usado como context manager em
    `util.arquivos.download_arquivo` (`with requests.Session() as session: ...`).

    IMPORTANTE: `download_arquivo` NÃO chama `requests.get` diretamente — ele
    cria uma sessão por tentativa (`with requests.Session() as session:
    session.get(...)`). Fazer mock de `requests.get` não intercepta nada
    nesse fluxo (o teste acaba batendo na rede de verdade). Este fixture
    substitui `requests.Session` por um mock cujo `.get()` (via context
    manager) retorna a `response` combinada, ou levanta `get_side_effect`
    se a própria chamada de `.get()` deve falhar.

    Uso:
        mock_session_cls = mock_session_factory(response=meu_response)
        with patch("util.arquivos.requests.Session", mock_session_cls):
            ...
    """

    def _factory(response=None, get_side_effect=None):
        mock_session = MagicMock()
        if get_side_effect is not None:
            mock_session.get.side_effect = get_side_effect
        else:
            mock_session.get.return_value = response

        mock_session_cls = MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session
        mock_session_cls.return_value.__exit__.return_value = False
        return mock_session_cls

    return _factory


@pytest.fixture
def mock_tqdm():
    with patch("tqdm.tqdm") as mock:
        mock.return_value.__enter__.return_value.update = MagicMock()
        yield mock


@pytest.fixture
def mock_response():
    def _mock(html: str, status_code: int = 200):
        response = Mock(spec=requests.Response)
        response.status_code = status_code
        response.text = html
        response.raise_for_status.return_value = None
        return response

    return _mock


@pytest.fixture
def df_sem_header():
    return pandas.DataFrame(
        [
            ["Região A", "10", "20"],
            ["Região B", "30", "40"],
        ]
    )


def assert_header_nao_encontrado(func, entrada, saida=None):
    with patch("src.tratamento_crimes.logger.error") as mock_logger_error:
        with pytest.raises(ValueError, match="Header não encontrado no CSV"):
            if saida:
                func(entrada, saida)
            else:
                func(entrada)

        mock_logger_error.assert_called_once_with(
            "Header não encontrado no arquivo %s",
            entrada,
        )

@pytest.fixture
def mock_csv():
    """
    Fixture para mockar a função open e simular leitura de CSV/Tabelas.
    Uso:
        with mock_csv(linhas):
            tratar_violencia_idosos("entrada.csv", ["saida_t4.csv", "saida_t5.csv"])
    """

    def _mock_csv(linhas):
        # Concatena as linhas simulando o arquivo
        conteudo = "\n".join(linhas)
        m = mock_open(read_data=conteudo)
        return patch("builtins.open", m)

    return _mock_csv