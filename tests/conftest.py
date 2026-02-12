import pytest
import pandas
from unittest.mock import MagicMock, patch, Mock, mock_open
import requests


@pytest.fixture
def mock_requests_get():
    with patch("requests.get") as mock:
        yield mock


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