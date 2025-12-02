import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_requests_get():
    """Mock para requests.get."""
    with patch("requests.get") as mock:
        yield mock


@pytest.fixture
def mock_tqdm():
    """Mock para evitar barra de progresso real nos testes."""
    with patch("tqdm.tqdm") as mock:
        mock.return_value.__enter__.return_value.update = MagicMock()
        yield mock
