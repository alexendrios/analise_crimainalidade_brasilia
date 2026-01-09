import pytest
from unittest.mock import MagicMock, patch
from unittest.mock import Mock
import requests

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
        
@pytest.fixture
def mock_response():
    def _mock(html: str, status_code: int = 200):
        response = Mock(spec=requests.Response)
        response.status_code = status_code
        response.text = html
        response.raise_for_status.return_value = None
        return response

    return _mock
