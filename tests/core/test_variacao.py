import pytest
from src.tratamento_crimes import calcular_variacao_percentual


@pytest.mark.parametrize(
    "base, delta, esperado",
    [
        (100, 20, 20.0),
        (0, 5, 500.0),
        (0, -5, -100.0),
        (0, 0, 0.0),
    ],
)
def test_calcular_variacao_percentual(base, delta, esperado):
    assert calcular_variacao_percentual(base, delta) == esperado
