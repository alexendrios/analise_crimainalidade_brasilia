import numpy as np
import pytest
from src.tratamento_populacao_df_csv import mae, rmse, mape, smape


def test_mae_rmse_mape_smape():
    y_true = np.array([100, 200, 300])
    y_pred = np.array([110, 190, 310])

    assert mae(y_true, y_pred) == pytest.approx(10)
    assert rmse(y_true, y_pred) == pytest.approx(np.sqrt((10**2 + 10**2 + 10**2) / 3))
    assert mape(y_true, y_pred) == pytest.approx(
        (10 / 100 + 10 / 200 + 10 / 300) / 3 * 100, abs=0.01
    )
    assert smape(y_true, y_pred) == pytest.approx(
        (abs(100 - 110) / 105 + abs(200 - 190) / 195 + abs(300 - 310) / 305) / 3 * 100,
        abs=0.01,
    )
