import pandas as pd
from src.scraping import normalizar_df


def test_normalizar_df():
    df = pd.DataFrame(
        [["Plano Piloto", "35.000 habitantes"]],
        columns=["região administrativa", "população"],
    )

    df_norm = normalizar_df(df)

    assert list(df_norm.columns) == ["região administrativa", "população"]
    assert df_norm.loc[0, "população"] == "35000"
