from pathlib import Path

from src.config import paths


def test_paths_sao_derivados_do_base_dir():
    assert isinstance(paths.BASE_DIR, Path)
    assert paths.DATA_DIR == paths.BASE_DIR / "data"
    assert paths.BRONZE_DIR == paths.DATA_DIR / "bronze"
    assert paths.CSV_BRONZE == paths.BRONZE_DIR / "csv"
    assert paths.PLANILHAS == paths.BRONZE_DIR / "planilhas"
    assert paths.ZIP == paths.BRONZE_DIR / "zip"
    assert paths.SILVER_DIR == paths.DATA_DIR / "silver"
    assert paths.OUTPUT_SILVER == paths.SILVER_DIR / "csv"
