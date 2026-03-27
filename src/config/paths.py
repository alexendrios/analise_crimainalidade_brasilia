from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
CSV_BRONZE = BRONZE_DIR / "csv"
PLANILHAS = BRONZE_DIR / "planilhas"
ZIP = BRONZE_DIR / "zip"

SILVER_DIR = DATA_DIR / "silver"
OUTPUT_SILVER = SILVER_DIR / "csv"

