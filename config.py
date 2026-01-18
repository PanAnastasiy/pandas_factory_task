import os
import urllib.parse
from pathlib import Path


class Settings:
    BASE_DIR = Path(__file__).resolve().parent

    DATA_DIR = BASE_DIR / "resources" / "csv" / "raw"
    PROCESSED_DIR = BASE_DIR / "resources" / "csv" / "processed"
    SQL_DIR = BASE_DIR / "core" / "sql" / "procedures"
    INPUT_CSV_PATH = DATA_DIR / "factory_data.csv"
    SQL_BOM_SCRIPT_PATH = SQL_DIR / "bom_explosion.sql"

    POSTGRES_USER = os.getenv("POSTGRES_USER", "root")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "root")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5435")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "app")

    _user = urllib.parse.quote_plus(POSTGRES_USER)
    _pwd = urllib.parse.quote_plus(POSTGRES_PASSWORD)

    DB_URL = f"postgresql+psycopg://{_user}:{_pwd}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    RENAME_MAP = {
        "produced_material": "produced_material_id",
        "component_material": "component_material_id",
    }
    ID_COLUMNS = ["produced_material_id", "component_material_id", "plant_id"]


settings = Settings()
