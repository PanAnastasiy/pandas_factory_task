from pathlib import Path
from typing import Any, List

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import text

from config import settings
from core.repositories.base import BaseRepository
from core.services.base import BaseMaterialService


class MaterialETLService(BaseMaterialService):
    """
    Implements ETL operations for raw material data and BOM reporting.
    """

    def __init__(self, repository: BaseRepository):
        """
        Initializes the service with a repository for database operations.
        """

        self.repository = repository

    def _read_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Reads a CSV file into a DataFrame and strips whitespace from column names.
        """

        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        logger.debug(f"Reading CSV file: {file_path}")
        df = pd.read_csv(file_path)

        df.columns = df.columns.str.strip()
        return df

    def _transform_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renames DataFrame columns according to a predefined mapping in settings.
        """

        for old_name in settings.RENAME_MAP.keys():
            if old_name not in df.columns:
                logger.warning(
                    f"Expected column '{old_name}' not found in CSV. "
                    f"Available: {list(df.columns)}"
                )

        return df.rename(columns=settings.RENAME_MAP)

    def _clean_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans data types: converts IDs to strings, quantities to floats, handles NaNs.
        """

        logger.debug("Cleaning data types...")

        id_cols = getattr(
            settings,
            "ID_COLUMNS",
            [
                "plant_id",
                "produced_material_id",
                "component_material_id",
                "produced_material_release_type",
                "produced_material_production_type",
                "component_material_release_type",
                "component_material_production_type",
            ],
        )

        for col in id_cols:
            if col in df.columns:

                df[col] = df[col].fillna("").astype(str)

                mask_bad = df[col].str.lower().isin(["nan", "none", "null", ""])

                if col in ["produced_material_id", "plant_id"]:
                    if mask_bad.any():
                        count = mask_bad.sum()
                        logger.warning(
                            f"Column '{col}': removed {count} empty/invalid rows."
                        )
                        df = df[~mask_bad]

        numeric_cols = ["produced_material_quantity", "component_material_quantity"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .apply(pd.to_numeric, errors="coerce")
                    .fillna(0.0)
                )

        if "month" in df.columns:
            df["month"] = (
                pd.to_numeric(df["month"], errors="coerce").fillna(0).astype(int)
            )
            if (df["month"] == 0).any():
                logger.warning("Found rows with invalid 'month'. Removing them.")
                df = df[df["month"] > 0]

        df = df.replace({np.nan: None})

        return df

    def run_import_pipeline(self, file_path: Path = settings.INPUT_CSV_PATH) -> int:
        """
        Executes the full ETL pipeline: extract, transform, clean, and load raw data.
        Aggregation happens later in SQL.
        """

        logger.info("Starting ETL pipeline...")

        try:
            # 1. Extract
            logger.info("Step 1: Extract")
            df = self._read_csv(file_path)

            # 2. Transform & Clean
            logger.info("Step 2: Transform & Clean")
            df = self._transform_columns(df)
            df = self._clean_data_types(df)

            required_cols = ["produced_material_id", "month", "year"]
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                msg = f"Validation Error: Columns {missing} are missing."
                logger.critical(msg)
                raise ValueError(msg)

            # 3. Load
            records = df.to_dict(orient="records")
            logger.info(f"Step 3: Load ({len(records)} raw rows)")

            self.repository.truncate_table()
            self.repository.bulk_insert(records)

            logger.success(f"ETL finished successfully. Rows loaded: {len(records)}")
            return len(records)

        except Exception as e:
            logger.exception("Critical error in ETL pipeline")
            raise e

    def generate_bom_report(self) -> List[Any]:
        """
        1. Reads and executes the SQL BOM script (calculation & insertion).
        2. Selects and returns the calculated data from 'bom_reports' table.
        """

        if not settings.SQL_BOM_SCRIPT_PATH.exists():
            logger.error(f"SQL file not found: {settings.SQL_BOM_SCRIPT_PATH}")
            raise FileNotFoundError("SQL script not found")

        logger.info(f"Reading SQL script: {settings.SQL_BOM_SCRIPT_PATH.name}")
        with open(settings.SQL_BOM_SCRIPT_PATH, "r", encoding="utf-8") as f:
            calc_query = f.read()

        try:

            logger.info("Executing BOM calculation script...")
            self.repository.session.execute(text(calc_query))
            self.repository.session.commit()

            logger.info("Fetching generated BOM report...")
            select_query = """
                SELECT * 
                FROM bom_reports 
                ORDER BY plant, year, fin_material_id, component_id
            """

            return self.repository.execute_raw_sql(select_query)

        except Exception as e:
            logger.exception("Error generating BOM report")
            self.repository.session.rollback()
            raise e
