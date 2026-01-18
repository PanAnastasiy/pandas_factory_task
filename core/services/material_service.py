from pathlib import Path
from typing import Any, List

import numpy as np
import pandas as pd
from loguru import logger

from config import settings
from core.repositories.base import BaseRepository
from core.services.base import BaseMaterialService


class MaterialETLService(BaseMaterialService):

    def __init__(self, repository: BaseRepository):
        self.repository = repository

    def _read_csv(self, file_path: Path) -> pd.DataFrame:
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        logger.debug(f"Reading CSV file: {file_path}")
        df = pd.read_csv(file_path)

        df.columns = df.columns.str.strip()
        return df

    def _transform_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for old_name, new_name in settings.RENAME_MAP.items():
            if old_name not in df.columns:
                logger.warning(
                    f"Column '{old_name}' not found in CSV. Available columns: {list(df.columns)}"
                )

        return df.rename(columns=settings.RENAME_MAP)

    def _clean_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.debug("Cleaning data types...")

        for col in settings.ID_COLUMNS:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)

                mask_bad = df[col].isin(["", "nan", "None"])
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
                    .str.replace(",", "")
                    .apply(pd.to_numeric, errors="coerce")
                )

        df = df.replace({np.nan: None})

        return df

    def _deduplicate_by_year(self, df: pd.DataFrame) -> pd.DataFrame:
        subset_cols = [
            "year",
            "plant_id",
            "produced_material_id",
            "component_material_id",
        ]
        actual_subset = [c for c in subset_cols if c in df.columns]

        initial_len = len(df)
        df_dedup = df.drop_duplicates(subset=actual_subset)
        dropped_len = initial_len - len(df_dedup)

        if dropped_len > 0:
            logger.info(f"Duplicates removed (year aggregation): {dropped_len}")

        return df_dedup

    def run_import_pipeline(self, file_path: Path = settings.INPUT_CSV_PATH) -> int:
        logger.info("Starting ETL pipeline...")

        try:
            logger.info("Step 1: Extract")
            df = self._read_csv(file_path)

            logger.info("Step 2: Transform")
            df = self._transform_columns(df)
            df = self._clean_data_types(df)
            df = self._deduplicate_by_year(df)

            required_col = "produced_material_id"
            if required_col not in df.columns:
                msg = f"Validation Error: Column '{required_col}' is missing."
                logger.critical(msg)
                raise ValueError(msg)

            records = df.to_dict(orient="records")

            logger.info(f"Step 3: Load ({len(records)} rows)")
            self.repository.truncate_table()
            self.repository.bulk_insert(records)

            logger.success(f"ETL finished successfully. Rows loaded: {len(records)}")
            return len(records)

        except Exception as e:
            logger.exception("Critical error in ETL pipeline")
            raise e

    def generate_bom_report(self) -> List[Any]:
        if not settings.SQL_BOM_SCRIPT_PATH.exists():
            logger.error(f"SQL file not found: {settings.SQL_BOM_SCRIPT_PATH}")
            raise FileNotFoundError("SQL script not found")

        logger.info(f"Executing SQL script: {settings.SQL_BOM_SCRIPT_PATH.name}")
        with open(settings.SQL_BOM_SCRIPT_PATH, "r", encoding="utf-8") as f:
            sql_query = f.read()

        return self.repository.execute_raw_sql(sql_query)
