from typing import Any, Dict, List

from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import Session

from core.models.raw_data import RawFactoryData
from core.repositories.base import BaseRepository


class RawDataRepository(BaseRepository):
    """
    Provides database operations for raw factory data records.
    """

    def __init__(self, session: Session):
        """
        Initializes the repository with an active SQLAlchemy session.
        """

        self.session = session

    def truncate_table(self) -> None:
        """
        Clears the raw factory data table and resets identity values if supported.
        """

        table_name = RawFactoryData.__tablename__
        logger.debug(f"Truncating table {table_name}...")
        try:
            self.session.execute(
                text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
            )
        except Exception:
            self.session.rollback()
            logger.debug(f"TRUNCATE not supported, using DELETE for {table_name}")
            self.session.execute(text(f"DELETE FROM {table_name};"))
        self.session.commit()

    def bulk_insert(self, data: List[Dict[str, Any]]) -> None:
        """
        Performs a bulk insert of raw factory data records.
        """

        if not data:
            logger.warning("No data to insert.")
            return

        logger.debug(f"Inserting batch of {len(data)} records...")
        self.session.bulk_insert_mappings(RawFactoryData, data)
        self.session.commit()

    def execute_raw_sql(self, sql_query: str) -> List[Any]:
        """
        Executes a raw SQL query and returns all fetched rows.
        """

        logger.debug("Executing raw SQL query...")
        result = self.session.execute(text(sql_query))
        return result.fetchall()
