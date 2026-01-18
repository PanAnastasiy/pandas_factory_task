from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List


class BaseMaterialService(ABC):
    """
    Defines a common interface for material ETL and reporting services.
    """

    @abstractmethod
    def run_import_pipeline(self, file_path: Path) -> int:
        """
        Runs the ETL pipeline to import data from a CSV file and returns the number of rows loaded.
        """

        pass

    @abstractmethod
    def generate_bom_report(self) -> List[Any]:
        """
        Generates a bill-of-materials report by executing a SQL query.
        """

        pass
