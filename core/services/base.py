from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List


class BaseMaterialService(ABC):

    @abstractmethod
    def run_import_pipeline(self, file_path: Path) -> int:
        pass

    @abstractmethod
    def generate_bom_report(self) -> List[Any]:
        pass
