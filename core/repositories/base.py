from abc import ABC, abstractmethod


class BaseRepository(ABC):

    @abstractmethod
    def truncate_table(self) -> None:
        pass

    @abstractmethod
    def bulk_insert(self, data: list[dict[str, any]]) -> None:
        pass

    @abstractmethod
    def execute_raw_sql(self, sql_query: str) -> list[any]:
        pass
