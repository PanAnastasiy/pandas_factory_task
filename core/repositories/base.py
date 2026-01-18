from abc import ABC, abstractmethod


class BaseRepository(ABC):
    """
    Defines a common interface for database repository operations.
    """

    @abstractmethod
    def truncate_table(self) -> None:
        """
        Removes all records from the underlying table.
        """

        pass

    @abstractmethod
    def bulk_insert(self, data: list[dict[str, any]]) -> None:
        """
        Inserts multiple records into the underlying table in a single operation.
        """

        pass

    @abstractmethod
    def execute_raw_sql(self, sql_query: str) -> list[any]:
        """
        Executes a raw SQL query and returns the result.
        """

        pass
