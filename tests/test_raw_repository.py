from unittest.mock import MagicMock

from core.models.raw_data import RawFactoryData
from core.repositories.raw_repository import RawDataRepository


def test_repository_truncate_postgres_style():
    mock_session = MagicMock()
    repo = RawDataRepository(mock_session)

    repo.truncate_table()

    assert mock_session.execute.called
    args, _ = mock_session.execute.call_args
    assert "TRUNCATE TABLE" in str(args[0])
    mock_session.commit.assert_called_once()


def test_repository_truncate_fallback():
    mock_session = MagicMock()
    mock_session.execute.side_effect = [Exception("Syntax Error"), None]

    repo = RawDataRepository(mock_session)
    repo.truncate_table()

    mock_session.rollback.assert_called_once()
    assert mock_session.execute.call_count == 2
    args, _ = mock_session.execute.call_args
    assert "DELETE FROM" in str(args[0])
    mock_session.commit.assert_called_once()


def test_repository_bulk_insert():
    mock_session = MagicMock()
    repo = RawDataRepository(mock_session)

    data = [{"id": 1, "name": "test"}]
    repo.bulk_insert(data)

    mock_session.bulk_insert_mappings.assert_called_once_with(RawFactoryData, data)
    mock_session.commit.assert_called_once()


def test_repository_bulk_insert_empty():
    mock_session = MagicMock()
    repo = RawDataRepository(mock_session)

    repo.bulk_insert([])

    mock_session.bulk_insert_mappings.assert_not_called()


def test_execute_raw_sql():
    mock_session = MagicMock()
    repo = RawDataRepository(mock_session)
    query = "SELECT 1"

    repo.execute_raw_sql(query)

    assert mock_session.execute.called
    args, _ = mock_session.execute.call_args
    assert str(args[0]) == query
