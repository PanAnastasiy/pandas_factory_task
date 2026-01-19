from pathlib import Path
from unittest.mock import MagicMock, mock_open

import pandas as pd
import pytest
from sqlalchemy import text

from core.repositories.raw_repository import RawDataRepository
from core.services.material_service import MaterialETLService


@pytest.fixture
def mock_repo():
    repo = MagicMock(spec=RawDataRepository)
    repo.session = MagicMock()
    return repo


@pytest.fixture
def service(mock_repo):
    return MaterialETLService(mock_repo)


@pytest.fixture
def sample_df():
    # Создаем 3 строки:
    # 0: Все валидно
    # 1: Валидный месяц, но Quantity = NaN (должна остаться, qty -> 0.0)
    # 2: Невалидный месяц (должна удалиться)
    return pd.DataFrame(
        {
            "year": [2024, 2024, 2024],
            "month": ["1", "2", "nan"],
            "plant_id": ["P1", "P1", "P1"],
            "produced_material": ["MAT-1", "MAT-1", "MAT-1"],
            "component_material": ["COMP-1", "COMP-1", "COMP-1"],
            "produced_material_quantity": ["1,000.50", "nan", "500.00"],
            "produced_material_release_type": ["FIN", "FIN", "FIN"],
        }
    )


def test_etl_transformations(service, sample_df, mocker):
    mocker.patch(
        "config.settings.RENAME_MAP",
        {
            "produced_material": "produced_material_id",
            "component_material": "component_material_id",
        },
    )
    mocker.patch(
        "config.settings.ID_COLUMNS",
        ["produced_material_id", "component_material_id", "plant_id"]
    )

    # 1. Проверяем переименование
    df_renamed = service._transform_columns(sample_df)
    assert "produced_material_id" in df_renamed.columns
    assert "produced_material" not in df_renamed.columns

    # 2. Проверяем очистку типов
    df_cleaned = service._clean_data_types(df_renamed)

    # Исходно было 3 строки. Одна с 'nan' в месяце должна удалиться. Останется 2.
    assert len(df_cleaned) == 2

    # --- Проверка строки 0 (Валидная) ---
    val = df_cleaned.iloc[0]["produced_material_quantity"]
    assert isinstance(val, float)
    assert val == 1000.5
    assert df_cleaned.iloc[0]["month"] == 1

    # --- Проверка строки 1 (Валидный месяц, NaN количество) ---
    # Эта строка теперь существует (индекс 1), так как месяц у нее "2"
    val_nan = df_cleaned.iloc[1]["produced_material_quantity"]
    assert val_nan == 0.0
    assert df_cleaned.iloc[1]["month"] == 2


def test_run_import_pipeline_success(service, mocker):
    mock_df = pd.DataFrame(
        {
            "produced_material_id": ["1"],
            "component_material_id": ["2"],
            "year": [2024],
            "month": [1],
            "plant_id": ["P1"],
            "produced_material_quantity": [100.0]
        }
    )

    mocker.patch.object(service, "_read_csv", return_value=mock_df)
    mocker.patch.object(service, "_transform_columns", return_value=mock_df)
    mocker.patch.object(service, "_clean_data_types", return_value=mock_df)

    mocker.patch("config.settings.INPUT_CSV_PATH", Path("dummy.csv"))

    count = service.run_import_pipeline()

    assert count == 1
    service.repository.truncate_table.assert_called_once()
    service.repository.bulk_insert.assert_called_once()
    args, _ = service.repository.bulk_insert.call_args
    assert len(args[0]) == 1
    assert args[0][0]["month"] == 1


def test_run_import_pipeline_missing_column(service, mocker):
    bad_df = pd.DataFrame({
        "produced_material_id": ["1"],
        "year": [2024]
        # month is missing
    })

    mocker.patch.object(service, "_read_csv", return_value=bad_df)
    mocker.patch.object(service, "_transform_columns", return_value=bad_df)
    mocker.patch.object(service, "_clean_data_types", return_value=bad_df)
    mocker.patch("config.settings.INPUT_CSV_PATH", Path("dummy.csv"))

    with pytest.raises(ValueError, match="Validation Error"):
        service.run_import_pipeline()


def test_read_csv_file_not_found(service):
    mock_path = MagicMock()
    mock_path.exists.return_value = False
    mock_path.__str__.return_value = "fake_file.csv"

    with pytest.raises(FileNotFoundError):
        service._read_csv(mock_path)


def test_generate_bom_report(service, mocker):
    mock_path = MagicMock()
    mock_path.exists.return_value = True
    mock_path.name = "mock_script.sql"

    mocker.patch("config.settings.SQL_BOM_SCRIPT_PATH", mock_path)

    mock_sql_content = "DELETE FROM reports; INSERT INTO reports..."
    mocker.patch("builtins.open", mock_open(read_data=mock_sql_content))

    service.generate_bom_report()

    service.repository.session.execute.assert_called_once()
    service.repository.session.commit.assert_called_once()
    service.repository.execute_raw_sql.assert_called_once()

    call_args = service.repository.execute_raw_sql.call_args[0][0]
    assert "SELECT *" in call_args
    assert "FROM bom_reports" in call_args