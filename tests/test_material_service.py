from pathlib import Path
from unittest.mock import MagicMock, mock_open

import pandas as pd
import pytest

from core.repositories.raw_repository import RawDataRepository
from core.services.material_service import MaterialETLService


@pytest.fixture
def mock_repo():
    return MagicMock(spec=RawDataRepository)


@pytest.fixture
def service(mock_repo):
    return MaterialETLService(mock_repo)


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "year": [2024, 2024],
            "plant_id": ["P1", "P1"],
            "produced_material": ["MAT-1", "MAT-1"],
            "component_material": ["COMP-1", "COMP-1"],
            "produced_material_quantity": ["1,000.50", "nan"],
            "produced_material_release_type": ["FIN", "FIN"],
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
        "config.settings.ID_COLUMNS", ["produced_material_id", "component_material_id"]
    )

    df_renamed = service._transform_columns(sample_df)
    assert "produced_material_id" in df_renamed.columns
    assert "produced_material" not in df_renamed.columns

    df_cleaned = service._clean_data_types(df_renamed)

    val = df_cleaned.iloc[0]["produced_material_quantity"]
    assert isinstance(val, float)
    assert val == 1000.5

    assert df_cleaned.iloc[1]["produced_material_quantity"] is None

    df_final = service._deduplicate_by_year(df_cleaned)
    assert len(df_final) == 1


def test_run_import_pipeline_success(service, mocker):
    mock_df = pd.DataFrame(
        {
            "produced_material_id": ["1"],
            "component_material_id": ["2"],
            "year": [2024],
            "plant_id": ["P1"],
        }
    )

    mocker.patch.object(service, "_read_csv", return_value=mock_df)
    mocker.patch.object(service, "_transform_columns", return_value=mock_df)
    mocker.patch.object(service, "_clean_data_types", return_value=mock_df)
    mocker.patch.object(service, "_deduplicate_by_year", return_value=mock_df)

    mocker.patch("config.settings.INPUT_CSV_PATH", Path("dummy.csv"))

    count = service.run_import_pipeline()

    assert count == 1
    service.repository.truncate_table.assert_called_once()
    service.repository.bulk_insert.assert_called_once()


def test_run_import_pipeline_missing_column(service, mocker):
    bad_df = pd.DataFrame({"other_col": [1]})

    mocker.patch.object(service, "_read_csv", return_value=bad_df)
    mocker.patch.object(service, "_transform_columns", return_value=bad_df)
    mocker.patch.object(service, "_clean_data_types", return_value=bad_df)
    mocker.patch.object(service, "_deduplicate_by_year", return_value=bad_df)
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

    mock_sql_content = "SELECT * FROM test"
    mocker.patch("builtins.open", mock_open(read_data=mock_sql_content))

    service.generate_bom_report()

    service.repository.execute_raw_sql.assert_called_once_with(mock_sql_content)
