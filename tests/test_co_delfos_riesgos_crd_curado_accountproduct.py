import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from extra.config.hudiconfig import (
    get_hudi_options,
)
from tables_energy.co_energy_market_curado_providers.co_energy_market_curado_providers import EnergyProviders


@pytest.fixture
def mock_spark_session():
    """Mock de la sesión Spark para pruebas unitarias."""
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def mock_args():
    """Mock de los argumentos del job Glue."""
    return {
        "JOB_NAME": "co_energy_market_curado_providers",
        "ACCOUNT": "test_account",
        "ENV": "qa",
        "PROCESS_DATE": "20231204",
        "TYPE_PROCESS": "INC",
    }


@pytest.fixture
def energy_providers(mock_args, mock_spark_session):
    """Fixture para crear una instancia de EnergyProviders."""
    with patch.object(EnergyProviders, '__init__', lambda x, y: None):  # Mock constructor
        energy_providers = EnergyProviders(mock_args)
        energy_providers._spark = mock_spark_session
        energy_providers.glue_context = None
        energy_providers.path_raw_data = "s3://test-bucket/raw-data"
        energy_providers.path_curated_data = "s3://test-bucket/curated-data"
        energy_providers.catalog_database = "test_catalog"
        return energy_providers

@patch('tables_energy.co_energy_market_curado_providers.co_energy_market_curado_providers.EnergyProviders.read_raw_data')
@patch('tables_energy.co_energy_market_curado_providers.co_energy_market_curado_providers.EnergyProviders.write_to_hudi')
@patch('tables_energy.co_energy_market_curado_providers.co_energy_market_curado_providers.EnergyProviders.process_data')
@patch('tables_energy.co_energy_market_curado_providers.co_energy_market_curado_providers.Job')
def test_run(mock_job, mock_process_data, mock_write_to_hudi, mock_read_raw_data, energy_providers):
    """Test del flujo completo de ejecución."""

    # Simulamos el job de Glue, lo inicializamos correctamente en el test
    mock_job_instance = MagicMock()
    mock_job.return_value = mock_job_instance  # Se asegura de que Job sea mockeado correctamente

    energy_providers.job = mock_job_instance

    # Simulamos que el DataFrame leído no está vacío
    mock_df = MagicMock()
    mock_read_raw_data.return_value = mock_df
    mock_process_data.return_value = mock_df
    mock_write_to_hudi.return_value = None  # No devuelve nada, sólo ejecuta el proceso

    # Ejecutamos el método run
    energy_providers.run()

    # Verificamos que los métodos fueron llamados correctamente
    mock_read_raw_data.assert_called_once()
    mock_process_data.assert_called_once_with(mock_df)
    mock_write_to_hudi.assert_called_once_with(mock_df)
    mock_job_instance.commit.assert_called_once()  # Verificamos que el método commit fue llamado

@patch('extra.config.hudiconfig.get_hudi_options')
def test_get_hudi_options(mock_get_hudi_options):
    """Test de la función get_hudi_options."""
    mock_get_hudi_options.return_value = {
        "hoodie.table.name": "curated_table",
        "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "bulk_insert"
    }

    options = get_hudi_options("test_database", "curated_table")

    assert options["hoodie.table.name"] == "curated_table"
    assert options["hoodie.datasource.write.storage.type"] == "COPY_ON_WRITE"
    assert options["hoodie.datasource.write.operation"] == "bulk_insert"
