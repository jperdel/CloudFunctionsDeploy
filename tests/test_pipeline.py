import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.functions.load_data import load_data
from src.functions.create_or_update_table import create_or_update_table

# TEST 1: Verificar que load_data devuelve un DataFrame correctamente
@patch('google.cloud.bigquery.Client')
def test_load_data_success(mock_bq_client):
    # Configuramos el Mock para que devuelva un DF falso
    mock_df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    
    # Simulamos la cadena: Client().query().to_dataframe()
    mock_query_job = MagicMock()
    mock_query_job.to_dataframe.return_value = mock_df
    mock_bq_client.return_value.query.return_value = mock_query_job
    
    result = load_data("SELECT * FROM table")
    
    assert isinstance(result, pd.DataFrame)
    assert result.shape == (2, 2)
    assert not result.empty

# TEST 2: Verificar que el guardado se llama con los parámetros correctos
@patch('google.cloud.bigquery.Client')
def test_create_or_update_table_calls(mock_bq_client):
    df = pd.DataFrame({'a': [1]})
    
    # Simulamos que la carga y la obtención de la tabla funcionan
    mock_client_inst = mock_bq_client.return_value
    mock_job = MagicMock()
    mock_client_inst.load_table_from_dataframe.return_value = mock_job
    
    # Ejecutamos la función
    create_or_update_table(df, "proyect", "dataset", "table")
    
    # Verificamos que se llamó a la función de carga de BQ
    assert mock_client_inst.load_table_from_dataframe.called
    args, kwargs = mock_client_inst.load_table_from_dataframe.call_args
    assert args[0].equals(df) # Comprobamos que el DF pasado es el correcto
    assert args[1] == "proyect.dataset.table"

# TEST 3: Verificar manejo de errores (opcional pero muy recomendado)
@patch('google.cloud.bigquery.Client')
def test_load_data_error(mock_bq_client):
    # Simulamos que BigQuery lanza una excepción
    mock_bq_client.return_value.query.side_effect = Exception("Error de conexión")
    
    with pytest.raises(Exception):
        load_data("SELECT 1")