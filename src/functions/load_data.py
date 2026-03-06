from google.cloud import bigquery

import pandas as pd

from loguru import logger

def load_data(query: str) -> pd.DataFrame:
    '''
    Lee los datos originales en crudo desde BigQuery

    Args:
        query: query para leer desde el cliente de BigQuery

    Returns:
        pandas.DataFrame
    '''

    logger.info("Iniciando consulta en BigQuery...")
    logger.debug(f"Query enviada: {query}")


    with logger.catch(message="Fallo al cargar datos desde BigQuery", reraise=True):
        client = bigquery.Client()

        df = client.query(query).to_dataframe()

        logger.success(f"Datos cargados correctamente: {df.shape[0]} filas recuperadas.")

        return df