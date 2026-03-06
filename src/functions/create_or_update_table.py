import pandas as pd

from google.cloud import bigquery

from loguru import logger

def create_or_update_table(df: pd.DataFrame, project_id: str, dataset_name: str, table_name: str) -> None:
    '''
    Escribe los datos dados en una tabla en BigQuery

    Args:
        df: datos tabulares a guardar en BigQuery
        project_id: proyecto en GCP. Éste debe existir de antemano y debemos tener credenciales
        dataset_name: nombre del dataset donde queremos escribir. Éste debe existir de antemano y debemos tener credenciales
        table_name: nombre de la tabla. No tiene por qué existir de antemano. Si no existe, se crea
    '''
    
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    
    logger.info(f"Subiendo {len(df)} filas a la tabla: {table_id}...")
    
    with logger.catch(message="Fallo al escribir la tabla en BigQuery en {table_id}", reraise=True):

        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True, 
        )

        job = client.load_table_from_dataframe(
            df, 
            table_id, 
            job_config=job_config
        )
            
        job.result()

        table = client.get_table(table_id)
        logger.success(f"Escritura completada. La tabla {table_name} ahora tiene {table.num_rows} filas totales.")