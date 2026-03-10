# ID del proyecto. Debe existir de antemano
PROJECT_ID = "poc-484813"

# Dataset de origen (Público)
DATASET_SILVER = "ml_datasets"
TABLE_SILVER = "penguins"
SOURCE_TABLE_FQN = f"bigquery:bigquery-public-data.{DATASET_SILVER}.{TABLE_SILVER}"

# Dataset de destino
# Nombre del dataset dentro del proyecto. Debe existir de antemano
DATASET_NAME = "datamart_gold_clasificados"
# Nombre de la tabla a crear/modificar. No tiene por qué existir de antemano
TABLE_NAME = "agregado_clasificados"
TARGET_TABLE_FQN = f"bigquery:{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"

# Región donde van los datos
# OLD: da problemas # LOCATION = "europe-southwest1" 
LOCATION = "eu"

# Query para obtener los datos de los últimos 90 días
QUERY = """
SELECT *
FROM `bigquery-public-data.ml_datasets.penguins`
LIMIT 10
"""

# TODO: días de antigüedad, rangos de ventana, definiciones de segmentos de interés. Ver una vez desarrollado el código de segmentación.