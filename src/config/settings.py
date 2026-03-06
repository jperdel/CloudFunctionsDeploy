# ID del proyecto. Debe existir de antemano
PROJECT_ID = "poc-484813"

# Nombre del dataset dentro del proyecto. Debe existir de antemano
DATASET_NAME = "datamart_gold_clasificados"

# Nombre de la tabla a crear/modificar. No tiene por qué existir de antemano
TABLE_NAME = "agregado_clasificados"

# Query para obtener los datos de los últimos 90 días
QUERY = """
SELECT *
FROM `bigquery-public-data.ml_datasets.penguins`
LIMIT 10
"""