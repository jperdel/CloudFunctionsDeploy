# from src.config.settings import *
# from src.functions.load_data import load_data
# from src.functions.create_or_update_table import create_or_update_table

# from loguru import logger

# from datetime import datetime

# from pathlib import Path

# import sys
# import os

# # --- CONFIGURACIÓN DE LOGS ---
# logger.remove()

# # 1. Siempre a Consola (Para ver logs en GCP y en tu terminal local)
# logger.add(sys.stdout, 
#            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>", 
#            level="DEBUG")

# # 2. Configuración con Pathlib (Solo para local)
# LOG_DIR = Path("logs")
# IS_CLOUD = os.getenv('K_SERVICE') is not None

# if not IS_CLOUD:
#     # Creamos la carpeta si no existe de forma segura
#     LOG_DIR.mkdir(exist_ok=True)
    
#     logger.add(LOG_DIR / "pipeline_{time:YYYY-MM-DD}.log", 
#                rotation="00:00", 
#                retention="7 days", 
#                level="DEBUG")

# def data_aggregation_pipeline(request):

#     logger.info("=" * 80) # Línea divisoria
#     logger.info(f"INICIANDO EJECUCIÓN: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

#     try:
#         # READ BIGQUERY TABLE
#         df = load_data(QUERY)

#         # TODO: BUSINESS LOGIC HERE

#         # CREATE OR UPDATE TABLE
#         create_or_update_table(df, PROJECT_ID, DATASET_NAME, TABLE_NAME)

#         logger.success("La ETL se completó con éxito.")

#     except Exception as e:
#         logger.exception("La ETL no se pudo completar. Abortando ejecución.")
#         exit(1)

#     finally:
#         logger.info("=" * 80)

#     return "200"

# if __name__ == "__main__":

#     logger.info("Ejecutando script en modo local (directo)")
    
#     resultado = data_aggregation_pipeline(None)



import sys
import os
from datetime import datetime
from pathlib import Path
from loguru import logger

# Tus imports existentes
from src.config.settings import *
from src.functions.load_data import load_data
from src.functions.create_or_update_table import create_or_update_table

from src.governance.lineage_logger import LineageLogger

logger.remove()
logger.add(sys.stdout, 
           format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>", 
           level="DEBUG")

LOG_DIR = Path("logs")
IS_CLOUD = os.getenv('K_SERVICE') is not None

if not IS_CLOUD:
    LOG_DIR.mkdir(exist_ok=True)
    logger.add(LOG_DIR / "pipeline_{time:YYYY-MM-DD}.log", 
               rotation="00:00", 
               retention="7 days", 
               level="DEBUG")

lineage = LineageLogger(PROJECT_ID, LOCATION, "pipeline-definitivo-v4")

def data_aggregation_pipeline(request):
    logger.info("=" * 80)
    logger.info(f"INICIANDO EJECUCIÓN: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Iniciar Run en Dataplex
    lineage.start_run()
    success = False

    try:
        source_table = f"bigquery:{PROJECT_ID}.{DATASET_SILVER}.{TABLE_SILVER}"
        target_table = f"bigquery:{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"

        # READ BIGQUERY TABLE
        df = load_data(QUERY)

        # TODO: BUSINESS LOGIC HERE

        # CREATE OR UPDATE TABLE
        create_or_update_table(df, PROJECT_ID, DATASET_NAME, TABLE_NAME)

        # REGISTER LINEAGE
        lineage.log_transformation(source_table, target_table)
        
        logger.success("La ETL se completó con éxito.")
        success = True

    except Exception as e:
        logger.exception("La ETL no se pudo completar. Abortando ejecución.")
        success = False
        return f"Error: {str(e)}", 500

    finally:
        lineage.end_run(success=success)
        logger.info("=" * 80)

    return "200"

if __name__ == "__main__":
    logger.info("Ejecutando script en modo local (directo)")
    data_aggregation_pipeline(None)