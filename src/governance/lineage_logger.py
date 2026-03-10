import time # <--- IMPORTANTE
from google.cloud import datacatalog_lineage_v1
from datetime import datetime
from loguru import logger

class LineageLogger:
    def __init__(self, project_id: str, location: str, process_id: str):
        self.project_id = project_id
        self.location = "us" # Mantenemos 'us' por estabilidad SSL
        self.process_id = f"proc-{process_id}".lower().replace("_", "-").replace(" ", "-")[:40]
        
        # Cliente global
        self.client = datacatalog_lineage_v1.LineageClient()
        
        # Este será el path que usaremos, pero lo actualizaremos si Google nos da uno distinto
        self.process_name = f"projects/{project_id}/locations/{self.location}/processes/{self.process_id}"
        self.run_name = None

    def _ensure_process_exists(self):
        try:
            response = self.client.get_process(name=self.process_name)
            self.process_name = response.name # Aseguramos el nombre oficial
        except Exception:
            logger.info(f"Creando proceso maestro de linaje...")
            parent = f"projects/{self.project_id}/locations/{self.location}"
            process = datacatalog_lineage_v1.Process(display_name=self.process_id)
            try:
                # Al crear, capturamos la respuesta oficial de Google
                response = self.client.create_process(parent=parent, process=process)
                self.process_name = response.name
                logger.success(f"Proceso creado. Esperando propagación...")
                
                # --- EL TRUCO DEL ALMENDRUCO ---
                # Pausamos 2 segundos SOLO la primera vez para evitar el Error 400
                time.sleep(2) 
            except Exception as e:
                logger.error(f"Error al crear proceso: {e}")
                raise e

    def start_run(self):
        try:
            self._ensure_process_exists()
            
            run = datacatalog_lineage_v1.Run()
            run.state = datacatalog_lineage_v1.Run.State.STARTED
            run.start_time = datetime.utcnow()
            
            # Usamos el process_name confirmado por la respuesta de Google
            response = self.client.create_run(parent=self.process_name, run=run)
            self.run_name = response.name
            logger.debug(f"Run de Dataplex iniciado con éxito.")
            return self.run_name
        except Exception as e:
            # Si vuelve a dar 400 aquí, imprimimos el process_name para depurar
            logger.warning(f"Error 400 en start_run. Path usado: {self.process_name}")
            logger.warning(f"Detalle: {e}")
            return None

    def log_transformation(self, source_fqn: str, target_fqn: str):
        if not self.run_name: return
        try:
            event = datacatalog_lineage_v1.LineageEvent()
            event.links = [
                datacatalog_lineage_v1.EventLink(
                    source={"fully_qualified_name": source_fqn},
                    target={"fully_qualified_name": target_fqn}
                )
            ]
            event.start_time = datetime.utcnow()
            self.client.create_lineage_event(parent=self.run_name, lineage_event=event)
            logger.info(f"Vínculo de linaje creado: {source_fqn} -> {target_fqn}")
        except Exception as e:
            logger.error(f"No se pudo registrar el evento de linaje: {e}")

    def end_run(self, success: bool = True):
        if not self.run_name:
            logger.warning("No hay un Run activo para cerrar.")
            return

        try:
            # 1. Recuperamos el Run actual de la API
            # Esto trae el start_time original que Google ya conoce
            run = self.client.get_run(name=self.run_name)
            
            # 2. Actualizamos solo los campos de cierre
            run.state = (datacatalog_lineage_v1.Run.State.COMPLETED if success 
                         else datacatalog_lineage_v1.Run.State.FAILED)
            run.end_time = datetime.utcnow()
            
            # 3. Enviamos la actualización
            # Incluimos 'state' y 'end_time' en la máscara
            self.client.update_run(
                run=run, 
                update_mask={"paths": ["state", "end_time"]}
            )
            
            status_text = "SUCCESS" if success else "FAILED"
            logger.info(f"Run de linaje cerrado con éxito | Estado: {status_text}")
            
        except Exception as e:
            # Si falla el GET o el UPDATE, lo logueamos pero no rompemos la ETL
            logger.error(f"Error al cerrar el Run de Dataplex: {e}")