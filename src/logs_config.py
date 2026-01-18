import logging, sys

## Esto supone que el ETL se va a correr on-prem y se requieren almacenar los logs de ejecuciones automaticcas en un archivo .log
# En una nube no hay necesidad de hacer esto. Los prints se almacenan en el servicio de logs, como CloudWatch

def setup_logging(store_logs=False, log_file="logs.log"):
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    class StdoutToLogger:
        def write(self, msg):
            if msg.strip():
                logging.getLogger().info(msg.rstrip())
        def flush(self):
            pass
    
    if store_logs:
        sys.stdout = StdoutToLogger()
        sys.stderr = StdoutToLogger()

