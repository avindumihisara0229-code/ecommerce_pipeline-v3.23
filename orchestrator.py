import subprocess
import sys
import logging
from datetime import datetime

# LOGGING MECHANISM: Setup a formal log file
logging.basicConfig(
    filename='ecommerce_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_pipeline():
    logging.info("==================================================")
    logging.info("STARTING MASTER E-COMMERCE DATA PIPELINE")
    
    print("Pipeline started. Check ecommerce_pipeline.log for details...")

    # 1. Run Ingestion Flow
    logging.info("TASK 1: Triggering Data Ingestion...")
    ingest_process = subprocess.run([sys.executable, "ingestion.py"])
    
    if ingest_process.returncode != 0:
        logging.error("MONITORING ALERT: Pipeline Failed during Ingestion. Halting execution.")
        return

    # 2. Run ETL Flow
    logging.info("TASK 2: Triggering PySpark ETL...")
    etl_process = subprocess.run([sys.executable, "etl_pyspark.py"])
    
    if etl_process.returncode != 0:
        logging.error("MONITORING ALERT: Pipeline Failed during ETL processing. Halting execution.")
        return

    logging.info("PIPELINE COMPLETION SUCCESSFUL!")
    logging.info("==================================================")

if __name__ == "__main__":
    run_pipeline()