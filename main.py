"""
Main ETL Pipeline Orchestrator
------------------------------
This script serves as the primary entry point for the Medallion Architecture 
Data Pipeline. It sequentially executes the raw layer 
while providing detailed performance metrics and logging.

Workflow:
raw: Raw Data Ingestion (CSV to SQL)
"""

import time
from python.config import logging
from python.ingestion_script import csv_files, fetching_and_loading_process

# Initialize logger for the global pipeline execution
logger = logging.getLogger(__name__)

def run_pipeline():
    """
    Executes the full ELT pipeline and monitors performance.
    """
    logger.info('--- Pipeline Execution Started ---')
    print('Starting Data Pipeline Execution...')

    try:
        total_start_time = time.time()

        # --- Bronze Layer (Ingestion) ---
        print('Starting raw Layer Ingestion...')
        fetching_and_loading_process(csv_files)

        # --- Summary Section ---
        total_end_time = time.time()
        total_duration = round(total_end_time - total_start_time, 2)
        logger.info(f'--- 🏁 Pipeline Finished Successfully (Total Time: {total_duration}s) ---')
        success_count = len(csv_files)
        logger.info(f'📦 raw Layer Complete: {success_count} assets ingested.')

        print("-" * 50)
        print(f'✅ Full ELT Pipeline completed in {total_duration} seconds.')
        print(f'📦 raw Layer Complete: {success_count} assets ingested.')
        print("-" * 50)

    except Exception as e:
        logger.critical(f'🛑 Pipeline Crashed: {e}', exc_info=True)
        print(f"FATAL ERROR: Pipeline failed. Check 'app.log' for details.")

if __name__ == '__main__':
    run_pipeline()