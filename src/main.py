import logging
import sys
import time
from pathlib import Path

import psycopg2


def main():
    """
    The main function to start the data pipeline:
    1. Load settings.
    2. Get data from yfinance.
    3. Standardize data.
    4. Convert currencies.
    5. Connect to the DB.
    6. Create tables (if necessary).
    7. Load index metadata into the DB.
    8. Load quotes into the DB.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)

    start_time = time.time()
    logger.info("Starting the market data pipeline...")

    try:
        project_root = Path(__file__).resolve().parent.parent
        if str(project_root) not in sys.path:
            sys.path.append(str(project_root))
            logger.debug(f'Added {project_root} to sys.path')
        
        from src.config import settings
        from src.data_collection import yf_collector
        from src.data_processing import currency_converter, standardizer
        from src.storage import postgres_writer
    except ImportError as e:
        logger.exception(f"ERROR: Failed to import necessary modules: {e}.\
                        Ensure the script is run correctly or PYTHONPATH is set.")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"ERROR: An unexpected error occurred during initial setup: {e}")
        sys.exit(1)

    logger.info('Config loaded!')
    if not settings.INDICES:
        logger.warning('WARNING! No indices found in config. Exiting...')
        sys.exit(0)
    
    tickers = [index['ticker'] for index in settings.INDICES]
    if not tickers:
        logger.warning("WARNING! Ticker list is empty. Exiting.")
        sys.exit(0)
    
    fetch_period = settings.FETCH_PERIOD
    fetch_interval = settings.FETCH_INTERVAL

    logger.info(f'Target tickers {tickers}.\
                \nFetch period {fetch_period}. Fetch interval {fetch_interval}.\
                \nFetching raw data from yfinance...')
    
    raw_data_df = yf_collector.fetch_market_data(
        tickers=tickers,
        period=fetch_period,
        interval=fetch_interval
    )
    if raw_data_df is None or raw_data_df.empty:
        logger.warning('WARNING! No data received from pipeline. Pipiline finished.')
        sys.exit(0)
    logger.info(f'Raw data fetched! Shape: {raw_data_df.shape}\
                \nStandardizing data...')
    
    standardized_df = standardizer.standardize_data(raw_data_df, settings.INDICES)
    if standardized_df is None or standardized_df.empty:
        logger.error('Data standartization failed or resulted in empty DataFrame. Exiting.')
        sys.exit(1)
    logger.info(f'Data standardized! Shape: {standardized_df.shape}\
                \nConverting prices to target currency...')
    
    final_df = currency_converter.convert_to_target_currency(
    standardized_df,
    target_currency=settings.TARGET_CURRENCY
    )
    if final_df is None or final_df.empty:
        logger.error("Currency conversion failed or resulted in empty DataFrame. Exiting.")
        sys.exit(1)
    logger.info(f"Currency conversion completed. Final DataFrame shape: {final_df.shape}")
    logger.debug(f"Final DataFrame columns: {final_df.columns.tolist()}\
                \nFinal DataFrame head:\n{final_df.head()}")

    connection_attempt = None
    try:
        connection_attempt = postgres_writer.get_db_connection(conn_id="postgres_marketdata")

        if connection_attempt is None:
            logger.error("Failed to establish database connection using Airflow connection 'postgres_marketdata'. Exiting.")
            sys.exit(1)

        with connection_attempt as connection:
            logger.info("Database connection established. Proceeding with operations...")

            logger.info("Ensuring database tables exist...")
            if not postgres_writer.create_tables(connection):
                logger.error("Failed to create or verify database tables. Raising error to trigger rollback.")
                raise RuntimeError("Table creation failed")

            logger.info("Upserting index metadata...")
            meta_cols = ['ticker', 'name', 'country', 'exchange', 'original_currency']
            existing_meta_cols = [col for col in meta_cols if col in final_df.columns]
            if 'ticker' in existing_meta_cols:
                indices_metadata_df = final_df[existing_meta_cols].drop_duplicates(subset=['ticker']).reset_index(drop=True)
                meta_result = postgres_writer.upsert_indices(connection, indices_metadata_df)
                if meta_result == -1:
                     logger.error("Failed to upsert index metadata. Raising error to trigger rollback.")
                     raise RuntimeError("Index metadata upsert failed")
                else:
                     logger.info(f"Index metadata upsert processed {meta_result} rows.")
            else:
                 logger.error("Cannot extract metadata for upsert, 'ticker' column missing. Raising error.")
                 raise RuntimeError("Metadata extraction failed")

            logger.info("Upserting quotes data...")
            quotes_result = postgres_writer.upsert_quotes(connection, final_df)
            if quotes_result == -1:
                logger.error("Failed to upsert quotes data. Raising error to trigger rollback.")
                raise RuntimeError("Quotes data upsert failed")
            else:
                logger.info(f"Quotes data upsert processed {quotes_result} rows.")

    except (psycopg2.Error, RuntimeError, Exception) as e:
        logger.exception(f"An error occurred during database operations: {e}")
        sys.exit(1)

    end_time = time.time()
    logger.info(f"Pipeline finished successfully in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()