import logging
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras as extras

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def get_db_connection(conn_id: str = "postgres_marketdata") -> Optional[psycopg2.extensions.connection]:
    """
    Establishes a connection to the PostgreSQL database using an Airflow Connection.
    Args:
        conn_id: The Airflow Connection ID for the PostgreSQL database.
    Returns:
        psycopg2 connection object or None if connection fails.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = pg_hook.get_conn()
        conn_params = pg_hook.get_connection(conn_id)
        logger.info(f"DB connected successfully to {conn_params.schema} on {conn_params.host}:{conn_params.port} using Airflow connection '{conn_id}'")
        return conn
    except Exception as e:
        logger.error(f"ERROR: Unable to connect to the database using Airflow connection '{conn_id}': {e}", exc_info=True)
        return None


def create_tables(conn: psycopg2.extensions.connection) -> bool:
    """
    Creates the indices and quotes tables if they do not exist.
    Args:
        conn: Active psycopg2 connection.
    Returns:
        True if tables were created or already exist, False otherwise.
    """
    commands = (
        """
        CREATE TABLE IF NOT EXISTS indices (
            ticker VARCHAR(30) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            country VARCHAR(100),
            exchange VARCHAR(100),
            original_currency VARCHAR(3) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """,
        """
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
           IF row(NEW.*) IS DISTINCT FROM row(OLD.*) THEN
              NEW.updated_at = NOW();
              RETURN NEW;
           ELSE
              RETURN OLD;
           END IF;
        END;
        $$ language 'plpgsql';
        """,
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_indices_modtime') THEN
                CREATE TRIGGER update_indices_modtime
                BEFORE UPDATE ON indices
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();
            END IF;
        END $$;
        """,
        """
        CREATE TABLE IF NOT EXISTS quotes (
            ticker VARCHAR(30) NOT NULL,
            timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            adjusted_close DOUBLE PRECISION,
            volume BIGINT,
            inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, timestamp_utc),
            CONSTRAINT fk_ticker
                FOREIGN KEY(ticker)
                REFERENCES indices(ticker)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        );
        """,
        """
        ALTER TABLE quotes
        ADD COLUMN IF NOT EXISTS open_usd DOUBLE PRECISION,
        ADD COLUMN IF NOT EXISTS high_usd DOUBLE PRECISION,
        ADD COLUMN IF NOT EXISTS low_usd DOUBLE PRECISION,
        ADD COLUMN IF NOT EXISTS close_usd DOUBLE PRECISION,
        ADD COLUMN IF NOT EXISTS adjusted_close_usd DOUBLE PRECISION;
        """,
    )
    try:
        with conn.cursor() as cur:
            for command in commands:
                cur.execute(command)
        conn.commit()
        logger.info("Tables 'indices' and 'quotes' created successfully.")
        return True
    except psycopg2.Error as e:
        logger.error(f"ERROR: While creating tables: {e}", exc_info=True)
        conn.rollback()
        return False


def upsert_indices(
    conn: psycopg2.extensions.connection, indices_df: pd.DataFrame
) -> int:
    """
    Inserts or updates index metadata in the 'indices' table.
    Args:
        conn: Active psycopg2 connection.
        indices_df: DataFrame with columns:
        ['ticker', 'name', 'country', 'exchange', 'original_currency'].
    Returns:
        Number of rows successfully processed (inserted or updated). Returns -1 on error.
    """
    if indices_df.empty:
        logger.info("Indices DataFrame is empty, nothing to upsert.")
        return 0

    required_cols = {"ticker", "name", "original_currency"}
    if not required_cols.issubset(indices_df.columns):
        logger.error(
            f"ERROR: Indices DataFrame missing required cols\
                    {required_cols - set(indices_df.columns)}"
        )
        return -1

    cols_to_insert = ["ticker", "name", "country", "exchange", "original_currency"]
    actual_cols = [col for col in cols_to_insert if col in indices_df.columns]
    insert_cols_str = ", ".join(actual_cols)
    update_cols_str = ", ".join(
        [f"{col} = EXCLUDED.{col}" for col in actual_cols if col != "ticker"]
    )
    values_placeholder = ", ".join(["%s"] * len(actual_cols))

    sql = f"""
    INSERT INTO indices ({insert_cols_str})
    VALUES ({values_placeholder})
    ON CONFLICT (ticker) DO UPDATE
    SET {update_cols_str}
    WHERE indices.ticker = EXCLUDED.ticker;
    """

    data_tuples = [
        tuple(r[col] if pd.notna(r[col]) else None for col in actual_cols)
        for _, r in indices_df[actual_cols].iterrows()
    ]

    try:
        with conn.cursor() as cur:
            extras.execute_batch(cur, sql, data_tuples)
        conn.commit()
        logger.info(
            f"Successfully upserted {len(data_tuples)} rows into 'indices' table."
        )
        return len(data_tuples)
    except psycopg2.Error as e:
        logger.error(f"ERROR: While upserting indices {e}", exc_info=True)
        conn.rollback()
        return -1
    except Exception as e:
        logger.error(
            f"ERROR: Unexpected error occured during indices upsert: {e}", exc_info=True
        )
        conn.rollback()
        return -1


def upsert_quotes(conn: psycopg2.extensions.connection, quotes_df: pd.DataFrame) -> int:
    """
    Inserts or updates quote data in the 'quotes' table using ON CONFLICT.
    Args:
        conn: Active psycopg2 connection.
        quotes_df: Standardized DataFrame with columns including
        ['ticker', 'timestamp_utc', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume'].
    Returns:
        Number of rows successfully processed (inserted or updated). Returns -1 on error.
    """
    if quotes_df.empty:
        logger.info("Quotes DataFrame is empty, nothing to upsert.")
        return 0

    required_cols = {"ticker", "timestamp_utc"}
    if not required_cols.issubset(quotes_df.columns):
        logger.error(
            f"Quotes DataFrame missing required columns: {required_cols - set(quotes_df.columns)}"
        )
        return -1

    db_cols = [
        "ticker",
        "timestamp_utc",
        "open",
        "high",
        "low",
        "close",
        "adjusted_close",
        "volume",
        "open_usd",
        "high_usd",
        "low_usd",
        "close_usd",
        "adjusted_close_usd",
    ]
    cols_to_use = [col for col in db_cols if col in quotes_df.columns]

    if "ticker" not in cols_to_use or "timestamp_utc" not in cols_to_use:
        logger.error(
            "Primary key columns ('ticker', 'timestamp_utc') are missing from the DataFrame."
        )
        return -1

    insert_cols_str = ", ".join(f'"{c}"' for c in cols_to_use)
    update_cols_str = ", ".join(
        [
            f'"{col}" = EXCLUDED."{col}"'
            for col in cols_to_use
            if col not in ["ticker", "timestamp_utc"]
        ]
    )

    sql = f"""
    INSERT INTO quotes ({insert_cols_str})
    VALUES %s
    ON CONFLICT (ticker, timestamp_utc) DO UPDATE
    SET {update_cols_str}
    WHERE quotes.ticker = EXCLUDED.ticker AND quotes.timestamp_utc = EXCLUDED.timestamp_utc;
    """

    df_prepared = quotes_df[cols_to_use].copy()
    df_prepared = df_prepared.replace({pd.NA: None, pd.NaT: None, float("nan"): None})
    if "volume" in df_prepared.columns:
        df_prepared["volume"] = (
            df_prepared["volume"]
            .astype(object)
            .where(df_prepared["volume"].notna(), None)
        )

    data_tuples = list(df_prepared.itertuples(index=False, name=None))

    if not data_tuples:
        logger.info("No data tuples to insert after preparation.")
        return 0

    try:
        with conn.cursor() as cur:
            extras.execute_values(cur, sql, data_tuples, page_size=1000)
        conn.commit()
        logger.info(
            f"Successfully upserted {len(data_tuples)} rows into 'quotes' table."
        )
        return len(data_tuples)
    except psycopg2.errors.ForeignKeyViolation as fk_error:
        logger.error(
            f"ForeignKeyViolation: Ensure all tickers in the quotes data exist in the 'indices' table first. Details: {fk_error}"
        )
        conn.rollback()
        return -1
    except psycopg2.Error as e:
        logger.error(f"Error upserting quotes: {e}", exc_info=True)
        conn.rollback()
        return -1
    except Exception as e:
        logger.error(f"Unexpected error during quotes upsert: {e}", exc_info=True)
        conn.rollback()
        return -1


if __name__ == "__main__":
    print("Starting postgres_writer.py for testing...")

    try:
        import sys
        from pathlib import Path

        project_root = Path(__file__).resolve().parent.parent.parent
        if str(project_root) not in sys.path:
            sys.path.append(str(project_root))

        from src.config import settings
        from src.data_collection.yf_collector import fetch_market_data
        from src.data_processing.standardizer import standardize_data

        print("Step 1: Get DB connection...")
        db_config = {
            "DB_NAME": settings.DB_NAME,
            "DB_USER": settings.DB_USER,
            "DB_PASSWORD": settings.DB_PASSWORD,
            "DB_HOST": settings.DB_HOST,
            "DB_PORT": settings.DB_PORT,
        }
        connection = get_db_connection(db_config)

        if connection:
            print("Step 2: Create tables (if not exist)...")
            if not create_tables(connection):
                print("Failed to create tables. Exiting.")
                sys.exit(1)

            print("Step 3: Fetch and standardize data for testing...")
            if settings.INDICES:
                test_tickers = [index["ticker"] for index in settings.INDICES[:2]]
                raw_data = fetch_market_data(
                    tickers=test_tickers, period="7d", interval="60m"
                )

                if raw_data is not None:
                    standardized_df = standardize_data(raw_data, settings.INDICES)

                    if standardized_df is not None and not standardized_df.empty:
                        print(
                            f"Standardized data shape: {standardized_df.shape}\
                            \nFirst 5 rows of standardized data: {standardized_df.head()}\
                            \nStep 4: Prepare and Upsert Index Metadata..."
                        )

                        meta_cols = [
                            "ticker",
                            "name",
                            "country",
                            "exchange",
                            "original_currency",
                        ]
                        existing_meta_cols = [
                            col for col in meta_cols if col in standardized_df.columns
                        ]
                        if "ticker" in existing_meta_cols:
                            indices_metadata_df = (
                                standardized_df[existing_meta_cols]
                                .drop_duplicates(subset=["ticker"])
                                .reset_index(drop=True)
                            )
                            meta_result = upsert_indices(
                                connection, indices_metadata_df
                            )
                            print(
                                f"Index metadata to upsert: {indices_metadata_df}\
                                \nIndices upsert result: {meta_result} rows processed."
                            )
                        else:
                            print("Cannot extract metadata, 'ticker' column missing.")
                            meta_result = -1

                        if meta_result != -1:
                            quotes_result = upsert_quotes(connection, standardized_df)
                            print(
                                f"\nStep 5: Upsert Quotes Data...\
                                Quotes upsert result: {quotes_result} rows processed."
                            )

                            if quotes_result > 0:
                                print(
                                    "\nStep 6: Verification Query (Last 5 quotes for each test ticker)"
                                )
                                try:
                                    with connection.cursor(
                                        cursor_factory=psycopg2.extras.DictCursor
                                    ) as cur:
                                        query = """
                                        SELECT * FROM quotes
                                        WHERE ticker = ANY(%s)
                                        ORDER BY ticker, timestamp_utc DESC
                                        LIMIT 10;
                                        """
                                        cur.execute(query, (test_tickers,))
                                        results = cur.fetchall()
                                        if results:
                                            print("\nVerification Results:")
                                            for row in results:
                                                print(dict(row))
                                        else:
                                            print(
                                                "No results found in verification query."
                                            )
                                except psycopg2.Error as e:
                                    print(f"Verification query failed: {e}")
                        else:
                            print(
                                "Skipping quotes upsert due to previous error or no data."
                            )

                    else:
                        print("Standardization failed or returned empty DataFrame.")
                else:
                    print("Failed to fetch raw data.")
            else:
                print("No indices configured in settings.")

            connection.close()
            print("\nDatabase connection closed.")
        else:
            print(
                "Failed to connect to the database. Check credentials and server status."
            )

    except ImportError as e:
        print(
            f"Failed to import necessary modules: {e}. Run from project root or check PYTHONPATH."
        )
    except Exception as e:
        print(f"An unexpected error occurred during testing: {e}", exc_info=True)
        if "connection" in locals() and connection and not connection.closed:
            connection.close()
            print("Database connection closed due to error.")
