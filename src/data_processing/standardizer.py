import logging
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def standardize_data(
    raw_df: pd.DataFrame, indices_config: List[Dict]
) -> Optional[pd.DataFrame]:
    """
    Standardizes DataFrame received from yf_collector.
    1. Transforms MultiIndex cols in 'long' format.
    2. Renames cols to snake_case (open, high, low, close, adjusted_close, volume).
    3. Adds col 'ticker'.
    4. Converts timestamp to UTC.
    5. Adds index metadata (name, country, original_currency, exchange) from configuration.
    6. Ensures correct data types.
    Args:
        raw_df: DataFrame returned by yf_collector. Must have:
            - DatetimeIndex (potentially with timezone).
            - MultiIndex columns (level 0: ticker, level 1: OHLCV).
        indices_config: List of dictionaries with index configuration (from settings.INDICES).
    Returns:
        Standardized DataFrame with cols:
        ['timestamp_utc', 'ticker', 'name', 'country', 'original_currency',
        'exchange', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']
        None, if input data incorrect or empty.
    """
    if raw_df is None or raw_df.empty:
        logger.warning("WARNING! Input DataFrame to standardize is empty or None.")
        return None
    if not isinstance(raw_df.columns, pd.MultiIndex):
        logger.error("ERROR: Expected Dataframe with MultiIndex columns.")
        return None

    logger.info(f"Data standartization started. Shape: {raw_df.shape}")
    try:
        if not isinstance(raw_df.columns, pd.MultiIndex):
            logger.error(
                "Expected a DataFrame with MultiIndex columns, but got a different format. Check the yf_collector call."
            )
            return None

        logger.debug(f"Original columns: {raw_df.columns}")
        logger.debug(f"Column level names: {raw_df.columns.names}")
        logger.debug(f"Index name: {raw_df.index.name}")

        ticker_level_index = 0
        ticker_level_name_in_cols = raw_df.columns.names[ticker_level_index]
        ticker_col_name_after_reset = (
            ticker_level_name_in_cols
            if ticker_level_name_in_cols
            else f"level_{ticker_level_index}"
        )

        time_index_name = raw_df.index.name
        time_col_name_after_reset = time_index_name if time_index_name else "index"

        logger.info(
            f"Suggested time column name after reset_index: '{time_col_name_after_reset}'"
        )
        logger.info(
            f"Suggested ticker column name after reset_index: '{ticker_col_name_after_reset}'"
        )

        if raw_df.index.name is None:
            raw_df.index.name = "index"

        df_long = raw_df.stack(
            level=ticker_level_index, future_stack=True
        ).reset_index()

        logger.info(f"Cols AFTER stack().reset_index(): {df_long.columns.tolist()}")

        rename_map = {
            time_col_name_after_reset: "timestamp",
            ticker_col_name_after_reset: "ticker",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adjusted_close",
            "Volume": "volume",
        }

        actual_columns = df_long.columns.tolist()
        rename_map_filtered = {}
        missing_keys_in_df = []
        for key, value in rename_map.items():
            if key in actual_columns:
                rename_map_filtered[key] = value
            else:
                if key in [time_col_name_after_reset, ticker_col_name_after_reset]:
                    missing_keys_in_df.append(key)

        if missing_keys_in_df:
            logger.error(
                f"Critical columns '{missing_keys_in_df}' not found in DataFrame after stack/reset: {actual_columns}. Unable to continue."
            )
            return None

        df_long = df_long.rename(columns=rename_map_filtered)
        logger.info(f"Cols after rename: {df_long.columns.tolist()}")

        if "timestamp" not in df_long.columns:
            logger.error("'timestamp' col is missing after rename.")
            return None
        if "ticker" not in df_long.columns:
            logger.error("'ticker' col is missing after rename.")
            return None
    except Exception as e:
        logger.error(
            f"Error converting MultiIndex or renaming columns: {e}", exc_info=True
        )
        return None
    logger.info(f"Data converted to long format. Shape: {df_long.shape}")

    if "timestamp" not in df_long.columns:
        logger.error("'timestamp' column not found after reset_index()")
        return None

    try:
        df_long["timestamp"] = pd.to_datetime(df_long["timestamp"])

        if df_long["timestamp"].dt.tz is not None:
            logger.info(
                f"Timezone found {df_long['timestamp'].dt.tz}. Converting to UTC."
            )
            df_long["timestamp_utc"] = df_long["timestamp"].dt.tz_convert("UTC")
        else:
            logger.warning(
                "WARNING! Source timezone not found (possibly daily data). Localization in UTC."
            )
            df_long["timestamp_utc"] = df_long["timestamp"].dt.tz_localize("UTC")

        df_long = df_long.drop(columns=["timestamp"])
        logger.info("Timestamp succesfully converted to UTC.")
    except Exception as e:
        logger.error(f"ERROR: converting timestamp to UTC: {e}", exc_info=True)
        return None

    if not indices_config:
        logger.warning("Indices configuration is empty. No metadata will be added.")
        df_long["name"] = None
        df_long["country"] = None
        df_long["original_currency"] = None
        df_long["exchange"] = None
    else:
        try:
            meta_df = pd.DataFrame(indices_config)
            required_meta_cols = ["ticker", "name", "country", "currency", "exchange"]
            if not all(col in meta_df.columns for col in required_meta_cols):
                logger.error(
                    f"ERROR: Not all required cols {required_meta_cols}\
                            found in indices_config"
                )
                meta_df = meta_df[
                    [col for col in required_meta_cols if col in meta_df.columns]
                ]
            if "ticker" in meta_df.columns:
                meta_df = meta_df.rename(columns={"currency": "original_currency"})
                df_merged = pd.merge(
                    df_long,
                    meta_df[
                        ["ticker", "name", "country", "original_currency", "exchange"]
                    ],
                    on="ticker",
                    how="left",
                )
                if df_merged.shape[0] != df_long.shape[0]:
                    logger.warning(
                        "WARNING! Number of rows changed after metadata merge.\
                                    Check tickers in config."
                    )
                df_long = df_merged
                logger.info("Metadata succesfully added.")
            else:
                logger.error(
                    "ERROR: 'ticker' col is missing in indices_config. Cannot add metadata."
                )
                return None
        except Exception as e:
            logger.error(f"ERROR: Adding metadata: {e}", exc_info=True)
            return None

    try:
        dtype_map = {
            "open": "float64",
            "high": "float64",
            "low": "float64",
            "close": "float64",
            "adjusted_close": "float64",
            "volume": "float64",
            "ticker": "str",
            "name": "str",
            "country": "str",
            "original_currency": "str",
            "exchange": "str",
        }
        for col, dtype in dtype_map.items():
            if col in df_long.columns:
                if dtype == "str":
                    df_long[col] = df_long[col].astype(str).fillna("")
                else:
                    df_long[col] = pd.to_numeric(df_long[col], errors="coerce")
                    if not pd.api.types.is_float_dtype(df_long[col]):
                        df_long[col] = df_long[col].astype(dtype)

        if "volume" in df_long.columns and df_long["volume"].isnull().sum() == 0:
            try:
                df_long["volume"] = df_long["volume"].astype("Int64")
                logger.info("'volume' col successfully converted to Int64.")
            except Exception as e_int:
                logger.warning(
                    f"WARNING! Cannot convert 'volume' col to Int64: {e_int}.\
                                 float64 will be used instead."
                )
        elif "volume" in df_long.columns:
            logger.info("'volume' col contains NaN, float64 will be used instead.")
        final_columns = [
            "timestamp_utc",
            "ticker",
            "name",
            "country",
            "original_currency",
            "exchange",
            "open",
            "high",
            "low",
            "close",
            "adjusted_close",
            "volume",
        ]
        existing_final_columns = [
            col for col in final_columns if col in df_long.columns
        ]
        if len(existing_final_columns) != len(final_columns):
            missing_cols = set(final_columns) - set(existing_final_columns)
            logger.warning(
                f"WARNING! Some final cols are missing: {missing_cols}.\
                            DataFrame may be incomplete."
            )
        df_standardized = df_long[existing_final_columns]
    except Exception as e:
        logger.error(
            f"ERROR: while setting data types or selecting columns {e}", exc_info=True
        )
        return None

    logger.info(f"Standartizing completed. Final shape: {df_standardized.shape}")
    nan_counts = (
        df_standardized[["open", "high", "low", "close", "adjusted_close", "volume"]]
        .isnull()
        .sum()
    )
    logger.info(f"NaN counts in selected columns:\n{nan_counts[nan_counts > 0]}")

    return df_standardized


if __name__ == "__main__":
    print("Starting standardizer.py for testing...")

    try:
        import sys
        from pathlib import Path

        project_root = Path(__file__).resolve().parent.parent.parent
        if str(project_root) not in sys.path:
            sys.path.append(str(project_root))

        from src.config import settings
        from src.data_collection.yf_collector import fetch_market_data

        if settings.INDICES:
            test_tickers = [index["ticker"] for index in settings.INDICES[:3]]
            print(
                f"\nTesting with tickers: {test_tickers}\
                \nStep 1: raw intraday data fetching..."
            )
            raw_data = fetch_market_data(
                tickers=test_tickers, period="5d", interval="60m"
            )

            if raw_data is not None:
                print(
                    f"Raw data fetched successfully. Shape: {raw_data.shape}\
                        \nRaw data example (head): {raw_data.head()}\
                        \nStep 2: Standardizing data..."
                )
                standardized_df = standardize_data(raw_data, settings.INDICES)

                if standardized_df is not None:
                    print(
                        f"\nStandardization successful!\
                        \nFinal Shape: {standardized_df.shape}\
                        \nFirst 5 rows of standardized DataFrame: {standardized_df.head()}\
                        \nLast 5 rows of standardized DataFrame: {standardized_df.tail()}\
                        \nInformation about standardized DataFrame: {standardized_df.info()}\
                        \nChecking for unique values in selected columns:"
                    )
                    for col in [
                        "ticker",
                        "name",
                        "country",
                        "original_currency",
                        "exchange",
                    ]:
                        if col in standardized_df.columns:
                            print(f"  {col}: {standardized_df[col].unique()}")
                        else:
                            print(
                                f"  {col}: Column not found in standardized DataFrame."
                            )

                    if "timestamp_utc" in standardized_df.columns:
                        print(
                            f"\nColumn Timezone 'timestamp_utc': {standardized_df['timestamp_utc'].dt.tz}"
                        )
                    else:
                        print(
                            "\n'timestamp_utc' col is missing in standardized DataFrame."
                        )
                else:
                    print("\nStandardization failed.")
            else:
                print("\nCannot fetch raw data for testing.")

            print(
                "\n\nTest with daily data...\
                \nStep 1: raw daily data fetching..."
            )
            raw_daily_data = fetch_market_data(
                tickers=[test_tickers[0]], period="1mo", interval="1d"
            )
            if raw_daily_data is not None:
                print(
                    f"Raw daily data fetched successfully. Shape: {raw_daily_data.shape}\
                    \nRaw data index timezone: {raw_daily_data.index.tz}\
                    \nStep 2: Standardizing daily data..."
                )
                standardized_daily_df = standardize_data(
                    raw_daily_data, settings.INDICES
                )

                if standardized_daily_df is not None:
                    print(
                        f"\nStandardization successful!\
                    \nFinal Shape: {standardized_daily_df.shape}\
                    \nFirst 5 rows of standardized DataFrame: {standardized_daily_df.head()}\
                    \nInformation about standardized DataFrame: {standardized_daily_df.info()}\
                    \nColumn Timezone 'timestamp_utc': {standardized_daily_df['timestamp_utc'].dt.tz}"
                    )
                else:
                    print("\nStandardization failed.")
            else:
                print("\nCannot fetch raw daily data for testing.")
        else:
            print("Cannot load indices from settings for testing.")
    except ImportError as e:
        print(
            f"Cannot import modules: {e}\
            \nMake sure you run script from the root folder of the project (`python src/data_processing/standardizer.py`)\
            \nOr that the {project_root} folder is added to PYTHONPATH."
        )
    except Exception as e:
        print(f"An error occurred while testing: {e}", exc_info=True)
