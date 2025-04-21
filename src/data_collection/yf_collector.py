import logging
from typing import List, Optional

import pandas as pd
import yfinance as yf
import os

logger = logging.getLogger(__name__)


def fetch_market_data(
    tickers: List[str], period: str = "7d", interval: str = "60m"
) -> Optional[pd.DataFrame]:
    """
    Downloads historical market data for a list of tickers using yfinance.
    Args:
        tickers (List[str]): A list of ticker symbols (e.g., '^GSPC', '^GDAXI').
        period (str): Time period for loading data (transfering to yf.download).
        interval (str): Interval between data points (transfering to yf.download).
    Returns:
        pandas DataFrame with OHCLV data, indexed by Datetime,
        with columns grouped by ticker (MultiIndex).
        Returns None if loading fails.
    """
    try:
        cache_path = "/tmp/yfinance_tz_cache"
        if os.path.exists(cache_path):
            if os.path.isfile(cache_path):
                logger.warning(f"Removing existing file at cache path: {cache_path}")
                os.remove(cache_path)
            elif os.path.isdir(cache_path):
                logger.info(f"Cache directory already exists: {cache_path}")
            os.makedirs(cache_path, exist_ok=True)
        else:
            os.makedirs(cache_path, exist_ok=True)

        yf.set_tz_cache_location(cache_path)
        logger.info(f"Set yfinance timezone cache location to: {cache_path}")
    except Exception as e:
        logger.warning(f"Could not set/prepare yfinance cache location: {e}")
        
    if not tickers:
        logger.warning("WARNING! Tickers list for fetch_market_data is empty.")
        return None

    logger.info(
        f"Fetching data for tickers: {tickers}, period: {period}, interval: {interval}"
    )
    try:
        data = yf.download(
            tickers=tickers,
            period=period,
            interval=interval,
            group_by="ticker",
            auto_adjust=False,
            actions=False,
            progress=False,
            ignore_tz=False,
        )

        if data.empty:
            logger.warning(
                f"WARNING! yfinance returned empty data for tickers: {tickers},\
                           period: {period}, interval: {interval}"
            )
            if data.isnull().all().all():
                logger.error("ERROR: All loaded data is NaN!")
                return None
            else:
                logger.warning(
                    "WARNING! DataFrame is not empty, but contains NaN values."
                )
                return data
        if isinstance(data.columns, pd.MultiIndex) and tickers:
            first_ticker = data.columns.levels[0][0]
            expected_cols = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}
            actual_cols = set(data[first_ticker].columns)
            if not expected_cols.issubset(actual_cols):
                logger.warning(
                    f"Not all expected columns {expected_cols}\
                                found for ticker {first_ticker}.\
                                Found: {actual_cols}"
                )
        elif not isinstance(data.columns, pd.MultiIndex) and len(tickers) == 1:
            expected_cols = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}
            actual_cols = set(data.columns)
            if not expected_cols.issubset(actual_cols):
                logger.warning(
                    f"Not all expected columns {expected_cols}\
                                found for ticker {tickers[0]}.\
                                Found: {actual_cols}"
                )
        logger.info(
        f"Data fetched successfully. DataFrame shape: {data.shape}. Timezone: {data.index.tz}"
        )
        return data
    except Exception as e:
        logger.error(f"ERROR: An error occurred while fetching data {e}", exc_info=True)
        return None


if __name__ == "__main__":
    """
    This code runs, only when this file is executed directly:
    python src/data_collection/yf_collector.py
    (Make sure, you are in the root directory of the project or configured PYTHONPATH.)
    """
    print("Launching yf_collector.py for testing...")

    try:
        import sys
        from pathlib import Path

        project_root = Path(__file__).resolve().parent.parent.parent
        if str(project_root) not in sys.path:
            sys.path.append(str(project_root))

        from src.config import settings

        if settings.INDICES:
            test_tickers_list = [index["ticker"] for index in settings.INDICES[:3]]
            print(
                f"Testing with tickers: {test_tickers_list}\
                \nIntraday data (5d, interval = 60m)"
            )
            intraday_data = fetch_market_data(
                tickers=test_tickers_list, period="5d", interval="60m"
            )

            if intraday_data is not None:
                print(
                    f"\nIntraday data loaded successfully. Shape: {intraday_data.shape}\
                    \nFirst 5 rows: {intraday_data.head()}\
                    \nLast 5 rows: {intraday_data.tail()}\
                    \n Index timezone: {intraday_data.index.tz}\
                    \nInformation about DataFrame: {intraday_data.info()}"
                )
                if isinstance(intraday_data.columns, pd.MultiIndex):
                    print(f"Column structure is MultiIndex: {intraday_data.columns}")
                    if test_tickers_list[0] in intraday_data.columns.levels[0]:
                        print(
                            f"Data for {test_tickers_list[0]}: {intraday_data[test_tickers_list[0]].head()}"
                        )
                    else:
                        print(
                            f"Ticker {test_tickers_list[0]} not found in DataFrame columns."
                        )
                else:
                    print("Cannot load intraday data")

            print("\nDaily data (1mo, interval = 1d)")
            daily_data = fetch_market_data(
                tickers=test_tickers_list, period="1mo", interval="1d"
            )

            if daily_data is not None:
                print(
                    f"Daily data loaded successfully. Shape: {daily_data.shape}\
                        \nFirst 5 rows: {daily_data.head()}\
                        \nIndex timezone: {daily_data.index.tz}"
                )
            else:
                print("Cannot load daily data.")

            print("\nIncorrect ticker (INVALID_TICKER_XYZ)")
            invalid_ticker_data = fetch_market_data(
                tickers=["INVALID_TICKER_XYZ", test_tickers_list[0]],
                period="1d",
                interval="60m",
            )
            if invalid_ticker_data is not None:
                print(
                    f"Loading with invalid ticker returned DataFrame (expecting NaN for invalid):\
                       {invalid_ticker_data}"
                )
            else:
                print(
                    "Loading with invalid ticker failed. (or yfinance processed differently)."
                )

        else:
            print("Cannot load indices for testing.")

    except ImportError:
        print(
            f"Cannot import src.config.settings.\
            \nMake sure, you are in the root directory of the project (`python src/data_collection/yf_collector.py`)\
            or folder {project_root} is added to PYTHONPATH."
        )
    except Exception as e:
        print(f"An error occurred while testing: {e}")
