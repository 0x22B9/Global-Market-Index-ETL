import json
import logging
import socket
from datetime import date
from typing import Dict, Optional

import pandas as pd
import requests
from urllib3.util import connection

logger = logging.getLogger(__name__)

RATE_CACHE: Dict[tuple, float] = {}
CACHE_MISSES = set()

_original_allowed_gai_family = connection.allowed_gai_family
connection.allowed_gai_family = lambda: socket.AF_INET

def get_exchange_rate(
    base_currency: str, target_currency: str, rate_date: date
) -> Optional[float]:
    """
    Fetches the exchange rate for a specific date using the frankfurter.app API.
    Uses a simple in-memory cache to avoid redundant lookups.
    Args:
        base_currency: The currency to convert from (e.g., 'EUR').
        target_currency: The currency to convert to (e.g., 'USD').
        rate_date: The date for which to fetch the rate (datetime.date object).
    Returns:
        The exchange rate (float) or None if not available or on error.
    """
    if base_currency == target_currency:
        return 1.0

    cache_key = (base_currency, target_currency, rate_date)
    if cache_key in RATE_CACHE:
        return RATE_CACHE[cache_key]
    if cache_key in CACHE_MISSES:
        return None

    date_str = rate_date.strftime("%Y-%m-%d")
    url = f"https://api.frankfurter.app/{date_str}?from={base_currency}&to={target_currency}"

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()

        data = response.json()

        if "rates" in data and target_currency in data["rates"]:
            rate = data["rates"][target_currency]
            if rate is None:
                logger.warning(
                    f"API returned null rate for {base_currency} -> {target_currency} on {date_str}"
                )
                CACHE_MISSES.add(cache_key)
                return None
            rate = float(rate)
            RATE_CACHE[cache_key] = rate
            logger.debug(
                f"Fetched rate via API for {base_currency}->{target_currency} on {date_str}: {rate}"
            )
            return rate
        else:
            logger.warning(
                f"Rate for {target_currency} not found in API response for {base_currency} on {date_str}. Response: {data}"
            )
            CACHE_MISSES.add(cache_key)
            return None

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err} for URL: {url}")
        CACHE_MISSES.add(cache_key)
        return None
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error occurred: {conn_err} for URL: {url}")
        CACHE_MISSES.add(cache_key)
        return None
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout error occurred: {timeout_err} for URL: {url}")
        CACHE_MISSES.add(cache_key)
        return None
    except requests.exceptions.RequestException as req_err:
        logger.error(f"An ambiguous request error occurred: {req_err} for URL: {url}")
        CACHE_MISSES.add(cache_key)
        return None
    except json.JSONDecodeError as json_err:
        response_text = response.text if "response" in locals() else "N/A"
        logger.error(
            f"Failed to decode JSON response: {json_err}. Response text: {response_text[:200]}... for URL: {url}"
        )
        CACHE_MISSES.add(cache_key)
        return None
    except (ValueError, TypeError) as e:
        logger.error(
            f"Error processing rate data for {base_currency}->{target_currency} on {date_str}: {e}"
        )
        CACHE_MISSES.add(cache_key)
        return None
    except Exception as e:
        logger.error(
            f"An unexpected error occurred in get_exchange_rate: {e}", exc_info=False
        )
        CACHE_MISSES.add(cache_key)
        return None


def convert_to_target_currency(
    df: pd.DataFrame,
    target_currency: str = "USD",
    price_cols: list = ["open", "high", "low", "close", "adjusted_close"],
) -> pd.DataFrame:
    """
    Adds columns with prices converted to the target currency (e.g., USD).
    Applies the daily exchange rate for the timestamp's date to each price.
    Args:
        df: Standardized DataFrame containing 'timestamp_utc' (datetime),
            'original_currency' (str), and price columns.
        target_currency: The target currency code (e.g., 'USD').
        price_cols: List of price column names to convert.
    Returns:
        DataFrame with added columns (e.g., 'open_usd', 'high_usd', etc.).
        Original columns are preserved. Returns original df if conversion fails.
    """
    if df is None or df.empty:
        logger.warning("Input DataFrame for currency conversion is empty or None.")
        return df

    required_cols = {"timestamp_utc", "original_currency"}
    if not required_cols.issubset(df.columns):
        logger.error(
            f"ERROR: Input DataFrame is missing required columns: {required_cols - set(df.columns)}"
        )
        return df

    if not pd.api.types.is_datetime64_any_dtype(df["timestamp_utc"]):
        try:
            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
        except Exception as e:
            logger.error(f"Failed to convert 'timestamp_utc' to datetime: {e}")
            return df

    logger.info(
        f"Starting currency conversion to {target_currency} for {df.shape[0]} rows."
    )

    df["rate_date"] = df["timestamp_utc"].dt.date

    unique_pairs = df[["original_currency", "rate_date"]].drop_duplicates()
    rates_map = {}

    logger.info(
        f"Found {len(unique_pairs)} unique (currency, date) pairs needing rates."
    )

    for _, row in unique_pairs.iterrows():
        currency = row["original_currency"]
        r_date = row["rate_date"]
        if currency != target_currency and currency is not None and pd.notna(currency):
            rate = get_exchange_rate(currency, target_currency, r_date)
            rates_map[(currency, r_date)] = rate

    df["exchange_rate"] = df.apply(
        lambda row: rates_map.get((row["original_currency"], row["rate_date"]), None)
        if row["original_currency"] != target_currency
        else 1.0,
        axis=1,
    )

    missing_rates_count = df["exchange_rate"].isnull().sum()
    if missing_rates_count > 0:
        logger.warning(
            f"WARNING! Could not find exchange rates for {missing_rates_count} rows."
        )

    for col in price_cols:
        target_col_name = f"{col}_{target_currency.lower()}"
        if col in df.columns:
            df[target_col_name] = df[col] * df["exchange_rate"]
            df[target_col_name] = pd.to_numeric(df[target_col_name], errors="coerce")
            logger.info(f"Created converted column: {target_col_name}")
        else:
            logger.warning(
                f"Price column '{col}' not found in DataFrame, skipping conversion for it."
            )

    df = df.drop(columns=["rate_date", "exchange_rate"])

    logger.info("Currency conversion finished.")
    return df


if __name__ == "__main__":
    print("Starting currency_converter.py for testing...")

    sample_data = {
        "timestamp_utc": pd.to_datetime(
            [
                "2025-04-17 10:00:00",
                "2025-04-17 11:00:00",
                "2025-04-18 10:00:00",
                "2025-04-18 11:00:00",
                "2025-04-18 12:00:00",
                "2025-04-19 09:00:00",
                "2025-04-19 10:00:00",
            ],
            utc=True,
        ),
        "ticker": ["^GDAXI", "^GDAXI", "^FTSE", "^FTSE", "^N225", "^N225", "^GSPC"],
        "original_currency": ["EUR", "EUR", "GBP", "GBP", "JPY", "JPY", "USD"],
        "open": [15000, 15010, 7500, 7505, 30000, 30050, 4500],
        "high": [15050, 15060, 7520, 7525, 30100, 30150, 4510],
        "low": [14980, 15000, 7490, 7500, 29950, 30000, 4490],
        "close": [15040, 15050, 7515, 7520, 30080, 30100, 4505],
        "adjusted_close": [15040, 15050, 7515, 7520, 30080, 30100, 4505],
        "volume": [1000, 1100, 2000, 2100, 5000, 5100, 9000],
    }
    test_df = pd.DataFrame(sample_data)
    print("\nOriginal Test DataFrame:")
    print(test_df)
    print(test_df.info())

    print("\nConverting to USD...")
    converted_df = convert_to_target_currency(test_df.copy(), target_currency="USD")

    if converted_df is not None:
        print("\nConverted DataFrame:")
        print(converted_df)
        print(converted_df.info())

        eur_row = converted_df[converted_df["original_currency"] == "EUR"].iloc[0]
        gbp_row = converted_df[converted_df["original_currency"] == "GBP"].iloc[0]
        jpy_row = converted_df[converted_df["original_currency"] == "JPY"].iloc[0]
        usd_row = converted_df[converted_df["original_currency"] == "USD"].iloc[0]

        print("\nSample Converted Values (first row for each currency):")
        if "open_usd" in eur_row and pd.notna(eur_row["open_usd"]):
            print(
                f"  EUR Open: {eur_row['open']} -> USD Open: {eur_row['open_usd']:.4f}"
            )
        else:
            print("  EUR Open: Conversion likely failed (rate unavailable?)")

        if "open_usd" in gbp_row and pd.notna(gbp_row["open_usd"]):
            print(
                f"  GBP Open: {gbp_row['open']} -> USD Open: {gbp_row['open_usd']:.4f}"
            )
        else:
            print("  GBP Open: Conversion likely failed (rate unavailable?)")

        if "open_usd" in jpy_row and pd.notna(jpy_row["open_usd"]):
            print(
                f"  JPY Open: {jpy_row['open']} -> USD Open: {jpy_row['open_usd']:.4f}"
            )
        else:
            print("  JPY Open: Conversion likely failed (rate unavailable?)")

        if "open_usd" in usd_row and pd.notna(usd_row["open_usd"]):
            print(
                f"  USD Open: {usd_row['open']} -> USD Open: {usd_row['open_usd']:.4f} (Should be ~1:1)"
            )
            assert abs(usd_row["open"] - usd_row["open_usd"]) < 1e-6, (
                "USD conversion failed"
            )
        else:
            print("  USD Open: Conversion failed")
    else:
        print("Currency conversion returned None or failed.")
