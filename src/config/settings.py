import json
import os
from pathlib import Path

from dotenv import load_dotenv

env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
INDICES_CONFIG_PATH = BASE_DIR / "src" / "config" / "indices.json"


def load_indices_config():
    """Loads the indices configuration from a JSON file."""
    try:
        with open(INDICES_CONFIG_PATH, "r", encoding="utf-8") as f:
            indices = json.load(f)
        if indices and isinstance(indices, list):
            required_keys = {"name", "ticker", "country", "exchange", "currency"}
            if not required_keys.issubset(indices[0].keys()):
                print(
                    f"WARNING! Some required keys are missing in indices.json for {indices[0].get('name')}"
                )
        else:
            raise ValueError("Indices config is empty or NOT a list!")
        return indices
    except FileNotFoundError:
        print(f"ERROR: Indices config file not found at {INDICES_CONFIG_PATH}")
        return []
    except json.JSONDecodeError:
        print(f"ERROR: Invalid JSON format in {INDICES_CONFIG_PATH}")
        return []
    except Exception as e:
        print(f"ERROR: An error occurred while loading indices config {e}")
        return []


INDICES = load_indices_config()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
    print(
        "WARNING! Database credentials (DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)"
        " are missing or not configured! Check .env file."
    )

FETCH_PERIOD = "2d"
FETCH_INTERVAL = "60m"
DATA_FETCH_INTERVAL_HOURS = 6
TARGET_CURRENCY = "USD"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
TIMEZONE = "UTC"

YFINANCE_TIMEOUT = 30