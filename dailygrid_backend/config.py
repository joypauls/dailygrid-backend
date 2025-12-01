from dotenv import load_dotenv
import os

load_dotenv()

EIA_API_KEY = os.getenv("EIA_API_KEY")
EIA_BASE_URL = "https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/"
if not EIA_API_KEY:
    raise RuntimeError("EIA_API_KEY not found in environment")

PROCESSED_OUTPUT_FILE = "public/data/daily_energy_mix_latest.json"
RAW_OUTPUT_FILE = "public/data/raw.json"

SUPPORTED_REGIONS = [
    "US48",  # all lower 48 states
    "CISO",  # CAISO
    # "ERCO",  # ERCOT
]

DEFAULT_TIMEZONE = "Central"
