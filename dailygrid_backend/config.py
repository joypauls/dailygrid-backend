from dotenv import load_dotenv
import os

load_dotenv()

EIA_API_KEY = os.getenv("EIA_API_KEY")
EIA_BASE_URL = "https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/"
OUTPUT_FILE = "public/data/daily_energy_mix_latest.json"

if not EIA_API_KEY:
    raise RuntimeError("EIA_API_KEY not found in environment")
