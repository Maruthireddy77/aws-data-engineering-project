import os

INPUT_PATH = os.getenv(
    "INPUT_PATH",
    "data/raw/customers.csv",
)

OUTPUT_PATH = os.getenv(
    "OUTPUT_PATH",
    "data/processed/customers",
)

LOG_LEVEL = os.getenv(
    "LOG_LEVEL",
    "INFO",
)