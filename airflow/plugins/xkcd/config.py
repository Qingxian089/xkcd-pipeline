from typing import Final

# API Configuration
XKCD_BASE_URL: Final[str] = "https://xkcd.com"
XKCD_INFO_ENDPOINT: Final[str] = "info.0.json"

# API Rate Limiting
DEFAULT_RATE_LIMIT_DELAY: Final[float] = 0.3  # seconds between requests
# MAX_CALLS_PER_MINUTE: Final[int] = 100

# Retry Configuration
DEFAULT_RETRY_LIMIT: Final[int] = 3
DEFAULT_RETRY_MIN_DELAY: Final[int] = 1  # seconds
DEFAULT_RETRY_MAX_DELAY: Final[int] = 10  # seconds
DEFAULT_RETRY_MULTIPLIER: Final[int] = 2

# Batch Processing
DEFAULT_BATCH_SIZE: Final[int] = 80

# Database Configuration
DEFAULT_POSTGRES_CONN_ID: Final[str] = "xkcd_postgres"
DEFAULT_SCHEMA: Final[str] = "xkcd"
DEFAULT_TABLE: Final[str] = "raw_xkcd_comics"

# Logging
DEFAULT_LOG_LEVEL: Final[int] = "INFO"

# HTTP Status Codes
HTTP_OK: Final[int] = 200
HTTP_NOT_FOUND: Final[int] = 404

# Task Configuration
MAX_ACTIVE_RUNS: Final[int] = 1
POLLING_INTERVAL_MINUTES: Final[int] = 60  # minutes
MAX_POLLING_RETRIES: Final[int] = 16  # 8:00 - 24:00

# DBT Configuration
DBT_PROJECT_DIR: Final[str] = "/opt/airflow/dbt/xkcd_analytics"
DEFAULT_MODEL_NAME: Final[str] = "stg_xkcd_comics+"  # Default: Run stg_xkcd_comics and all downstream models
