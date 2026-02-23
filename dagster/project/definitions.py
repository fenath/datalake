from dagster import Definitions

from .bronze import raw_excel
from .silver import carros_astro
from .ingestion_table import ensure_ingestion_table
from .resources import iceberg_catalog, s3_client

from .crypto_candles.assets import crypto_assets
from .crypto_candles.assets.sensors import minio_csv_sensor
from .crypto_candles.assets.ops import crypto_ingestion_job, log_job


defs = Definitions(
    assets=[
        raw_excel, 
        carros_astro,
        ensure_ingestion_table,
        *crypto_assets
        ],
    resources={
        "iceberg_catalog": iceberg_catalog,
        "s3_client": s3_client,
    },
    sensors=[
        minio_csv_sensor
        ],
    jobs=[
        crypto_ingestion_job,
        log_job
        ]
)

