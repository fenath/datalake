from dagster import asset, AssetExecutionContext
import polars as pl
from io import BytesIO
from pyiceberg.expressions import EqualTo
from project.utils.ingestion import add_ingestion_metadata

bucket = "raw"

@asset(required_resource_keys={"s3_client", "iceberg_catalog"})
def bronze_crypto_candles(
    context: AssetExecutionContext,
    raw_crypto_files
):
    s3 = context.resources.s3_client
    catalog = context.resources.iceberg_catalog

    table_name = "bronze.crypto_candles"

    for key in raw_crypto_files:
        context.log.info(f"Ingesting {key}")

        response = s3.get_object(Bucket=bucket, Key=key)
        buffer = BytesIO(response["Body"].read())

        df = pl.read_csv(buffer)
        df = add_ingestion_metadata(df, key)

        arrow_table = df.to_arrow()

        table = catalog.create_table_if_not_exists(
            table_name,
            schema=arrow_table.schema
        )

        table.overwrite(
            arrow_table,
            overwrite_filter=EqualTo("_source_file", key),
        )

    context.log.info("✔ RAW ingestão concluída")

    return table_name
