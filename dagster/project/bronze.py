from dagster import asset, AssetExecutionContext
import polars as pl
from datetime import datetime
from io import BytesIO

from .iceberg_utils import set_table


@asset(group_name="bronze")
def raw_excel(context: AssetExecutionContext):

    iceberg_catalog = context.resources.iceberg_catalog
    s3_client = context.resources.s3_client

    bucket = "raw"
    key = "carros_astro/ENTRADAS E SAÍDAS ASTRO AUTOMÓVEIS.xlsx"

    response = s3_client.get_object(Bucket=bucket, Key=key)
    buffer = BytesIO(response["Body"].read())

    df = pl.read_excel(buffer, has_header=False)
    df = df.with_columns(pl.all().cast(pl.Utf8))

    df = df.with_columns(
        pl.lit(key).alias("_source_file"),
        pl.lit(datetime.utcnow().isoformat()).alias("_ingestion_time"),
    )

    set_table(
        iceberg_catalog,
        "bronze.raw_excel",
        df,
        mode="reset",
    )

    return df

