from typing import cast
from dagster import asset, AssetExecutionContext
import polars as pl
from pyiceberg.expressions import In
from pyiceberg.io.pyarrow import schema_to_pyarrow

@asset(required_resource_keys={"iceberg_catalog"})
def silver_crypto_candles(
    context: AssetExecutionContext,
    bronze_crypto_candles
):
    catalog = context.resources.iceberg_catalog

    table = catalog.load_table("bronze.crypto_candles")

    df = cast(pl.DataFrame, pl.from_arrow(table.scan().to_arrow()))

    df = df.rename({"open_time": "timestamp"})

    df = df.with_columns(
        timestamp=pl.from_epoch(pl.col("timestamp"), time_unit="ms"),
        symbol=pl.lit("BTCUSDT"),
        timeframe=pl.lit("5m"),
    )

    files = df["_source_file"].unique().to_list()

    silver_table = catalog.load_table("silver.cripto_currency")

    iceberg_fields = [f.name for f in silver_table.schema().fields]
    df = df.select(iceberg_fields)

    arrow_schema = schema_to_pyarrow(silver_table.schema())
    arrow_table = df.to_arrow().cast(arrow_schema)

    silver_table.overwrite(
        arrow_table,
        overwrite_filter=In("_source_file", files),
    )

    context.log.info("✅ Novos valores corretos")

    return "silver.cripto_currency"
