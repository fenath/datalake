from dagster import asset, AssetExecutionContext
import polars as pl
from .iceberg_utils import set_table


@asset(
    group_name="silver",
    deps=["raw_excel"],
)
def carros_astro(context: AssetExecutionContext):

    iceberg_catalog = context.resources.iceberg_catalog

    table = iceberg_catalog.load_table("bronze.raw_excel")
    df = pl.from_arrow(table.scan().to_arrow())

    anos = df["column_4"].str.to_integer(strict=False).unique().to_list()
    first_ano = [a for a in anos if a is not None][0] - 1

    df = df.with_columns(
        ano_raw=(
            pl.col("column_4")
            .str.to_integer(strict=False)
            .forward_fill()
            .fill_null(first_ano)
        ),
        is_label=(
            pl.col("column_4").is_not_null()
            & pl.col("column_1").is_null()
        ),
    )

    df = df.drop_nulls(subset=["column_1"])

    silver_df = df.select([
        "column_1",
        "column_4",
        "_source_file",
        "_ingestion_time",
    ])

    set_table(
        iceberg_catalog,
        "silver.carros_astro",
        silver_df,
        mode="reset",
    )

    return silver_df

