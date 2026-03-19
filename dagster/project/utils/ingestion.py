import polars as pl
from datetime import datetime, timezone

def add_ingestion_metadata(df, file): 
    return df.with_columns(
        pl.lit(file).alias("_source_file"),
        pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingestion_time"), 
    )

