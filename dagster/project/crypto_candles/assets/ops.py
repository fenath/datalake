from pyiceberg.io.pyarrow import schema_to_pyarrow
from typing import cast
from io import BytesIO
import dagster as dg
from dagster import Config, op, job, AssetExecutionContext
import polars as pl
from pyiceberg.expressions import EqualTo, In
from project.utils.ingestion import add_ingestion_metadata

# Definimos a estrutura dos parâmetros
class IngestionConfig(Config):
    file_path: str
    symbol: str
    time_frame: str

@op(required_resource_keys={"iceberg_catalog", "s3_client"})
def ingest_crypto_file_op(context, config: IngestionConfig):
    # Acessamos os parâmetros via config
    file_path = config.file_path
    symbol = config.symbol
    time_frame = config.time_frame
    
    s3 = context.resources.s3_client
    catalog = context.resources.iceberg_catalog
    
    context.log.info(f"Ingestando {symbol} ({time_frame}) do caminho: {file_path}")

    table_name = "bronze.crypto_candles"
    key = file_path
    bucket = "raw"

    context.log.info(f"Ingesting {key}")

    response = s3.get_object(Bucket=bucket, Key=key)
    buffer = BytesIO(response["Body"].read())

    df = pl.read_csv(buffer)
    df = add_ingestion_metadata(df, key)
    context.log.info(df.head(10))

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
    
    return { 
            "status": "success", 
            "symbol": symbol, 
            "time_frame": time_frame,
            "file_path": file_path
            }

@op(required_resource_keys={"iceberg_catalog", "s3_client"})
def silver_crypto(context, bronze_metadata):
    # Este op recebe o retorno do anterior (bronze_metadata)
    table_name = "silver.crypto_candles"
    catalog = context.resources.iceberg_catalog
    file_path = bronze_metadata["file_path"]
    symbol = bronze_metadata["symbol"]
    time_frame = bronze_metadata["time_frame"]
    
    context.log.info(f"Iniciando tratamento Silver para {symbol}")
    table = catalog.load_table("bronze.crypto_candles")
    table = table.scan(
            row_filter=f"_source_file='{file_path}'"
            )

    df = cast(pl.DataFrame, pl.from_arrow(table.to_arrow()))
    df = df.rename({"open_time": "timestamp"})
    df = df.with_columns(
        timestamp=pl.from_epoch(pl.col("timestamp"), time_unit="ms"),
        symbol=pl.lit(symbol),
        timeframe=pl.lit(time_frame),
    )

    files = df["_source_file"].unique().to_list()
    silver_table = catalog.load_table("silver.cripto_currency")
    iceberg_fields = [f.name for f in silver_table.schema().fields]
    df = df.select(iceberg_fields)
    arrow_schema = schema_to_pyarrow(silver_table.schema())
    arrow_table = df.to_arrow().cast(arrow_schema)
    silver_table.overwrite(
        arrow_table,
        overwrite_filter=In("_source_file", files), #type: ignore
    )

    context.log.info("✅ Novos valores corretos")
    return table_name

@job
def crypto_ingestion_job():
    bronze_dict = ingest_crypto_file_op()
    silver_crypto(bronze_dict)

@op
def log_config(context, config: Config):
    context.log.info(f"config: {config.__dict__}")

@job
def log_job():
    log_config()
