from dagster import AssetExecutionContext, asset
from pyiceberg.schema import Schema
from pyiceberg.types import TimestampType, StringType, FloatType, DoubleType, NestedField


@asset
def ensure_ingestion_table(
        context: AssetExecutionContext,
        ):

    catalog = context.resources.iceberg_catalog
    table_name = "metadata.ingested_files"

    # Criando o namespace de controle se não existir
    if "metadata" not in catalog.list_namespaces():
        catalog.create_namespace("metadata")

    schema = Schema(
            NestedField(field_id=1, name="file_path", field_type=StringType(), required=True),
            NestedField(field_id=2, name="checksum", field_type=StringType(), required=True),
            NestedField(field_id=3, name="ingested_at", field_type=TimestampType(), required=True),
            NestedField(field_id=4, name="status", field_type=StringType(), required=True),
            )

    if not catalog.table_exists(table_name):
        catalog.create_table(
            table_name,
            schema=schema # Defina o schema com file_path, checksum, etc.
        )
    return
