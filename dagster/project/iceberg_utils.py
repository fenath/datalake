# Helper para criação de tabelas
import logging
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.transforms import IdentityTransform
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField
from pyiceberg.io.pyarrow import pyarrow_to_schema


logger = logging.getLogger("iceberg_utils")


def build_partition_spec(schema, partition_cols):
    # nenhum particionamento
    if not partition_cols:
        return PartitionSpec()

    fields = []

    for i, col in enumerate(partition_cols, start=1000):
        field = schema.find_field(col)

        if field is None:
            raise ValueError(f"Coluna '{col}' não existe no schema")

        fields.append(
            PartitionSpec.Field(
                source_id=field.field_id,
                field_id=i,
                transform=IdentityTransform(),
                name=col,
            )
        )

    return PartitionSpec(*fields)

def update_schema(table, arrow_schema):
    """
    Atualiza o schema da tabela Iceberg se houver colunas novas no DataFrame.
    
    Parameters
    ----------
    table : Iceberg table
        Tabela a ser verificada/atualizada
    arrow_schema : pyarrow.Schema
        Schema do DataFrame que será inserido
        
    Returns
    -------
    bool
        True se o schema foi atualizado, False caso contrário
    """ 
    # Atualiza o schema adicionando as novas colunas
    with table.update_schema() as update:
        update.union_by_name(arrow_schema)
    
    logger.info(f"✔ schema atualizado")
    return True


def set_table(
    catalog,
    table_name: str,
    df,
    partition_cols=None,
    mode="append",
):
    """
    Cria ou carrega uma tabela Iceberg e escreve dados nela.

    Parameters
    ----------
    catalog : Iceberg catalog
    table_name : str
    df : polars.DataFrame
    partition_cols : list[str] | None
    mode : 'append' | 'overwrite'
    """

    # garante nomes limpos
    df = df.rename(lambda c: c.strip())

    arrow_table = df.to_arrow()
    arrow_schema = arrow_table.schema

    # -------------------
    # load or create
    # -------------------
    try:
        table = catalog.load_table(table_name)
        update_schema(table, arrow_schema)
        logger.info(f"✔ tabela '{table_name}' carregada")

    except Exception as e:
        if "NoSuchTable" not in str(type(e)):
            raise  # erro real → não cria tabela
    
        logger.info(f"⚙ criando tabela '{table_name}'")
    
        partition_spec = build_partition_spec(arrow_schema, partition_cols)
    
        table = catalog.create_table(
            identifier=table_name,
            schema=arrow_schema,
            partition_spec=partition_spec,
            properties={
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "schema.name-mapping.default": "true",
            },
        )
    # -------------------
    # write
    # -------------------
    if mode == "overwrite":
        table.overwrite(arrow_table)
    elif mode == "append":
        table.append(arrow_table)
    elif mode in ("reset"):
        logger.info("⚠ full reset da tabela")
        table.delete("true")   # remove todos datafiles ativos
        table.append(arrow_table)

    logger.info(f"✔ dados gravados ({mode})")

    return table
