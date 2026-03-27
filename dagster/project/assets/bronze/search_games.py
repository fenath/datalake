import dagster as dg
from project.resources import IcebergDuckDBResource

@dg.asset
def game_schedule_not_requested(
        context: dg.AssetExecutionContext,
        duckdb: IcebergDuckDBResource):
    with duckdb.get_connection() as con:
        with open(__file__ + "/queries/incremental_schedule_generator.sql") as f:
            qr = f.read()

        dates = con.sql(qr).fetchall()

        context.log_event(
                dg.AssetMaterialization(
                    asset_key="game_schedule_not_requested",
                    metadata={
                        "requests": str(dates)
                        }
                    )
                )
        return dates
