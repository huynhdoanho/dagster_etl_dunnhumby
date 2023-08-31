import pandas as pd
from dagster import asset, Output, AssetsDefinition, Definitions


def make_assets(
    table_name: str,
    io_manager_key: str = "minio_io_manager",
    compute_kind: str = "MySQL",
    group_name: str = "bronze"
) -> AssetsDefinition:

    @asset(
        name=f"bronze_{table_name}",
        io_manager_key=io_manager_key,
        compute_kind=compute_kind,
        group_name=group_name,
        required_resource_keys={"mysql_io_manager"},
        key_prefix=["bronze"],
    )
    def bronze_table(context) -> Output[pd.DataFrame]:
        # define SQL statement
        sql_stm = f"SELECT * FROM {table_name}"
        # use context with resources mysql_io_manager (defined by required_resource_keys)
        # using extract_data() to retrieve data as Pandas Dataframe
        pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
        # return Pandas Dataframe
        # with metadata information
        return Output(
                        pd_data,
                        metadata={
                                    "table": f"{table_name}",
                                    "records count": len(pd_data),
                                },
                        )
    return bronze_table

