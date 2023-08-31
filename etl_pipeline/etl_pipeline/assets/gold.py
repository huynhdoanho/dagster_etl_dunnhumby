import pandas as pd
from dagster import asset, Output, AssetIn, AssetOut, multi_asset


@asset(
    ins={
        "sales_per_hh_demographic": AssetIn(key_prefix=["silver"]),
        "sensitivity": AssetIn(key_prefix=["silver"]),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["gold"],
    compute_kind="MinIO",
    group_name="gold"
)
def gold_dataset(
    sales_per_hh_demographic,
    sensitivity
) -> Output[pd.DataFrame]:

    gold_dataset = pd.merge(
        sales_per_hh_demographic,
        sensitivity[['household_key', 'sensitivity']],
        on=['household_key'],
        how="left"
    )

    gold_dataset = gold_dataset[gold_dataset["sensitivity"].isna() == False]

    gold_dataset = gold_dataset.drop(columns=['household_key'])

    return Output(
        gold_dataset,
        metadata={
            "table": "gold_dataset",
            "records count": len(gold_dataset),
        },
    )


@multi_asset(
    ins={
        "gold_dataset": AssetIn(
            key_prefix=["gold"],
        )
    },
    outs={
        "dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "table": "dataset"
            }
        )
    },
    compute_kind="PostgreSQL",
    group_name="warehouse"
)
def dataset(gold_dataset) -> Output[pd.DataFrame]:
    return Output(
        gold_dataset,
        metadata={
            "schema": "public",
            "table": "gold_dataset",
            "records counts": len(gold_dataset),
        },
    )