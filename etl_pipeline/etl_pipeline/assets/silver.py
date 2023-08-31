import pandas as pd
import numpy as np
from dagster import asset, Output, AssetIn


@asset(
    ins={
        "bronze_campaign_desc": AssetIn(key_prefix=["bronze"]),
        "bronze_campaign_table": AssetIn(key_prefix=["bronze"])
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["silver"],
    compute_kind="MinIO",
    group_name="silver"
)
def campaign_per_household(
    bronze_campaign_desc,
    bronze_campaign_table
) -> Output[pd.DataFrame]:

    # We call campaign the new dataframe merging the dataset
    campaign_per_household = pd.merge(bronze_campaign_desc[['CAMPAIGN', 'START_DAY']],
                        bronze_campaign_table[['household_key', 'CAMPAIGN']],
                        on="CAMPAIGN",
                        how="left")

    # Count number of campaign per household
    campaign_per_household['num_campaign'] = campaign_per_household.groupby(by='household_key')['CAMPAIGN'].transform('count')

    # Delete useless column
    campaign_per_household = campaign_per_household.drop(columns=['CAMPAIGN', 'START_DAY'])

    # Delete duplicates
    campaign_per_household.drop_duplicates(subset=['household_key', 'num_campaign'], keep="first", inplace=True)

    return Output(
            campaign_per_household,
            metadata={
                    "table": "campaign",
                    "records count": len(campaign_per_household),
            },
    )


@asset(
    ins={
        "bronze_coupon_redempt": AssetIn(key_prefix=["bronze"])
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["silver"],
    compute_kind="MinIO",
    group_name="silver"
)
def redemption_per_household(
    bronze_coupon_redempt
) -> Output[pd.DataFrame]:

    # Drop useless columns
    bronze_coupon_redempt = bronze_coupon_redempt.drop(columns=['DAY', 'COUPON_UPC'])

    # Keep only one occurence of coupon redeemed by campaign
    bronze_coupon_redempt.drop_duplicates(
        subset=['household_key', 'CAMPAIGN'],
        keep="first", inplace=True
    )

    # Count number of campaign the customer redeemed at least one coupon
    redemption_per_household = bronze_coupon_redempt.groupby(['household_key'], as_index=False)['CAMPAIGN'].agg(
        {'redeemed': pd.Series.nunique})

    return Output(
            redemption_per_household,
            metadata={
                    "table": "redemption_per_household",
                    "records count": len(redemption_per_household),
            },
    )


@asset(
    ins={
        "redemption_per_household": AssetIn(key_prefix=["silver"]),
        "campaign_per_household": AssetIn(key_prefix=["silver"])
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["silver"],
    compute_kind="MinIO",
    group_name="silver"
)
def sensitivity(
    redemption_per_household,
    campaign_per_household
) -> Output[pd.DataFrame]:

    # Merging of campaign and coupon redemption tables
    sensitivity = pd.merge(campaign_per_household, redemption_per_household, on=['household_key'], how="left")

    # Creation of our output variable
    sensitivity["sensitivity"] = np.where(sensitivity["redeemed"] > 0, 'Sensible', 'Not sensible')

    sensitivity = sensitivity.replace(np.nan, 0)

    return Output(
            sensitivity,
            metadata={
                    "table": "sensitivity",
                    "records count": len(sensitivity),
            },
    )


@asset(
    ins={
        "bronze_transaction_data": AssetIn(key_prefix=["bronze"])
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["silver"],
    compute_kind="MinIO",
    group_name="silver"
)
def sales_data(bronze_transaction_data) -> Output[pd.DataFrame]:

    # Exclude transactions related to returns
    bronze_transaction_data = bronze_transaction_data[bronze_transaction_data['SALES_VALUE'] > 0]
    bronze_transaction_data = bronze_transaction_data[bronze_transaction_data['QUANTITY'] > 0]

    # Calculate total sales per customer
    total_sales = bronze_transaction_data.groupby(by='household_key', as_index=False)['SALES_VALUE'].sum().rename(
        columns={'SALES_VALUE': 'Total_sales'})

    # Calculate total number of visits per customer
    total_visits = bronze_transaction_data.groupby(['household_key'], as_index=False)['BASKET_ID'].agg(
        {'total_visits': pd.Series.nunique})

    # Calculate median basket amount per customer
    temp_basket = bronze_transaction_data.groupby(['household_key', 'BASKET_ID'], as_index=False)['SALES_VALUE'].sum()
    temp_median_basket = temp_basket.groupby(['household_key'], as_index=False)['SALES_VALUE'].median().rename(
        columns={'SALES_VALUE': 'median_basket'})

    # Calculate average product price bought per customer
    temp_product = bronze_transaction_data.groupby(['household_key'], as_index=False)['SALES_VALUE'].mean().rename(
        columns={'SALES_VALUE': 'avg_price'})

    sales_data = total_sales.merge(total_visits, on='household_key')\
        .merge(temp_median_basket, on='household_key')\
        .merge(temp_product, on='household_key')


    return Output(
            sales_data,
            metadata={
                    "table": "sales_data",
                    "records count": len(sales_data),
            },
    )


@asset(
    ins={
        "bronze_hh_demographic": AssetIn(key_prefix=["bronze"]),
        "sales_data": AssetIn(key_prefix=["silver"])
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["silver"],
    compute_kind="MinIO",
    group_name="silver"
)
def sales_per_hh_demographic(
    bronze_hh_demographic,
    sales_data
) -> Output[pd.DataFrame]:

    sales_per_hh_demographic = bronze_hh_demographic.merge(sales_data, on='household_key')

    return Output(
            sales_per_hh_demographic,
            metadata={
                    "table": "sales_per_hh_demographic",
                    "records count": len(sales_per_hh_demographic),
            },
    )
