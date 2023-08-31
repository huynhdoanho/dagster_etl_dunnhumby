import pandas as pd
import numpy as np

# redemption_per_household = pd.read_csv("D:/FDE/final_project/etl_pipeline/redemption_per_household.csv")
# print(np.unique(redemption_per_household["redeemed"]))

df = pd.read_csv("D:/FDE/final_project/etl_pipeline/sensitivity.csv")
df2 = pd.read_csv("D:/FDE/final_project/etl_pipeline/sales_per_hh_demographic.csv")

# df = df.replace(np.nan, 0)

# sensitivity = pd.read_csv("D:/FDE/final_project/etl_pipeline/sensitivity.csv")
#
# sensitivity = sensitivity.replace(np.nan, 0)

print(df['household_key'].shape)

print(df2.shape)