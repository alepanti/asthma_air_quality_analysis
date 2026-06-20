# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e356aa96-deef-4254-b67d-257cead402cc",
# META       "default_lakehouse_name": "capstone_lh",
# META       "default_lakehouse_workspace_id": "f51179d3-a609-4970-b80c-ca7cecaccc24",
# META       "known_lakehouses": [
# META         {
# META           "id": "e356aa96-deef-4254-b67d-257cead402cc"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Build Model Dataset and EDA
# 
# Build the dataset from gold.fct_county_health to be used for model building and training. Perform exploratory data analysis (EDA) on chosen features.

# CELL ********************

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Load Fact Table

# CELL ********************

fct_df = spark.read.table('gold.fct_county_health')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Select Features
# 
# Drop rows with NULL in any features.

# CELL ********************

columns = [
    'asthma_crude_prev'
    ,'pm25_mean'
    ,'ozone_mean'
    ,'median_income'
    ,'poverty_rate'
    ,'pct_white'
    ,'pct_black_aa'
    ,'pct_hisp_lat'
    ,'pop_density'
    ,'smoke_crude_prev'
    ,'obesity_crude_prev'
]

model_df = fct_df.select(columns)
model_df = model_df.dropna()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f'Total fact rows: {fct_df.count()}')
print(f'Total rows to build model: {model_df.count()}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Save Model Dataset to Gold Table

# CELL ********************

model_df.write.format('delta').mode('overwrite') \
    .saveAsTable('capstone_lh.gold.ml_county_health')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## EDA

# MARKDOWN ********************

# #### Summary Statistics

# CELL ********************

model_df = model_df.toPandas()
summary_pd = model_df.describe().T.reset_index()
summary_pd = summary_pd.rename(columns={'index': 'variable'})

display(spark.createDataFrame(summary_pd))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Correlation Matrix

# CELL ********************

corr_mtx = model_df.corr(numeric_only=True)

display(spark.createDataFrame(
    corr_mtx.reset_index().rename(columns={'index': 'variable'}))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Heat Map

# CELL ********************

plt.figure()
plt.imshow(corr_mtx)
plt.colorbar(label='Correlation')
plt.xticks(range(len(corr_mtx.columns)), corr_mtx.columns, rotation=90)
plt.yticks(range(len(corr_mtx.index)), corr_mtx.index)
plt.title('Correlation Matrix Heat Map for Asthma Prevalence Variables')
plt.tight_layout()
plt.savefig(f'/lakehouse/default/Files/figures/heatmap_asthma.png')
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Distribution Plots

# CELL ********************

for col in columns:
    values = model_df[col]

    plt.figure(figsize=(7, 4))
    plt.hist(values, bins=30)
    plt.title(f'Distribution of {col}')
    plt.xlabel(col)
    plt.ylabel('Count')
    plt.tight_layout()
    plt.savefig(f'/lakehouse/default/Files/figures/{col}.png')
    plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
