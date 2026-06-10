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

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC create table if not exists dbo.cdc_places_releases (
# MAGIC     release_year int,
# MAGIC     dataset_id string,
# MAGIC     data_year int
# MAGIC ) using delta;
# MAGIC 
# MAGIC insert into dbo.cdc_places_releases
# MAGIC values
# MAGIC     (2025, 'i46a-9kgh', 2023)
# MAGIC     ,(2024, 'd3i6-k6z5', 2022)
# MAGIC     ,(2023 , '7cmc-7y5g', 2021)
# MAGIC     ,(2022, 'xyst-f73f', 2020)
# MAGIC     ,(2021, 'kmvs-jkvx', 2019)
# MAGIC     ,(2020, 'mssc-ksj7', 2018)
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
