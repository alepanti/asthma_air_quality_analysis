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

# # Generate County Dimension
# 
# Generate County Dimension from updated County and CDC Silver tables.

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC create or replace table gold.dim_county
# MAGIC using delta as 
# MAGIC select
# MAGIC     gc.county_fips
# MAGIC     ,gc.county_name
# MAGIC     ,gc.state_abbr
# MAGIC from silver.gaz_county gc
# MAGIC order by gc.county_fips;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Validate Row Counts

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select 
# MAGIC     count(distinct county_fips) as counties
# MAGIC     ,count(1) as row_count
# MAGIC from gold.dim_county;
# MAGIC 
# MAGIC select 
# MAGIC     count(distinct county_fips) as counties
# MAGIC from silver.gaz_county;

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
