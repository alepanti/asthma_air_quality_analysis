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

# # Create Gold Layer Geography Dimension
# 
# Generate dimension for county location data.

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC create or replace table gold.dim_geography
# MAGIC using delta as 
# MAGIC select distinct 
# MAGIC     county_fips
# MAGIC     ,latitude
# MAGIC     ,longitude
# MAGIC from silver.gaz_county
# MAGIC order by county_fips;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Validate Row Count

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(distinct county_fips) as counties
# MAGIC     ,count(1) as row_count
# MAGIC from gold.dim_geography;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
