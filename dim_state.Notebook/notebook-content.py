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

# # Generate State Dimension
# 
# Updates State dimension. 

# CELL ********************

# MAGIC %%sql
# MAGIC create or replace table gol.dim_state
# MAGIC using delta as 
# MAGIC select distinct 
# MAGIC     gz.state_abbr
# MAGIC     ,c.state_name
# MAGIC from sil.gaz_county gz
# MAGIC left join sil.census c 
# MAGIC     on gz.county_fips = c.county_fips
# MAGIC order by state_abbr;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Validate Row Counts

# CELL ********************

# MAGIC %%sql
# MAGIC select 
# MAGIC     count(distinct state_abbr) as uniq_states
# MAGIC     ,count(*) as row_count
# MAGIC from gol.dim_state;
# MAGIC 
# MAGIC select count(distinct state_abbr) as states from sil.gaz_county;


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
