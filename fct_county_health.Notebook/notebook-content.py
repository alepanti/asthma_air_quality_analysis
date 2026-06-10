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

try:
    release_year = int(mssparkutils.widgets.get('release_year'))
except:
    release_year = int(spark.sql("""
        select max(release_year) 
        from silver.cdc_places
    """).collect()[0][0])

print(f'Building fact table for CDC {release_year} Release')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC create table if not exists gold.fct_county_health (
# MAGIC     county_fips string
# MAGIC     ,asthma_crude_prev double
# MAGIC     ,smoke_crude_prev double
# MAGIC     ,obesity_crude_prev double
# MAGIC     ,copd_crude_prev double
# MAGIC     ,insur_access_crude_prev double
# MAGIC     ,ozone_mean double
# MAGIC     ,ozone_98p double
# MAGIC     ,pm25_mean double
# MAGIC     ,pm25_98p double
# MAGIC     ,median_income int
# MAGIC     ,poverty_rate double
# MAGIC     ,pct_white double
# MAGIC     ,pct_black_aa double
# MAGIC     ,pct_hisp_lat double
# MAGIC     ,pop_density double
# MAGIC     ,data_year int
# MAGIC ) using delta;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql()
insert overwrite table gold.fct_county_health
select distinct
    cdc.county_fips
    ,cdc.CASTHMA_CrudePrev as asthma_crude_prev
    ,cdc.CSMOKING_CrudePrev as smoke_crude_prev
    ,cdc.OBESITY_CrudePrev as obesity_crude_prev
    ,cdc.COPD_CrudePrev as copd_crude_prev
    ,cdc.ACCESS2_CrudePrev as insur_access_crude_prev
    ,oz.arithmetic_mean as ozone_mean
    ,oz.ninety_eighth_percentile as ozone_98p
    ,pm.arithmetic_mean as pm25_mean
    ,pm.ninety_eighth_percentile as pm25_98p
    ,cen.med_house_income as median_income
    ,cen.percent_below_pov as poverty_rate
    ,cen.percent_white as pct_white
    ,cen.percent_black_aa as pct_black_aa
    ,cen.percent_hisp_lat as pct_hisp_lat
    ,(d.total_pop / d.land_area_sqmi) as pop_density
    ,cdc.data_year
from silver.cdc_places cdc 
left join silver.aqs_oz oz
    on cdc.county_fips = oz.county_fips
left join silver.aqs_pm25 pm
    on cdc.county_fips = pm.county_fips
left join silver.census cen
    on cdc.county_fips = cen.county_fips
left join gold.dim_county d
    on cdc.county_fips = d.county_fips
where cdc.release_year = {release_year}
order by county_fips;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(distinct county_fips) from gol.fct_county_health;
# MAGIC select count(distinct *) from sil.cdc_places;
# MAGIC select count(1) from sil.cdc_places;
# MAGIC select count(distinct county_fips) from sil.cdc_places;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(1) from gol.fct_county_health
# MAGIC where asthma_crude_prev is null;

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
