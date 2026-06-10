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

data_year = int(spark.sql(f"""
    select data_year
    from dbo.cdc_places_releases
    where release_year = {release_year}
""").collect()[0][0])

print(f'Building fact table for CDC {release_year} Release, Based on {data_year} Data')

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
# MAGIC     ,release_year int
# MAGIC ) using delta;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
    delete from gold.fct_county_health
    where data_year = {data_year};
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
    insert into gold.fct_county_health
    select distinct
        cdc.county_fips
        ,cdc.CASTHMA_CrudePrev as asthma_crude_prev
        ,cdc.CSMOKING_CrudePrev as smoke_crude_prev
        ,cdc.OBESITY_CrudePrev as obesity_crude_prev
        ,oz.arithmetic_mean as ozone_mean
        ,oz.ninety_eighth_percentile as ozone_98p
        ,pm.arithmetic_mean as pm25_mean
        ,pm.ninety_eighth_percentile as pm25_98p
        ,cen.med_house_income as median_income
        ,cen.percent_below_pov as poverty_rate
        ,cen.percent_white as pct_white
        ,cen.percent_black_aa as pct_black_aa
        ,cen.percent_hisp_lat as pct_hisp_lat
        ,(cdc.total_pop / c.land_area_sqmi) as pop_density
        ,cdc.data_year
        ,cdc.release_year
    from silver.cdc_places cdc 
    left join silver.aqs_oz oz
        on cdc.county_fips = oz.county_fips
        and oz.data_year = {data_year}
    left join silver.aqs_pm25 pm
        on cdc.county_fips = pm.county_fips
        and pm.data_year = {data_year}
    left join silver.census cen
        on cdc.county_fips = cen.county_fips
        and cen.data_year = {data_year}
    left join silver.gaz_county c
        on cdc.county_fips = c.county_fips
    where cdc.release_year = {release_year}
    order by county_fips;
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

count = spark.sql(f"""
    select count(*) AS cnt 
    from gold.fct_county_health 
    where data_year = {data_year}
""").collect()[0]['cnt']

null_asthma = spark.sql(f"""
    select COUNT(*) AS cnt 
    from gold.fct_county_health 
    where 
        data_year = {data_year}
        and asthma_crude_prev IS NULL
""").collect()[0]['cnt']

print(f'null asthma count: {null_asthma}')
print(f'row count for year: {count}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
    select count(1) as silver_count 
    from silver.cdc_places
    where data_year = {data_year}
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(distinct county_fips) from 
# MAGIC silver.aqs_oz;
# MAGIC 
# MAGIC select count(distinct county_fips) FROM
# MAGIC silver.aqs_pm25;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
