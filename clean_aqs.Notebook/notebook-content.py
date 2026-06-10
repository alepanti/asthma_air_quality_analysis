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

# # Clean AQS Data
# 
# Cleans raw AQS Ozone and PM2.5 Pollutant data for given year. Loads into silver table.

# CELL ********************

from pyspark.sql.functions import concat, lpad, col, sum

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Grab Data Year

# CELL ********************

try:
    release_year = int(mssparkutils.widgets.get('release_year'))
except:
    release_year = int(spark.sql("""
        select max(release_year) AS latest
        from bronze.cdc_places
    """).collect()[0]['latest'])

data_year = int(spark.sql(f"""
    select data_year as data_year
    from bronze.cdc_places
    where release_year = {release_year}
""").collect()[0]['data_year'])

print(f'Cleaning {data_year} AQS Data')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Ingest Raw Ozone Data for Given Year

# CELL ********************

raw_oz = spark.read.table('capstone_lh.bronze.aqs_oz') \
    .filter(col('year') == data_year)
raw_oz.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Drop Unnecessary Columns

# CELL ********************

oz_df = raw_oz.select(
    'state_code',
    'county_code',
    'year',
    'pollutant_standard',
    'observation_percent',
    'validity_indicator',
    'arithmetic_mean',
    'ninety_eighth_percentile' 
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### List Pollutant Standards

# CELL ********************

oz_df.select('pollutant_standard').distinct().show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Select Valid Data from Ozone 8-Hour 2015 Standard

# CELL ********************

oz_df = oz_df.filter(
    (col('pollutant_standard') == 'Ozone 8-hour 2015') &
    (col('validity_indicator') == 'Y')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Create county_fips Column

# CELL ********************

oz_df = (oz_df
    .withColumn('county_fips',
        concat(
            lpad(col('state_code'), 2, '0'),
            lpad(col('county_code'), 3, '0'))
        )
    .withColumnRenamed('year', 'data_year')
)
oz_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Check for NULLs

# CELL ********************

null_counts = oz_df.select([sum(col(c).isNull().cast('int')).alias(c) for c in oz_df.columns])
null_counts.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Check for Duplicate Rows

# CELL ********************

oz_df.groupBy(oz_df.columns) \
  .count() \
  .filter(col('count') > 1) \
  .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Drop Duplicate Rows

# CELL ********************

cleaned_oz = oz_df.distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Delete and Append Cleaned AQS Ozone Data for Year

# CELL ********************

spark.sql(f"""
    delete from silver.aqs_oz
    where data_year = {data_year}
""")

cleaned_oz.write.format('delta').mode('append') \
    .saveAsTable('capstone_lh.silver.aqs_oz')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from silver.aqs_oz limit 20;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Ingest PM2.5 Data for Given Year

# CELL ********************

raw_pm = spark.read.table('capstone_lh.bronze.aqs_pm25') \
    .filter(col('year') == data_year)
raw_pm.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Select Necessary Columns

# CELL ********************

pm_df = raw_pm.select(
    'state_code',
    'county_code',
    'year',
    'pollutant_standard',
    'observation_percent',
    'validity_indicator',
    'arithmetic_mean',
    'ninety_eighth_percentile' 
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### List Pollutant Standards

# CELL ********************

pm_df.select('pollutant_standard').distinct().show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Filter Data to Valid Data Using Chosen Pollutant Standard

# CELL ********************

pm_df = pm_df.filter(
    (col('pollutant_standard') == 'PM25 Annual 2012') &
    (col('validity_indicator') == 'Y')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Generate county_fips Column

# CELL ********************

pm_df = (pm_df
    .withColumn('county_fips',
        concat(
            lpad(col('state_code'), 2, '0'),
            lpad(col('county_code'), 3, '0'))
        )
    .withColumnRenamed('year', 'data_year')
)
pm_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Check for NULLs

# CELL ********************

null_counts = pm_df.select([sum(col(c).isNull().cast('int')).alias(c) for c in pm_df.columns])
null_counts.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Check for Duplicate Rows

# CELL ********************

pm_df.groupBy(pm_df.columns) \
  .count() \
  .filter(col('count') > 1) \
  .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Drop Duplicate Rows

# CELL ********************

pm_cleaned = pm_df.distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Delete and Append PM2.5 Pollutant Data for Year

# CELL ********************

spark.sql(f"""
    delete from silver.aqs_pm25
    where data_year = {data_year}
""")

pm_cleaned.write.format('delta').mode('append') \
    .saveAsTable('capstone_lh.silver.aqs_pm25')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from silver.aqs_pm25 limit 20;

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
