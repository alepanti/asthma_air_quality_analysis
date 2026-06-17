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

from pyspark.sql.functions import concat, lpad, col, sum, count, avg
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Grab Data Year

# CELL ********************

try:
    release_year = int(release_year)
    logger.info(f'Using provided release_year parameter: {release_year}')
except Exception:
    release_year = int(spark.sql("""
        select MAX(release_year) as latest
        from dbo.cdc_places_releases
    """).collect()[0]['latest'])

    logger.warning(f'No valid parameter found. Using latest release_year: {release_year}')

data_year = spark.sql(f"""
    SELECT data_year
    FROM dbo.cdc_places_releases
    WHERE release_year = {release_year}
""").collect()[0]['data_year']

logger.info(f'Cleaning {data_year} AQS Data')


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

logger.info(f"Available Pollutant Standards: {oz_df.select('pollutant_standard').distinct().show(truncate=False)}")

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

oz_df = oz_df.distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Check for Multiple Readings in Single County

# CELL ********************

oz_df.groupBy('county_fips') \
    .agg(count('*').alias('record_count')) \
    .filter('record_count > 1') \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Aggregate Readings Using Average per County

# CELL ********************

oz_agg = (
    oz_df
    .groupBy(
        'state_code'
        ,'county_code'
        ,'county_fips'
        ,'data_year'
        ,'pollutant_standard'
    )
    .agg(
        avg('arithmetic_mean').alias('arithmetic_mean'),
        avg('ninety_eighth_percentile').alias('ninety_eighth_percentile'),
        avg('observation_percent').alias('observation_percent')
    )
)

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

oz_agg.write.format('delta').mode('append') \
    .saveAsTable('capstone_lh.silver.aqs_oz')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(distinct *) as unique
# MAGIC ,count(*) as total_rows
# MAGIC from silver.aqs_oz;


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

logger.info(f'Loaded {raw_pm.count()} rows from bronze')

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


logger.info(f"Available Pollutant Standards: {pm_df.select('pollutant_standard').distinct().show(truncate=False)}")

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
pm_df.count()

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
pm_df.count()

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

pm_df = pm_df.distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Check for Multiple Readings from One County

# CELL ********************

pm_df.groupBy('county_fips') \
    .agg(count('*').alias('record_count')) \
    .filter('record_count > 1') \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pm_agg = (
    pm_df
    .groupBy(
        'state_code'
        ,'county_code'
        ,'county_fips'
        ,'data_year'
        ,'pollutant_standard'
    )
    .agg(
        avg('arithmetic_mean').alias('arithmetic_mean'),
        avg('ninety_eighth_percentile').alias('ninety_eighth_percentile'),
        avg('observation_percent').alias('observation_percent')
    )
)
logger.info(f'inserting {pm_df.count()} rows into silver.aqs_pm25')

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

pm_agg.write.format('delta').mode('append') \
    .saveAsTable('capstone_lh.silver.aqs_pm25')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(distinct *) as unique
# MAGIC ,count(*) as total_rows
# MAGIC from silver.aqs_pm25;


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
