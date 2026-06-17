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

# # Clean CDC Data from Specific Release
# 
# Ingests raw CDC data from given year. Cleans the data and loads into silver table.
# If no input, cleans latest release.

# CELL ********************

from pyspark.sql.functions import regexp_replace, col, sum
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

# #### Grab Release Year

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

logger.info(f'Cleaning {release_year} CDC Data release')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Load Raw CDC Data for Year

# CELL ********************

raw_df = spark.read.table('capstone_lh.bronze.cdc_places') \
    .filter(col('release_year') == release_year)
raw_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Drop Unnecessary Columns

# CELL ********************

cleaned_df = raw_df.select(
    'release_year'
    ,'data_year'
    ,'stateabbr'
    ,'statedesc'
    ,'countyname'
    ,'countyfips'
    ,'totalpopulation'
    ,'totalpop18plus'
    ,'casthma_crudeprev'
    ,'casthma_crude95ci'
    ,'casthma_adjprev'
    ,'casthma_adj95ci'
    ,'csmoking_crudeprev'
    ,'csmoking_crude95ci'
    ,'csmoking_adjprev'
    ,'csmoking_adj95ci'
    ,'obesity_crudeprev'
    ,'obesity_crude95ci'
    ,'obesity_adjprev'
    ,'obesity_adj95ci'
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cast to Correct Data Types

# CELL ********************

cleaned_df = (cleaned_df
    .withColumn('totalpopulation',
        regexp_replace(col('totalpopulation'), ',', '').cast('integer'))
    .withColumn('totalpop18plus',
        regexp_replace(col('totalpop18plus'), ',', '').cast('integer'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Use Descriptive Names for Columns

# CELL ********************

cleaned_df = cleaned_df.withColumnsRenamed(
    {
        'StateAbbr': 'state_abbr',
        'StateDesc': 'state_name',
        'CountyName': 'county_name',
        'CountyFIPS': 'county_fips',
        'TotalPopulation': 'total_pop',
        'TotalPop18plus': 'total_pop_18p'
    }
)

cleaned_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Check for NULL Values

# CELL ********************

null_counts = cleaned_df.select([sum(col(c).isNull().cast('int')).alias(c) for c in cleaned_df.columns])
null_counts.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Drop Rows with NULL in Asthma Measurement

# CELL ********************

cleaned_df = cleaned_df.dropna(subset=['CASTHMA_CrudePrev'])
cleaned_df.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Check for Duplicate Rows

# CELL ********************

duplicate_df = cleaned_df.groupBy(cleaned_df.columns) \
                 .count() \
                 .filter(col('count') > 1)

duplicate_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Drop and Append CDC Data for Year into Silver

# CELL ********************

spark.sql(f"""
    delete from silver.cdc_places
    where release_year = {release_year}
""")

cleaned_df.write.format('delta').mode('append') \
    .saveAsTable('capstone_lh.silver.cdc_places')

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
# MAGIC from silver.cdc_places;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(1) from silver.cdc_places where release_year = 2022;
# MAGIC select * from silver.cdc_places where release_year = 2022 limit 10;

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
