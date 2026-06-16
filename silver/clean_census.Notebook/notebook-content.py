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

# # Clean Census Data for Year
# 
# Cleans ACS Census data from the year loaded. 

# CELL ********************

from pyspark.sql.functions import split, col, sum
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

# #### Grab Year to Clean from Inputs

# CELL ********************

release_year = 2025

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

logger.info(f'Cleaning {data_year} Census Data')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Load Raw Census Data from Year

# CELL ********************

raw_df = spark.read.table('capstone_lh.bronze.census') \
    .filter(col('data_year') == data_year)
raw_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Break 'NAME' Column into County and State Name, Drop Unnecessary Columns

# CELL ********************

county_state = split(col('NAME'), ',')

cleaned_df = raw_df.withColumn('county_name', county_state.getItem(0)) \
       .withColumn('state_name', county_state.getItem(1))
cleaned_df = cleaned_df.drop('NAME', 'DP05_0001E')
cleaned_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Rename Columns to Be more Descriptive

# CELL ********************

cleaned_df = cleaned_df.withColumnsRenamed(
    {
        'CountyFIPS': 'county_fips',
        'DP03_0062E': 'med_house_income',
        'DP03_0119PE': 'percent_below_pov',
        'DP05_0037PE': 'percent_white',
        'DP05_0038PE': 'percent_black_aa',
        'DP05_0076PE': 'percent_hisp_lat'
    }
)
cleaned_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cast Columns to Correct Data Types

# CELL ********************

cols_to_cast = ['percent_below_pov', 'percent_white', 'percent_black_aa', 'percent_hisp_lat']

cleaned_df = cleaned_df.select(
    *[col(c) for c in cleaned_df.columns if c not in cols_to_cast],
    *[col(c).cast('double').alias(c) for c in cols_to_cast]
)

cleaned_df = cleaned_df.withColumn('med_house_income', col('med_house_income').cast('int'))
cleaned_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Check for NULL values

# CELL ********************

null_counts = cleaned_df.select([sum(col(c).isNull().cast('int')).alias(c) for c in cleaned_df.columns])
null_counts.show()

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

# ####

# MARKDOWN ********************

# #### Drop Year Data from Silver table, Append Cleaned Data

# CELL ********************

spark.sql(f"""
    delete from silver.census
    where data_year = {data_year}
""")

cleaned_df.write.format('delta').mode('append') \
    .saveAsTable('capstone_lh.silver.census')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from silver.census limit 20;

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
