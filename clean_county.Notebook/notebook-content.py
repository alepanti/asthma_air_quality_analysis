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

# # Clean Gazeteer County Data
# 
# This notebook ingests bronze.gaz_county, cleanses the data, performs minor transformations on the data, and loads it into silver.gaz_county table.

# CELL ********************

from pyspark.sql.functions import col, sum, regexp_replace

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Read raw county data into dataframe

# CELL ********************

raw_df = spark.read.table('capstone_lh.bronze.gaz_county')
raw_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Drop Unnecessary Columns & Use Descriptive Column Names

# CELL ********************

cleaned_df = raw_df.drop('ANSICODE', 'AWATER', 'AWATER_SQMI')
cleaned_df = cleaned_df.withColumnsRenamed(
    {
        'GEOID': 'county_fips'
        ,'USPS': 'state_abbr'
        ,'NAME': 'county_name'
        ,'ALAND': 'land_area'
        ,'ALAND_SQMI': 'land_area_sqmi'
        ,'INTPTLAT': 'latitude'
        ,'INTPTLONG                                                                                                               ': 'longitude'
    }
)
cleaned_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cast Columns to Correct Data Types

# CELL ********************

cleaned_df = (cleaned_df
    .withColumn('land_area', col('land_area').cast('long'))
    .withColumn('land_area_sqmi', col('land_area_sqmi').cast('double'))
    .withColumn('longitude', regexp_replace(col('longitude'), '\\+', '').cast('double'))
    .withColumn('latitude', col('latitude').cast('double'))
)
cleaned_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check for NULLs

# CELL ********************

null_counts = cleaned_df.select(
    [
        sum(col(c).isNull().cast('int')).alias(c) for c in cleaned_df.columns
    ]
)
null_counts.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check for Duplicate Rows

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

# ### Load Data into Silver Table

# CELL ********************

cleaned_df.write.format('delta').mode('overwrite') \
    .saveAsTable('capstone_lh.silver.gaz_county')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from silver.gaz_county limit 20;

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
