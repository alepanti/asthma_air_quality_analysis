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

import requests
import pandas as pd
from pyspark.sql.functions import lit
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

logger.info(f'Loading Census PLACES {data_year}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

key = '598c3d77a4c4c7c77afc0c234199a83e8499f7ed'
base_url = f'https://api.census.gov/data/{data_year}/acs/acs5/profile'

#print(base_url, data_year)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dp03_params = {
    'get': 'NAME,DP03_0062E,DP03_0119PE',
    'for': 'county:*',
    'in': 'state:*',
    'key': key
}

reqs03 = requests.get(base_url, params=dp03_params)
dp03_data = reqs03.json()
dp03_df = pd.DataFrame(dp03_data[1:], columns=dp03_data[0])

dp03_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dp05_params = {
    'get': 'NAME,DP05_0001E,DP05_0037PE,DP05_0038PE,DP05_0076PE',
    'for': 'county:*',
    'in': 'state:*',
    'key': key
}

reqs05 = requests.get(base_url, params=dp05_params)
dp05_data = reqs05.json()
dp05_df = pd.DataFrame(dp05_data[1:], columns=dp05_data[0])

dp05_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dp03_df['CountyFIPS'] = dp03_df['state'] + dp03_df['county']
dp05_df['CountyFIPS'] = dp05_df['state'] + dp05_df['county']

dp03_df = dp03_df.drop(columns=['NAME', 'state', 'county'])
dp05_df = dp05_df.drop(columns=['state', 'county'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged = dp03_df.merge(dp05_df, on='CountyFIPS')
census = spark.createDataFrame(merged)
census = census.withColumn('data_year', lit(data_year))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC create table if not exists bronze.census (
# MAGIC     data_year int
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger.info('Truncate and insert capstone_lh.bronze.census')
spark.sql(f"""
    delete from bronze.census
    where data_year = {data_year}
""")

census.write.format('delta').mode('append').option('mergeSchema', 'True') \
    .saveAsTable('capstone_lh.bronze.census')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from bronze.census limit 20;

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
