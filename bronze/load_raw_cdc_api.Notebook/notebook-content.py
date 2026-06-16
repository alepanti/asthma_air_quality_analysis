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

try:
    release_year = int(release_year)
    logger.info(f'Using provided release_year parameter: {release_year}')
except Exception:
    release_year = int(spark.sql("""
        select MAX(release_year) as latest
        from dbo.cdc_places_releases
    """).collect()[0]['latest'])

    logger.warning(f'No valid parameter found. Using latest release_year: {release_year}')

dataset_id = spark.sql(f"""
    select dataset_id
    from dbo.cdc_places_releases
    where release_year = {release_year}
""").collect()[0]['dataset_id']

data_year = spark.sql(f"""
    select data_year
    from dbo.cdc_places_releases
    where release_year = {release_year}
""").collect()[0]['data_year']

logger.info(f'Loading {release_year} CDC release, ID: {dataset_id}, data year {data_year}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_url = f'https://data.cdc.gov/resource/{dataset_id}.json'
limit = 1000
offset = 0
cdc_data = []

while True:
    params = {
        '$limit' : limit,
        '$offset' : offset
    }
    reqs = requests.get(base_url, params=params)
    batch = reqs.json()

    if len(batch) == 0:
        break
    
    cdc_data.extend(batch)

    offset += limit
    logger.info(f'loaded {len(cdc_data)}')

logger.info(f'final row count = {len(cdc_data)}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = pd.DataFrame(cdc_data)
cdc_df = spark.createDataFrame(df)
cdc_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create schema if not exists bronze;
# MAGIC create table if not exists capstone_lh.bronze.cdc_places (
# MAGIC     release_year int 
# MAGIC     ,data_year int
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger.info('Truncate and insert capstone_lh.bronze.cdc_places')


spark.sql(f"""
    delete from capstone_lh.bronze.cdc_places
    where release_year = {release_year}
""")

cdc_df = cdc_df.withColumns(
    {
        'release_year': lit(release_year)
        ,'data_year' : lit(data_year)
    }
)

cdc_df.write.format('delta').mode('append').option('mergeSchema', 'True') \
    .saveAsTable('capstone_lh.bronze.cdc_places')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from bronze.cdc_places limit 10;

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
