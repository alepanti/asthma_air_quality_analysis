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

pip install pyaqsapi

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyaqsapi as aqs
from datetime import date
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

aqs.aqs_credentials(username='apanti2@wgu.edu', key='goldgazelle15')

try:
    release_year = int(mssparkutils.widgets.get('release_year'))
except:
    release_year = int(spark.sql("""
        select max(release_year) as latest
        from dbo.cdc_places_releases
    """).collect()[0]['latest'])

data_year = spark.sql(f"""
    SELECT data_year
    FROM dbo.cdc_places_releases
    WHERE release_year = {release_year}
""").collect()[0]['data_year'] 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

states = aqs.aqs_states()
state_code =  states['code'].tolist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pm25_df = pd.DataFrame()

for code in state_code:
    if int(code) > 56: # only load US
        break
    print('loading state' , code)
    state_data = aqs.bystate.annualsummary(parameter='88101',
                          bdate=date(year=data_year, month=1, day=1),
                          edate=date(year=data_year, month=12, day=31),
                          stateFIPS=code)
    pm25_df = pd.concat([pm25_df, state_data], ignore_index=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pm25_sp_df = spark.createDataFrame(pm25_df)

pm25_sp_df.write.format('delta').mode('append').option('mergeSchema', 'True') \
    .saveAsTable('capstone_lh.bronze.aqs_pm25')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

oz_df = pd.DataFrame()

for code in fips:
    if int(code) > 56: # only load US
        break
    print('loading state' , code)
    state_data = aqs.bystate.annualsummary(parameter='44201',
                          bdate=date(year=data_year, month=1, day=1),
                          edate=date(year=data_year, month=12, day=31),
                          stateFIPS=code)
    oz_df = pd.concat([oz_df, state_data], ignore_index=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

oz_sp_df = spark.createDataFrame(oz_df)

oz_sp_df.write.format('delta').mode('append').option('mergeSchema', 'True') \
    .saveAsTable('capstone_lh.bronze.aqs_oz')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
