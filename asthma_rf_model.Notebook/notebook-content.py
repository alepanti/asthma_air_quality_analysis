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

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_df = spark.read.table('capstone_lh.gold.ml_county_health')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_df = data_df.drop('county_fips', 'pm25_98p', 'ozone_98p')
input_col = [
    'smoke_crude_prev'
    ,'obesity_crude_prev'
    ,'ozone_mean'
    ,'pm25_mean'
    ,'median_income'
    ,'poverty_rate'
    ,'pct_white'
    ,'pct_black_aa'
    ,'pct_hisp_lat'
    ,'pop_density'
]

target = 'asthma_crude_prev'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assembler = VectorAssembler(inputCols=input_col, outputCol='features')

data_df = assembler.transform(data_df)
data = data_df.select('features', target)

train_data, test_data = data.randomSplit([0.7, 0.3], seed=42)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rf = RandomForestRegressor(featuresCol='features', labelCol=target, predictionCol=f'pred_{target}', numTrees=100)
rf_model = rf.fit(train_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

predictions = rf_model.transform(test_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eval_rmse = RegressionEvaluator(labelCol=target, predictionCol=f'pred_{target}', metricName='rmse')
rmse = eval_rmse.evaluate(predictions)

print(f'RMSE on test data: {rmse}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eval_r2 = RegressionEvaluator(labelCol=target, predictionCol=f'pred_{target}', metricName='r2')
r2 = eval_r2.evaluate(predictions)

print(f'R2 on test data: {r2}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eval_mae = RegressionEvaluator(labelCol=target, predictionCol=f'pred_{target}', metricName='mae')
mae = eval_mae.evaluate(predictions)

print(f'MAE on test data: {mae}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_importances = rf_model.featureImportances

importance_data = []
for index, feature_name in enumerate(input_col):
    importance_score = float(raw_importances[index])
    importance_data.append((feature_name, importance_score))

importance_df = pd.DataFrame(importance_data, columns=['Feature', 'Importance'])
importance_df = importance_df.sort_values(by='Importance', ascending=False).reset_index(drop=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.barh(importance_df['Feature'], importance_df['Importance'])
plt.xlabel('Importance')
plt.ylabel('Feature Name')
plt.title('Feature Importance')
plt.tight_layout()
plt.savefig(f'/lakehouse/default/Files/figures/rf_importance.png')
plt.show()

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
