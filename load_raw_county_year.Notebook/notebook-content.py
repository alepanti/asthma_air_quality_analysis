# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ── PARAMETERS ────────────────────────────────────────
try:
    file_name = mssparkutils.widgets.get("file_name")
except:
    file_name = "2025_Gaz_counties_national.txt"

print(f"Loading gazetteer file: {file_name}")



# ── VALIDATE FILE LOADED ───────────────────────────────


# ── WRITE TO BRONZE ────────────────────────────────────
# GAZ doesn't have a data_year so we don't do incremental
# Just overwrite — county boundaries don't change meaningfully year to year
df.write.format("delta").mode("overwrite") \
    .saveAsTable("capstone_lh.bro.raw_gaz_county")

print("Written to bronze.raw_gaz_county")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
