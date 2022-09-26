# Databricks notebook source
# MAGIC %run ./_utility-functions

# COMMAND ----------

# Define only so that we can reference known variables, 
# not actually invoking anything other functions.
DA = DBAcademyHelper()

# Remove all databases associated with this course
rows = spark.sql("SHOW DATABASES").collect()
for row in rows:
    db_name = row[0]
    if db_name.startswith(DA.db_name_prefix):
        print(f"Dropping database {db_name}")
        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")

# Remove all assets from DBFS associated with this course
if Paths.exists(DA.hidden.working_dir_root):
    result = dbutils.fs.rm(DA.hidden.working_dir_root, True)
    print(f"Deleted {DA.hidden.working_dir_root}: {result}")

print("Course environment succesfully reset.")
