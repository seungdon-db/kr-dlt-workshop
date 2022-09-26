# Databricks notebook source
# MAGIC %run ./_utility-functions

# COMMAND ----------

# MAGIC %run ./_multi-task-jobs-config

# COMMAND ----------

DA = DBAcademyHelper(asynchronous=False)      # Create the DA object with the specified lesson
#DA.cleanup(validate=False)  # Remove the existing database and files
DA.init(create_db=True)     # True is the default
#DA.install_datasets()       # Install (if necissary) and validate the datasets
DA.conclude_setup()         # Conclude the setup by printing the DA object's final state
