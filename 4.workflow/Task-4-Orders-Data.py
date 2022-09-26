# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Ingest Orders Data
# MAGIC 
# MAGIC In this notebook, we are going to simulate `orders` data ingestion and transformation. This notebook is going to **depend on Task-1** in which we already defined the orders table. In order to demonstrate the task dependency of the Workflow Jobs, we are not going to create the table and try to insert data into the existing table. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setup database

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE `${DA.db_name}`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert Order Data 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   orders
# MAGIC VALUES
# MAGIC   (1001, 800, 1001, 1004, DATE '2022-02-15'),
# MAGIC   (1002, 24, 1005, 1003, DATE '2022-04-01'),
# MAGIC   (1003, 1200, 1010, 1006, DATE '2021-09-13'),
# MAGIC   (1004, 15, 1001, 1007, DATE '2022-01-26'),
# MAGIC   (1005, 30, 1007, 1002, DATE '2021-11-17'),
# MAGIC   (1006, 18, 1003, 1009, DATE '2022-05-14'),
# MAGIC   (1007, 75, 1006, 1010, DATE '2021-10-29'),
# MAGIC   (1008, 8, 1006, 1008, DATE '2022-03-21'),
# MAGIC   (1009, 5, 1006, 1005, DATE '2022-02-05'),
# MAGIC   (1010, 546, 1002, 1001, DATE '2022-04-02');
# MAGIC   
# MAGIC   

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
