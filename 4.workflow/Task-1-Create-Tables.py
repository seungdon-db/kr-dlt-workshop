# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Create Tables
# MAGIC 
# MAGIC Our first task is going to be a simple data ingestion task. In a real project, we would ingest data from an external data source using a service such as AutoLoader or DLT. As in this course our goal is to introduce you to the Workflows, we will keep tasks as simple as possible. 
# MAGIC 
# MAGIC **Task-2, Task-3, and Task-4 are going to depend on this task.** This means if this fails, the rest of the job will not be executed. 
# MAGIC 
# MAGIC This notebook is going to be our first task in the Workflow Jobs. As this will be a simple demonstration notebook, you do not need to make any changes.
# MAGIC 
# MAGIC In this notebook we will create three sample tables; 
# MAGIC * Customers
# MAGIC * Products
# MAGIC * Orders

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
# MAGIC ## Create Customers Table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE customers (id INT, name STRING, city STRING);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Products Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE products (id INT, name STRING, category STRING);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Orders Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders (id INT, amount INT, customer_id INT, product_id INT, date DATE);

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
