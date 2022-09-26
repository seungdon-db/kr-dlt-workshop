# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Ingest Product Data
# MAGIC 
# MAGIC In this notebook, we are going to simulate `product` data ingestion and transformation. This notebook is going to **depend on Task-1** in which we already defined the products table. In order to demonstrate task dependency of the Workflow Jobs, we are not going to create the table and try to insert data into the existing table. 

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
# MAGIC ## Insert Products Data 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   products
# MAGIC VALUES
# MAGIC   (1001, "Phone", "Electronics"),
# MAGIC   (1002, "T-shirt", "Clothes"),
# MAGIC   (1003, "Bread", "Bakery"),
# MAGIC   (1004, "Lemonade", "Beverage"),
# MAGIC   (1005, "Water", "Beverage"),
# MAGIC   (1006, "TV", "Electronics"),
# MAGIC   (1007, "Yoghurt", "Food"),
# MAGIC   (1008, "Banana", "Produce"),
# MAGIC   (1009, "Shampoo", "Personal Care"),
# MAGIC   (1010, "Headphone", "Electronics");
# MAGIC   
# MAGIC   

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
