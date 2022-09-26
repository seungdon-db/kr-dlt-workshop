# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Ingest Customers Data
# MAGIC 
# MAGIC In this notebook, we are going to simulate `customer` data ingestion and transformation. This notebook is going to **depend on Task-1** in which we already defined the customer table. In order to demonstrate task dependency of the Workflow Jobs, we are not going to create the table and try to insert data into the existing table. 

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
# MAGIC ## Insert Customer Data 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   customers
# MAGIC VALUES
# MAGIC   (1001, "Shirley Smith", "BREMEN"),
# MAGIC   (1002, "Carmen Guzman", "VIENNA"),
# MAGIC   (1003, "Diana Hentz", "COLUMBUS"),
# MAGIC   (1004, "Marco Tirado", "Otselic"),
# MAGIC   (1005, "Mark Uczen", "Roseland"),
# MAGIC   (1006, "James Linton", "City of Sonoma"),
# MAGIC   (1007, "Jeffrey Malik", "PITTSBURGH"),
# MAGIC   (1008, "Ursula Izewski", "Washington County"),
# MAGIC   (1009, "Ricardo Mendez", "Riverhead"),
# MAGIC   (1010, "Fatina Hardin", "Bedford");
# MAGIC   
# MAGIC   

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
