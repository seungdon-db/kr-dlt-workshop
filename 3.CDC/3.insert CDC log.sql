-- Databricks notebook source
-- MAGIC %python 
-- MAGIC dbutils.widgets.text('database', 'seungdon', 'Target Database')

-- COMMAND ----------

SELECT * FROM ${database}.customer_bronze;

-- COMMAND ----------

desc ${database}.customer_bronze;

-- COMMAND ----------

insert into ${database}.customer_bronze values('my home seoul','seungdon.choi@databricks.com','1989d0b1-29f6-41e9-87b8-bfbf24491898','seungdon','choi','APPEND',cast(current_timestamp as string),NULL);

-- COMMAND ----------

SELECT * FROM ${database}.customer_silver WHERE firstname="seungdon"

-- COMMAND ----------

insert into seungdon.customer_bronze values('my home jeju','seungdon.choi@databricks.com','1989d0b1-29f6-41e9-87b8-bfbf24491898','seungdon','choi','UPDATE',cast(current_timestamp as string),NULL);

-- COMMAND ----------

SELECT * FROM ${database}.customer_silver WHERE firstname="seungdon"

-- COMMAND ----------


