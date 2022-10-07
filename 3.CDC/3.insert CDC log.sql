-- Databricks notebook source
-- MAGIC %python 
-- MAGIC dbutils.widgets.text('database', 'seoul', 'Target Database')

-- COMMAND ----------

SELECT * FROM ${database}.customer_bronze;

-- COMMAND ----------

desc ${database}.customer_bronze;

-- COMMAND ----------

insert into ${database}.customer_bronze values('my home seoul','seungdon.choi@databricks.com','1989d0b1-29f6-41e9-87b8-bfbf24491898','seungdon','choi','APPEND',cast(current_timestamp as string),NULL);

-- COMMAND ----------

SELECT * FROM ${database}.customer_silver WHERE firstname="seungdon"

-- COMMAND ----------

insert into ${database}.customer_bronze values('my home jeju','seungdon.choi@databricks.com','1989d0b1-29f6-41e9-87b8-bfbf24491898','seungdon','choi','UPDATE',cast(current_timestamp as string),NULL);

-- COMMAND ----------

SELECT * FROM ${database}.customer_silver WHERE firstname="seungdon"

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Delta Table ACID 

-- COMMAND ----------

CREATE TABLE ${database}.customer_silver_table AS SELECT * FROM ${database}.customer_silver;

-- COMMAND ----------

SELECT * FROM ${database}.customer_silver_table;

-- COMMAND ----------

SELECT email FROM ${database}.customer_silver_table WHERE firstname="Amy";

-- COMMAND ----------

UPDATE ${database}.customer_silver_table SET email ="korea@databricks.com" WHERE firstname="Amy";

-- COMMAND ----------

DELETE FROM ${database}.customer_silver_table WHERE firstname="Toni";

-- COMMAND ----------

MERGE INTO ${database}.customer_silver_table b
USING ${database}.customer_silver u
ON b.id=u.id
WHEN MATCHED 
  THEN UPDATE SET *
WHEN NOT MATCHED 
  THEN INSERT *

-- COMMAND ----------

SELECT email FROM ${database}.customer_silver_table WHERE firstname="Amy";

-- COMMAND ----------

DESC DETAIL ${database}.customer_silver_table

-- COMMAND ----------

-- DBTITLE 1,비결은  delta transaction log 
-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/"+dbutils.widgets.get("database")+".db/customer_silver_table"))

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/"+dbutils.widgets.get("database")+".db/customer_silver_table/_delta_log"))

-- COMMAND ----------

SELECT * FROM json.`dbfs:/user/hive/warehouse/${database}.db/customer_silver_table/_delta_log/00000000000000000002.json`

-- COMMAND ----------


