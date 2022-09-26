# Databricks notebook source
# MAGIC %md
# MAGIC # Wikipedia click stream 
# MAGIC 원본 json파일부터 Delta Live Tables 로 프로세싱된 테이블들에 대해서 확인해 봅시다

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from json.`/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json`

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from seungdon_wiki_demo.clickstream_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM seungdon_wiki_demo.top_pages;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM seungdon_wiki_demo.top_spark_referers;

# COMMAND ----------

# MAGIC %fs ls /FileStore/seungdon/wiki_demo

# COMMAND ----------


