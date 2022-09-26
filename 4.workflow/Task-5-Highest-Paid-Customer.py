# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Highest Paid Customer
# MAGIC 
# MAGIC In this notebook, we are going to merge `customers` and `orders` data to find the customer who spent the most in 2022. As this task depends on the previous tasks, we need to define depedency for the task. This task is going to depend on **Task-2** and **Task-4**. 

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
# MAGIC ## Customer with Highest Amount of Purchase 
# MAGIC 
# MAGIC ### Create Year Parameter
# MAGIC 
# MAGIC To demonstrate Workflow's parameter capability we are going to create a parameter for this notebook and pass the parameter value in the task definition. Let's create a `year` parameter and filter the `orders` based on this parameter. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET DROPDOWN year DEFAULT "2021" CHOICES SELECT DISTINCT date_format(date, 'y') FROM orders

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Aggregate Data
# MAGIC 
# MAGIC In order to find the customer who had highest total purchase, we need to join customer data and orders data. After joining both tables, the customer name, city and total purchase amount of the customer with highest total purchase is returned.
# MAGIC 
# MAGIC **Note:** The table in `line 5` is misspelled intentionally. Obviously, this is going to cause the task failure while executing the workflow. We are going to demonstrate how to use **Repair** job feature of Workflows after the run fails.   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   name, city, total_purchase
# MAGIC FROM
# MAGIC   customers AS c
# MAGIC   JOIN (
# MAGIC     SELECT
# MAGIC       customer_id,
# MAGIC       sum(amount) AS total_purchase
# MAGIC     FROM
# MAGIC       orders
# MAGIC     WHERE date > getArgument('year')
# MAGIC     GROUP BY
# MAGIC       customer_id
# MAGIC   ) AS p ON c.id = p.customer_id
# MAGIC ORDER BY
# MAGIC   total_purchase DESC
# MAGIC LIMIT
# MAGIC   1

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
