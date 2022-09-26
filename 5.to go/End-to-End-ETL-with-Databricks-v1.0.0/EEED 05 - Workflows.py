# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Orchestrating Jobs with Databricks
# MAGIC 
# MAGIC In this lab, you'll be configuring a workflow comprising of:
# MAGIC * A notebook that lands a new batch of data in a storage directory
# MAGIC * A Delta Live Table pipeline that processes this data through a series of tables
# MAGIC * A notebook that queries and updates various metrics output by DLT
# MAGIC 
# MAGIC By the end of this lab, you should feel confident:
# MAGIC * Scheduling a notebook as a Databricks Job
# MAGIC * Scheduling a DLT pipeline as a Databricks Task
# MAGIC * Configuring linear dependencies between tasks using the Databricks Jobs UI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC 
# MAGIC Run the following cell to configure your working environment for this course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup $mode="reset"

# COMMAND ----------

# MAGIC %md ## Generate Job Configuration
# MAGIC The configuration of your job includes parameters unique to a given user.
# MAGIC 
# MAGIC You will need to specify which language to use by uncommenting the appropriate line.
# MAGIC 
# MAGIC Run the following cell to print out the values used to configure your job in subsequent steps.

# COMMAND ----------

# pipeline_language = "SQL"
# pipeline_language = "Python"

DA.print_job_config(pipeline_language)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC When using the Jobs UI to orchestrate a workload with multiple tasks, you'll always begin by scheduling the first task.
# MAGIC 
# MAGIC Here, we'll start by scheduling the notebook batch job.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Workflows** button in the sidebar
# MAGIC 1. Select the **Jobs** tab (should be the default)
# MAGIC 1. Click the **Create Job** button.
# MAGIC 1. In the unlabled title field at the top of the screen (*Add a name for your job...*), set the name of the job to the value specified in the cell above.
# MAGIC     * This is an important step so that we can distinguish your job from all the other jobs being configured.
# MAGIC 1. In the field **Task name**, enter the task name specified above for **Task #1**
# MAGIC 1. In the **Type** field, select the value "**Notebook**" (should be the default)
# MAGIC 1. In the **Source** field, select the value "**Local**" (should be the default)
# MAGIC 1. Click on the **Path** field and then navigate to the notebook specified above in the **Resource** column for **Task #1**
# MAGIC 1. Configure the **Cluster**:
# MAGIC     1. Expand the cluster selections
# MAGIC     1. In the drop-down, select **New job cluster**
# MAGIC     1. In the field **Cluster name**, keep the default name
# MAGIC     1. In the field **Policy**, select **Unrestricted**
# MAGIC     1. In the field **Cluster mode**, select **Single node**
# MAGIC     1. In the field **Databricks runtime version** select the latest Photon LTS.
# MAGIC     1. Optionally check **Use Photon Acceleration**.
# MAGIC     1. In the field **Autopilot options** uncheck **Enable autoscaling local storage**
# MAGIC     1. In the field **Node type**, select the cloud-specific type:
# MAGIC         - AWS: **i3.xlarge**
# MAGIC         - MSA: **Standard_DS3_v2**
# MAGIC         - GCP: **n1-standard-4**
# MAGIC     1. Click **Confirm** to finalize your cluster settings.
# MAGIC 1. Click **Create** to create the first task.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule a DLT Pipeline as a Task
# MAGIC 
# MAGIC In this step, we'll schedule our DLT pipeline to execute after the success of the task we configured at the start of this lesson.
# MAGIC    
# MAGIC Steps:
# MAGIC 1. Click the "**+**" button in the bottom center of the screen to add a new task.
# MAGIC 1. In the field **Task name**, enter the task name specified above for **Task #2**
# MAGIC 1. In the **Type** field, select the value "**Delta Live Tables pipeline**"
# MAGIC 1. In the **Pipeline** field, select the pipline you created earlier, also specified above in the **Resource** column for **Task #2**
# MAGIC 1. In the field **Depends on**, select each dependency specified in the cell above for **Task #2**.
# MAGIC 1. Click **Create** to create the second task.
# MAGIC    
# MAGIC You should now see a screen with 2 boxes and a downward arrow between them. 
# MAGIC 
# MAGIC Your **Land-Data** task will be at the top, leading into your **DLT-Pipeline** task.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule an Additional Notebook Job
# MAGIC 
# MAGIC An additional notebook has been provided which queries some of the DLT metrics. 
# MAGIC 
# MAGIC We'll add this as a final task in our job.
# MAGIC 
# MAGIC Other than different configuration parameters, this step will be nearly identical to **Task #1** above.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the "**+**" button in the bottom center of the screen to add a new task.
# MAGIC 1. In the field **Task name**, enter the task name specified above for **Task #3**
# MAGIC 1. In the **Type** field, select the value "**Notebook**" (should be the default)
# MAGIC 1. In the **Source** field, select the value "**Local**" (should be the default)
# MAGIC 1. Click on the **Path** field and then navigate to the notebook specified above in the **Resource** column for **Task #3**
# MAGIC 1. Configure the **Cluster** - in this case the default value should be fine as it should attempt to resuce the cluster defined for **Task #1** above ("**Land-Data_cluster**").
# MAGIC 1. In the field **Depends on**, select each dependency specified in the cell above for **Task #3**.
# MAGIC 1. Click **Create** to create the third and final task.
# MAGIC 
# MAGIC You should now see a screen with 3 boxes and a downward arrow between them. 
# MAGIC 
# MAGIC Your **Land-Data** task will be at the top, leading into your **DLT-Pipeline** task which in turn leads into your **Event-Logs** task.

# COMMAND ----------

# MAGIC %md ## Run & Review the Job
# MAGIC 
# MAGIC 1. With all three tasks created, click the **Run now** button in the top right of the screen to run this job.
# MAGIC 1. Select the **Runs** tab to view the list of all active and previous runs.
# MAGIC 1. Click on the start time for this run under the **Active runs** section to visually track task progress.
# MAGIC 
# MAGIC Once all your tasks have succeeded, review the contents of each task to confirm expected behavior.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
