# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Introduction to Workflows
# MAGIC 이 데모 노트북에서는 Workflow의 User Interface를 사용해서 예제 테스크 오케스트레이션 워크플로우를 만드는지 실습해 보겠습니다. 
# MAGIC 
# MAGIC **Learning Objectives**
# MAGIC 
# MAGIC 이 노트북을 통해서 우리는 다음 작업들을 수행해 보겠습니다:
# MAGIC * notebook 테스크들을 연겷하여 Job Workflow를 만들기
# MAGIC * 테스크 파라미터를 적용하고 테스크간 연관관계 만들기 
# MAGIC * 모니터링과 디버깅 방법 익히기 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Classroom Setup
# MAGIC 
# MAGIC 다음 셋팅 셀을 수행하여서 각 유저별 *username*, *userhome*, and *database* 를 정의합니다. 

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Other Conventions
# MAGIC 
# MAGIC As you progress through this course, you will see various references to the object **`DA`**. 
# MAGIC 
# MAGIC This object is provided by Databricks Academy and is part of the curriculum and not part of a Spark or Databricsk API.
# MAGIC 
# MAGIC For example, the **`DA`** object exposes useful variables such as your username and various paths to the datasets in this course as seen here bellow

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Database Name:     {DA.db_name}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"User DB Locatin:   {DA.paths.user_db}")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT "${DA.db_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create and Configure a Workflow Job
# MAGIC 
# MAGIC In this lesson, we are going to create a job componsed of six different tasks. All of the tasks are simple notebooks but please note that you can use other task types such as DLT pipeline and custom python code.
# MAGIC 
# MAGIC ### Visual diagram of the job
# MAGIC 
# MAGIC Before starting to create the job, let's look at the visual diagram of the job that we want to build. As shown in the diagram below, **Task-2**, **Task-3** and **Task-4** depend on the **Task-1**. In addition, **Task-5** depends on **Task-2** and **Task-4**. Lastly, **Task-6** depends on the **Task-5**.  
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/itw-demo-workflow.png" alt="Example Job we want to create" width="500" h-align="center"/>
# MAGIC 
# MAGIC 
# MAGIC ### Task definition details for the job
# MAGIC 
# MAGIC Run the code block below which will print a table listing configuration details for the job and the tasks. You can simple copy the job name and tasks' name while building the example workflow. *Resource* column shows the path of the notebook that we are going to use for each task.

# COMMAND ----------

DA.print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create the Job
# MAGIC 1. Click the **Workflows** button on the sidebar
# MAGIC 1. Select the **Jobs** tab.
# MAGIC 1. Click the **Create Job** button.
# MAGIC 1. In the unlabeled title field at the top of the screen (*Add a name for your job...*), set the name of the job to the value specified in the cell above.
# MAGIC 
# MAGIC ### Create the First Task
# MAGIC 1. In the field **Task name**, enter the value specified in the cell above for **Task #1**
# MAGIC 1. In the field **Type**, select the value **Notebook**
# MAGIC 1. In the field **Source**, select the value **Local**
# MAGIC 1. In the field **Path**, browse to the resource (aka notebook) specified in the cell above for **Task #1**
# MAGIC 1. Configure the **Cluster**:
# MAGIC     1. Expand the cluster selections
# MAGIC     1. In the drop-down, select **New job cluster**
# MAGIC     1. In the field **Cluster name**, keep the default name
# MAGIC     1. In the field **Policy**, select **Unrestricted**
# MAGIC     1. In the field **Cluster mode**, select **Single node**
# MAGIC     1. In the field **Databricks runtime version**, select the latest Photon LTS.
# MAGIC     1. In the field **Autopilot options** uncheck **Enable autoscaling local storage**
# MAGIC     1. In the field **Node type**, select the cloud-specific type:
# MAGIC         - AWS: **i3.xlarge**
# MAGIC         - MSA: **Standard_DS3_v2**
# MAGIC         - GCP: **n1-standard-4**
# MAGIC     1. Click **Confirm** to finalize your cluster settings.
# MAGIC 1. In the field **Depends on**, specify each dependency specified in the cell above, if any.
# MAGIC 1. Click **Create** to create the first task.
# MAGIC 
# MAGIC **Note: In order to create a new cluster, you must have the required permissions. If not, you may use an existing job cluster.**
# MAGIC 
# MAGIC ### Create Additional Tasks
# MAGIC 1. Click the "**+**" button in the bottom center of the screen to add a new task.
# MAGIC 1. Repeat the steps above for **Create the First Task** substituting the correct configuration as defined in the previous cell.
# MAGIC 1. For *Task-5*, define a parameter that will be passed to the notebook. The parameter `key` should be `year` and the `value` should be `2022`.
# MAGIC 
# MAGIC 
# MAGIC ### Run the Job Manually
# MAGIC 1. Run the job manually to test if it runs as expected.
# MAGIC 1. **The job will fail in Task-5 as there is an error in the notebook. We are going to debug the code and repair the execution.** 
# MAGIC 
# MAGIC ### Repair the Failed Run
# MAGIC 1. Click on the failed task and it will take you the notebook. You should see a red box showing the error details. Click on the "Scroll to error" to see the code block where error was raised. 
# MAGIC 1. To fix the error, you need to edit the notebook. On the right panel you will see notebook link that you can open the notebook easily. In this example, you just need to fix the `customer` table name in the SQL statement.
# MAGIC 1. Go to **Runs** tab and click on the run latest failed run listed under **Completed Runs**. 
# MAGIC 1. Click on **Repair run** button on the top right corner. 
# MAGIC 1. Note that **Repair** process run unsuccessful task and any dependent tasks. Because successful tasks and any tasks that depend on them are not re-run, this feature reduces the time and resources required to recover from unsuccessful job runs.
# MAGIC 
# MAGIC ### Configure Job Schedule
# MAGIC 1. Schedule the job run periodically. For this example, specify the period to `every hour` at `00`. Select your `timezone` from the dropdown list.
# MAGIC 
# MAGIC ### Create Alerts
# MAGIC 1. Create an email alert to be sent to your email address when a task **fails**.
# MAGIC 
# MAGIC ### Setup Access Control
# MAGIC 1. Give `all users` `can view` permission. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Need Help?
# MAGIC 
# MAGIC If you had any issues while following the steps that we demonstrated in this demo, you can run the following code blocks for creating and running the job workflow.
# MAGIC 
# MAGIC #### Create and Run the Job
# MAGIC 
# MAGIC The first code block below will create the same job that we explained in the previous section based on the Workflows UI. 
# MAGIC 
# MAGIC While you can run the second code block to start the job, you can navigate to "Workflows" page in Databricks workspace and run it manually using the UI.

# COMMAND ----------

# Create the job
DA.create_job()

# COMMAND ----------

# Run the jub
DA.start_job()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean-up Classroom
# MAGIC 
# MAGIC For the clean-up process, we are going to delete the job we created and delete the database that we used.
# MAGIC 
# MAGIC ### Delete the Job
# MAGIC 
# MAGIC **IMPORTANT: To avoid any unnecessary compute resource usage, you should delete the job we created in this demo.** 
# MAGIC 
# MAGIC To delete a job:
# MAGIC 1. Go to **Jobs** page. 
# MAGIC 1. Click the delete icon shown in the **Action** column.
# MAGIC 
# MAGIC 
# MAGIC ### Delete Database and Files
# MAGIC 
# MAGIC Run the following cell to remove lessons-specific assets created during this lesson:

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
