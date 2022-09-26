-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DLT pipeline log analysis
-- MAGIC 
-- MAGIC Please Make sure you specify your own Database and Storage location. You'll find this information in the configuration menu of your Delta Live Table Pipeline.
-- MAGIC 
-- MAGIC **NOTE:** Please use Databricks Runtime 9.1 or above when running this notebook

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text('storage_location', '/techsummit/dlt/storage/first_last')
-- MAGIC dbutils.widgets.text('latest_update_id', 'Update_ID_fromUpdateDetails')

-- COMMAND ----------

-- MAGIC %python display(dbutils.fs.ls(dbutils.widgets.get('storage_location')))

-- COMMAND ----------

CREATE OR REPLACE VIEW loan_pipeline_logs
AS SELECT * FROM delta.`${storage_location}/system/events`;

SELECT * FROM loan_pipeline_logs
ORDER BY timestamp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Event Logs Analysis
-- MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
-- MAGIC 
-- MAGIC | Type of event | behavior |
-- MAGIC | --- | --- |
-- MAGIC | `user_action` | Events occur when taking actions like creating the pipeline |
-- MAGIC | `flow_definition`| Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information |
-- MAGIC | `output_dataset` and `input_datasets` | output table/view and its upstream table(s)/view(s) |
-- MAGIC | `flow_type` | whether this is a complete or append flow |
-- MAGIC | `explain_text` | the Spark explain plan |
-- MAGIC | `flow_progress`| Events occur when a data flow starts running or finishes processing a batch of data |
-- MAGIC | `metrics` | currently contains `num_output_rows` |
-- MAGIC | `data_quality` (`dropped_records`), (`expectations`: `name`, `dataset`, `passed_records`, `failed_records`)| contains an array of the results of the data quality rules for this particular dataset   * `expectations`|

-- COMMAND ----------

-- DBTITLE 1,Lineage Information
-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   details:flow_definition.output_dataset,
-- MAGIC   details:flow_definition.input_datasets,
-- MAGIC   details:flow_definition.flow_type,
-- MAGIC   details:flow_definition.schema,
-- MAGIC   details:flow_definition
-- MAGIC FROM loan_pipeline_logs
-- MAGIC WHERE details:flow_definition IS NOT NULL
-- MAGIC ORDER BY timestamp

-- COMMAND ----------

-- DBTITLE 1,Data Quality Metrics
SELECT
  id,
  timestamp,
  status_update,
  expectations.dataset,
  expectations.name,
  expectations.failed_records,
  expectations.passed_records
FROM(
  SELECT 
    id,
    timestamp,
    details:flow_progress.metrics.num_output_rows as output_records,
    details:flow_progress.data_quality.dropped_records,
    details:flow_progress.status as status_update,
    explode(from_json(details:flow_progress:data_quality:expectations
             , schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records': 42, 'failed_records': 42}]"))) expectations
  FROM loan_pipeline_logs
  WHERE details:flow_progress.metrics IS NOT NULL) data_quality

-- COMMAND ----------

-- DBTITLE 1,Runtime information of the Latest Pipeline Update
SELECT details:create_update:runtime_version:dbr_version FROM loan_pipeline_logs WHERE event_type = 'create_update' limit 1;

-- COMMAND ----------

-- DBTITLE 1,Cluster performance metrics
SELECT
  timestamp,
  Double(details :cluster_utilization.num_executors) as current_num_executors,
  Double(details :cluster_utilization.avg_num_task_slots) as avg_num_task_slots,
  Double(
    details :cluster_utilization.avg_task_slot_utilization
  ) as avg_task_slot_utilization,
  Double(
    details :cluster_utilization.avg_num_queued_tasks
  ) as queue_size,
  Double(details :flow_progress.metrics.backlog_bytes) as backlog
FROM
  loan_pipeline_logs
WHERE
  event_type IN ('cluster_utilization', 'flow_progress')
  AND origin.update_id = '${latest_update_id}'; 

-- COMMAND ----------

-- DBTITLE 1,DLT Enhanced Autoscaling for Streaming Workloads
SELECT
  timestamp,
  Double(
    case
      when details :autoscale.status = 'REQUESTED' then details :autoscale.desired_num_workers
      else null
    end
  ) as requested_workers,
  Double(
    case
      when details :autoscale.status = 'ACCEPTED' then details :autoscale.desired_num_workers
      else null
    end
  ) as accepted_workers,
  Double(
    case
      when details :autoscale.status = 'SUCCEEDED' then details :autoscale.desired_num_workers
      else null
    end
  ) as succeeded_workers,
  Double(
    case
      when details :autoscale.status = 'REJECTED' then details :autoscale.desired_num_workers
      else null
    end
  ) as rejected_workers
FROM
 loan_pipeline_logs
WHERE
  event_type = 'autoscale'
  AND origin.update_id = '${latest_update_id}';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DLT Logs FAQ
-- MAGIC 
-- MAGIC 1. Besides the quality control metrics, are actual dropped and failed records accessible in logs?
-- MAGIC 2. How to set up alerts based on certain observation in logs?
-- MAGIC 3. Is it possible to send cluster logs of DLT to external storage?
-- MAGIC 4. Can a user use DLT logs to find information about user actions?
