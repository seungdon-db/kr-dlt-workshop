# Databricks notebook source
class TaskConfig():
    def __init__(self, name, resource_type, resource, pipeline_id=None, depends_on=[], cluster="shared_cluster"):
        self.name = name
        self.resource = resource
        self.pipeline_id = pipeline_id
        self.resource_type = resource_type
        self.depends_on = depends_on
        self.cluster = cluster

class JobConfig():
    def __init__(self, job_name, tasks):
        self.job_name = job_name
        self.tasks = tasks

# COMMAND ----------

def get_job_config(self):
    """
    Returns the configuration to be used by the student in configuring the job.
    """
    base_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    base_path = "/".join(base_path.split("/")[:-1])
    
    da_name, da_hash = DA.get_username_hash()
    job_name = f"da-{da_name}-{da_hash}-{self.course_code.lower()}: Demo Job"
    
    return JobConfig(job_name, [
        TaskConfig(name="Task-1-Create-Tables",
                   resource_type="Notebook",
                   resource=f"{base_path}/Task-1-Create-Tables"),
                   #resource=f"{base_path}/<notebook_path>"),
        
        TaskConfig(name="Task-2-Customers-Data",
                   resource_type="Notebook",
                   depends_on = ["Task-1-Create-Tables"],
                   resource=f"{base_path}/Task-2-Customers-Data"),
        
        TaskConfig(name="Task-3-Products-Data",
                   resource_type="Notebook",
                   depends_on = ["Task-1-Create-Tables"],
                   resource=f"{base_path}/Task-3-Products-Data"),
        
        TaskConfig(name="Task-4-Orders-Data",
                   resource_type="Notebook",
                   depends_on = ["Task-1-Create-Tables"],
                   resource=f"{base_path}/Task-4-Orders-Data"),
        
        TaskConfig(name="Task-5-Highest-Paid-Customer",
                   resource_type="Notebook",
                   depends_on = ["Task-2-Customers-Data", "Task-4-Orders-Data"],
                   resource=f"{base_path}/Task-5-Highest-Paid-Customer"),
        
        TaskConfig(name="Task-6-Final-Step",
                   resource_type="Notebook",
                   depends_on = ["Task-5-Highest-Paid-Customer"],
                   resource=f"{base_path}/Task-6-Final-Step"),
    ])
    
DBAcademyHelper.monkey_patch(get_job_config)

# COMMAND ----------

def print_job_config(self):
    """
    Renders the configuration of the job as HTML
    """
    config = self.get_job_config()
    
    border_color = "1px solid rgba(0, 0, 0, 0.25)"
    td_style = f"white-space:nowrap; padding: 8px; border: 0; border-left: {border_color}; border-top: {border_color}"
    
    html = f"""  
    <p style="font-size: 16px">Job Name: <span style="font-weight:bold">{config.job_name}</span></p>
    
    <table style="width:100%; border-collapse: separate; border-spacing: 0; border-right: {border_color}; border-bottom: {border_color}; color: background-color: rgba(0, 0, 0, 0.8)">
        <tr>
            <td style="{td_style}; background-color: rgba(245,245,245,1); width:1em">Description</td>
            <td style="{td_style}; background-color: rgba(245,245,245,1); width:8em">Task Name</td>
            <td style="{td_style}; background-color: rgba(245,245,245,1); width:11em">Task Type</td>
            <td style="{td_style}; background-color: rgba(245,245,245,1)">Resource</td>
            <td style="{td_style}; background-color: rgba(245,245,245,1)">Depends On</td>
        </tr>
    """
    for i, task in enumerate(config.tasks):
        html += f"""
            <tr>
                <td style="{td_style}">Task #{i+1}:</td>
                <td style="{td_style}"><input type="text" value="{task.name}" style="width:100%; font-weight: bold"></td>
                <td style="{td_style}; font-weight: bold">{task.resource_type}</td>
                <td style="{td_style}; font-weight: bold">{task.resource}</td>
                <td style="{td_style}; font-weight: bold">{", ".join(task.depends_on)}</td>
            </tr>"""
        
    html += "\n</table>"
    displayHTML(html)

DBAcademyHelper.monkey_patch(print_job_config)

# COMMAND ----------

def create_job(self):
    """
    Creates the prescribed job.
    """
    import re, json
    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    config = self.get_job_config()
    print(f"Creating the job {config.job_name}")

    # Delete the existing pipeline if it exists
    client.jobs().delete_by_name(config.job_name, success_only=False)

    course_name = re.sub("[^a-zA-Z0-9]", "-", DA.course_name)
    while "--" in course_name: course_name = course_name.replace("--", "-")
    
    params = {
        "name": f"{config.job_name}",
        "tags": {
            "dbacademy.course": course_name,
            "dbacademy.source": course_name
        },
        "email_notifications": {},
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "tasks": [],
        "job_clusters": [{
            "job_cluster_key": "shared_cluster",
            "new_cluster": {
                "num_workers": 0,
                "spark_version": f"{client.clusters().get_current_spark_version()}",
                "spark_conf": { "spark.master": "local[*]" },
            },
        }]
    }
    
    for task in config.tasks:
        task_def = {
            "task_key": task.name,
        }
        params.get("tasks").append(task_def)
        if task.cluster is not None: task_def["job_cluster_key"] = task.cluster
        
        if task.pipeline_id is not None: task_def["pipeline_task"] = {"pipeline_id": task.pipeline_id}
        else: task_def["notebook_task"] = {"notebook_path": task.resource}
            
        if len(task.depends_on) > 0:
            task_def["depends_on"] = list()
            for key in task.depends_on: task_def["depends_on"].append({"task_key":key})
    
    instance_pool_id = client.clusters().get_current_instance_pool_id()
    cluster = params.get("job_clusters")[0].get("new_cluster")
    if instance_pool_id:
        cluster["instance_pool_id"] = instance_pool_id
    else:
        node_type_id = client.clusters().get_current_node_type_id()
        cluster["node_type_id"] = node_type_id
        
    # print(json.dumps(params, indent=4))
    
    json_response = client.jobs().create(params)
    self.job_id = json_response["job_id"]
    print(f"Created job {self.job_id}")

DBAcademyHelper.monkey_patch(create_job)

# COMMAND ----------

def start_job(self):
    "Starts the job and then blocks until it is TERMINATED or INTERNAL_ERROR"

    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    run_id = client.jobs().run_now(self.job_id).get("run_id")
    response = client.runs().wait_for(run_id)
    
    state = response.get("state").get("life_cycle_state")
    assert state in ["TERMINATED", "INTERNAL_ERROR", "SKIPPED"], f"Expected final state: {state}"

DBAcademyHelper.monkey_patch(start_job)
