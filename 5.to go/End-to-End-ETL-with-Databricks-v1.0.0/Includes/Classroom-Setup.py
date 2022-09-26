# Databricks notebook source
# MAGIC %run ./_utility-functions

# COMMAND ----------

class PipelineDataFactory:
    def __init__(self, source):
        self.source = source           # The real dataset source
        self.target = DA.paths.source  # What the student thinks of as source
        self.max_batch = 31
        self.batch = 0
    
    def load(self, continuous=False):
        if self.batch < self.max_batch:
            self.move_file("customers")
            self.move_file("orders")
            self.move_file("status")
            self.batch += 1
        else:
            print("Data source exhausted\n")
            
    def move_file(self, dataset):
        curr_file = f"{dataset}/{self.batch:02}.json"
        dbutils.fs.cp(f"{self.source}/{curr_file}", f"{self.target}/{curr_file}")

# COMMAND ----------

pipeline_language = None
reset = dbutils.widgets.getArgument("mode", "").lower() == "reset"

# Create the DA object with as a synchronous course
DA = DBAcademyHelper(lesson=None, asynchronous=False)   

if reset: DA.cleanup()   # Remove the existing database and files
DA.init(create_db=True)  # True is the default
DA.install_datasets()    # Install (if necissary) and validate the datasets

DA.paths.source = f"{DA.paths.working_dir}/raw"
DA.paths.storage_location = f"{DA.paths.working_dir}/output"

DA.pipeline_data_factory = PipelineDataFactory("dbfs:/mnt/dbacademy-datasets/end-to-end-etl-with-databricks")

DA.conclude_setup()      # Conclude the setup by printing the DA object's final state

