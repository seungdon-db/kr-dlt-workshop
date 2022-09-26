# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

# The following attributes are externalized to make them easy
# for content developers to update with every new course.

_course_code = "itw"
_naming_params = {"course": _course_code}
_course_name = "introduction-to-workflows"
_data_source_name = _course_name
_data_source_version = "v01"

_min_time = "3 min"   # The minimum amount of time to install the datasets (e.g. from Oregon)
_max_time = "10 min"  # The maximum amount of time to install the datasets (e.g. from India)

# Set to true only if this course uses streaming APIs which 
# in turn creates a checkpoints path for all checkpoint files.
_enable_streaming_support = False 

# COMMAND ----------

# Defines the files "expected" to exist in DBFS - required for execution in Financial and Government workspaces
_remote_files = []

# COMMAND ----------

class Paths():
    def __init__(self, working_dir, clean_lesson):
        global _course_employes_streaming
        
        self.working_dir = working_dir

        # The location of the user's database - presumes all courses have at least one DB.
        # The location of the database varies if we have a database per lesson or one database per course
        if clean_lesson: self.user_db = f"{working_dir}/{clean_lesson}.db"
        else:            self.user_db = f"{working_dir}/database.db"

        # When working with streams, it helps to put all checkpoints in their
        # own directory relative the the previously defined working_dir
        if _enable_streaming_support:
            self.checkpoints = f"{working_dir}/_checkpoints"    
            
    @staticmethod
    def exists(path):
        """
        Returns true if the specified path exists else false.
        """
        try: return len(dbutils.fs.ls(path)) >= 0
        except Exception:return False

    def print(self, padding="  "):
        """
        Prints all the pathes attached to this instance of Paths
        """
        max_key_len = 0
        for key in self.__dict__: 
            max_key_len = len(key) if len(key) > max_key_len else max_key_len
        for key in self.__dict__:
            label = f"{padding}DA.paths.{key}:"
            print(label.ljust(max_key_len+13) + self.__dict__[key])
        
    def __repr__(self):
        return self.__dict__.__repr__().replace(", ", ",\n").replace("{","").replace("}","").replace("'","")

# COMMAND ----------

class DBAcademyHelper():
    def __init__(self, lesson=None, asynchronous=True):
        import re, time

        self.start = int(time.time())
        
        # Intialize from our global variables defined at the top of the notebook
        global _course_code, _course_name, _data_source_name, _data_source_version, _naming_params, _remote_files
        self.course_code = _course_code
        self.course_name = _course_name
        self.remote_files = _remote_files
        self.naming_params = _naming_params
        self.data_source_name = _data_source_name
        self.data_source_version = _data_source_version

        # Are we running under test? If so we can "optimize" for parallel execution 
        # without affecting the student's runtime-experience. As in the student can
        # use one working directory and one database, but under test, we can use many
        from dbacademy import dbgems 
        is_smoke_test = (spark.conf.get("dbacademy.smoke-test", "false").lower() == "true")
        
        if lesson is None and asynchronous and is_smoke_test:
            # The developer did not define a lesson, we can run asynchronous, and this 
            # is a smoke test so we can define a lesson here for the sake of testing
            lesson = str(abs(hash(dbgems.get_notebook_path())) % 10000)
            
        self.lesson = None if lesson is None else lesson.lower()

        
        # Define username using the hive function (cleaner than notebooks API)
        self.username = spark.sql("SELECT current_user()").first()[0]

        # Create the database name prefix according to curriculum standards. This
        # is the value by which all databases in this course should start with.
        # Besides creating this lesson's database name, this value is used almost
        # exclusively in the Rest notebook.
        da_name, da_hash = self.get_username_hash()
        self.db_name_prefix = f"da-{da_name}@{da_hash}-{self.course_code}"      # Composite all the values to create the "dirty" database name
        self.db_name_prefix = re.sub("[^a-zA-Z0-9]", "_", self.db_name_prefix)  # Replace all special characters with underscores (not digit or alpha)
        while "__" in self.db_name_prefix: 
            self.db_name_prefix = self.db_name_prefix.replace("__", "_")        # Replace all double underscores with single underscores
        
        # This is the common super-directory for each lesson, removal of which 
        # is designed to ensure that all assets created by students is removed.
        # As such, it is not attached to the path object so as to hide it from 
        # students. Used almost exclusively in the Rest notebook.
        working_dir_root = f"dbfs:/mnt/dbacademy-users/{self.username}/{self.course_code}"

        if self.lesson is None:
            working_dir = working_dir_root         # No lesson, working dir is same as root
            self.paths = Paths(working_dir, None)  # Create the "visible" path
            self.hidden = Paths(working_dir, None) # Create the "hidden" path
            self.db_name = self.db_name_prefix     # No lesson, database name is the same as prefix
        else:
            working_dir = f"{working_dir_root}/{self.lesson}"                     # Working directory now includes the lesson name
            self.clean_lesson = re.sub("[^a-zA-Z0-9]", "_", self.lesson.lower())  # Replace all special characters with underscores
            self.paths = Paths(working_dir, self.clean_lesson)                    # Create the "visible" path
            self.hidden = Paths(working_dir, self.clean_lesson)                   # Create the "hidden" path
            self.db_name = f"{self.db_name_prefix}_{self.clean_lesson}"           # Database name includes the lesson name

        # Register the working directory root to the hidden set of paths
        self.hidden.working_dir_root = working_dir_root

        # This is where the datasets will be downloaded to and should be treated as read-only for all pratical purposes
        self.hidden.datasets = f"dbfs:/mnt/dbacademy-datasets/{self.data_source_name}/{self.data_source_version}"
        
        # This is the location in our Azure data repository of the datasets for this lesson
        self.data_source_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{self.data_source_name}/{self.data_source_version}"
    
    def get_username_hash(self):
        """
        Utility method to split the user's email address, dropping the domain, and then creating a hash based on the full email address and course_code. The primary usage of this function is in creating the user's database, but is also used in creating SQL Endpoints, DLT Piplines, etc - any place we need a short, student-specific name.
        """
        da_name = self.username.split("@")[0]                                   # Split the username, dropping the domain
        da_hash = abs(hash(f"{self.username}-{self.course_code}")) % 10000      # Create a has from the full username and course code
        return da_name, da_hash

    @staticmethod
    def monkey_patch(function_ref, delete=True):
        """
        This function "monkey patches" the specified function to the DBAcademyHelper class. While not 100% necissary, this pattern does allow each function to be defined in it's own cell which makes authoring notebooks a little bit easier.
        """
        import inspect
        
        signature = inspect.signature(function_ref)
        assert "self" in signature.parameters, f"""Missing the required parameter "self" in the function "{function_ref.__name__}()" """
        
        setattr(DBAcademyHelper, function_ref.__name__, function_ref)
        if delete: del function_ref        

# COMMAND ----------

def init(self, create_db=True):
    """
    This function aims to setup the invironment enabling the constructor to provide initialization of attributes only and thus not modifying the environment upon initialization.
    """

    spark.catalog.clearCache() # Clear the any cached values from previus lessons
    self.create_db = create_db # Flag to indicate if we are creating the database or not

    if create_db:
        print(f"\nCreating the database \"{self.db_name}\"")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_name} LOCATION '{self.paths.user_db}'")
        spark.sql(f"USE {self.db_name}")

DBAcademyHelper.monkey_patch(init)

# COMMAND ----------

def cleanup(self, validate=True):
    """
    Cleans up the user environment by stopping any active streams, dropping the database created by the call to init() and removing the user's lesson-specific working directory and any assets created in that directory.
    """

    for stream in spark.streams.active:
        print(f"Stopping the stream \"{stream.name}\"")
        stream.stop()
        try: stream.awaitTermination()
        except: pass # Bury any exceptions

    if spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.db_name}'").count() == 1:
        print(f"Dropping the database \"{self.db_name}\"")
        spark.sql(f"DROP DATABASE {self.db_name} CASCADE")

    if self.paths.exists(self.paths.working_dir):
        print(f"Removing the working directory \"{self.paths.working_dir}\"")
        dbutils.fs.rm(self.paths.working_dir, True)

    if validate:
        print()
        self.validate_datasets(fail_fast=False)
        
DBAcademyHelper.monkey_patch(cleanup)

# COMMAND ----------

def conclude_setup(self):
    """
    Concludes the setup of DBAcademyHelper by advertising to the student the new state of the environment such as predefined path variables, databases and tables created on behalf of the student and the total setup time. Additionally, all path attributes are pushed to the Spark context for reference in SQL statements.
    """

    import time

    # Inject the user's database name
    # Add custom attributes to the SQL context here.
    spark.conf.set("da.db_name", self.db_name)
    spark.conf.set("DA.db_name", self.db_name)
    
    # Automatically add all path attributes to the SQL context as well.
    for key in self.paths.__dict__:
        spark.conf.set(f"da.paths.{key.lower()}", self.paths.__dict__[key])
        spark.conf.set(f"DA.paths.{key.lower()}", self.paths.__dict__[key])

    print("\nPredefined Paths:")
    self.paths.print()

    if self.create_db:
        print(f"\nPredefined tables in {self.db_name}:")
        tables = spark.sql(f"SHOW TABLES IN {self.db_name}").filter("isTemporary == false").select("tableName").collect()
        if len(tables) == 0: print("  -none-")
        for row in tables: print(f"  {row[0]}")

    print(f"\nSetup completed in {int(time.time())-self.start} seconds")

DBAcademyHelper.monkey_patch(conclude_setup)

# COMMAND ----------

def block_until_stream_is_ready(self, query, min_batches=2):
    """
    A utility method used in streaming notebooks to block until the stream has processed n batches. This method serves one main purpose in two different usescase
    
    The purpose is to block the current command until the state of the stream is ready and thus allowing the next command to execute against the properly initialized stream.
    
    The first use case is in jobs where where the stream is started in one cell but execution of subsequent cells start prematurely.
    
    The second use case is to slow down students who likewise attempt to execute subsequent cells before the stream is in a valid state either by invoking subsequent cells directly or by execute the Run-All Command
        
    Note: it is best to show the students this code the first time so that they understand what it is doing and why, but from that point forward, just call it via the DA object.
    """
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")
    
DBAcademyHelper.monkey_patch(block_until_stream_is_ready)

# COMMAND ----------

def install_datasets(self, reinstall=False, repairing=False):
    """
    Install the datasets used by this course to DBFS.
    
    This ensures that data and compute are in the same region which subsequently mitigates performance issues when the storage and compute are, for example, on opposite sides of the world.
    """
    import time

    global _min_time, _max_time
    min_time = _min_time
    max_time = _max_time

    if not repairing: print(f"\nThe source for this dataset is\n{self.data_source_uri}/\n")

    if not repairing: print(f"Your dataset directory is\n{self.hidden.datasets}\n")
    existing = self.paths.exists(self.hidden.datasets)

    if not reinstall and existing:
        print(f"Skipping install of existing dataset.")
        print()
        self.validate_datasets(fail_fast=False)
        return 

    # Remove old versions of the previously installed datasets
    if existing:
        print(f"Removing previously installed datasets from\n{self.hidden.datasets}")
        dbutils.fs.rm(self.hidden.datasets, True)

    print(f"""\nInstalling the datasets to {self.hidden.datasets}""")

    print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
          region that your workspace is in, this operation can take as little as {min_time} and 
          upwards to {max_time}, but this is a one-time operation.""")

    files = dbutils.fs.ls(self.data_source_uri)
    what = "dataset" if len(files) == 1 else "datasets"
    print(f"\nInstalling {len(files)} {what}: ")
    
    install_start = int(time.time())
    for f in files:
        start = int(time.time())
        print(f"Copying /{f.name[:-1]}", end="...")

        dbutils.fs.cp(f"{self.data_source_uri}/{f.name}", f"{self.hidden.datasets}/{f.name}", True)
        print(f"({int(time.time())-start} seconds)")

    print()
    self.validate_datasets(fail_fast=True)
    print(f"""\nThe install of the datasets completed successfully in {int(time.time())-install_start} seconds.""")  

DBAcademyHelper.monkey_patch(install_datasets)

# COMMAND ----------

def list_r(self, path, prefix=None, results=None):
    """
    Utility method used by the dataset validation, this method performs a recursive list of the specified path and returns the sorted list of paths.
    """
    if prefix is None: prefix = path
    if results is None: results = list()
    
    files = dbutils.fs.ls(path)
    for file in files:
        data = file.path[len(prefix):]
        results.append(data)
        if file.isDir(): 
            self.list_r(file.path, prefix, results)
        
    results.sort()
    return results

DBAcademyHelper.monkey_patch(list_r)

# COMMAND ----------

def enumerate_remote_datasets(self):
    """
    Development function used to enumerate the remote datasets for use in validate_datasets()
    """
    files = self.list_r(self.data_source_uri)
    files = "_remote_files = " + str(files).replace("'", "\"")
    
    print("Copy the following output and paste it in its entirety into command #4 or the _utility-functions notebook.")
    print("-"*80)
    print(files)

DBAcademyHelper.monkey_patch(enumerate_remote_datasets)

# COMMAND ----------

def enumerate_local_datasets(self):
    """
    Development function used to enumerate the local datasets for use in validate_datasets()
    """
    files = self.list_r(self.hidden.datasets)
    files = "_remote_files = " + str(files).replace("'", "\"")
    
    print("Copy the following output and paste it in its entirety into command #4 or the _utility-functions notebook.")
    print("-"*80)
    print(files)

DBAcademyHelper.monkey_patch(enumerate_local_datasets)

# COMMAND ----------

def do_validate(self):
    """
    Utility method to compare local datasets to the registered list of remote files.
    """
    
    local_files = self.list_r(self.hidden.datasets)
    
    for file in local_files:
        if file not in self.remote_files:
            print(f"\n  - Found extra file: {file}")
            print(f"  - This problem can be fixed by reinstalling the datasets")
            return False

    for file in self.remote_files:
        if file not in local_files:
            print(f"\n  - Missing file: {file}")
            print(f"  - This problem can be fixed by reinstalling the datasets")
            return False
        
    return True
    

def validate_datasets(self, fail_fast:bool):
    """
    Validates the "install" of the datasets by recursively listing all files in the remote data repository as well as the local data repository, validating that each file exists but DOES NOT validate file size or checksum.
    """
    import time
    start = int(time.time())
    print(f"Validating the local copy of the datsets", end="...")
    
    if not self.do_validate():
        if fail_fast:
            raise Exception("Validation failed - see previous messages for more information.")
        else:
            print("\nAttempting to repair local dataset...\n")
            self.install_datasets(reinstall=True, repairing=True)
    
    print(f"({int(time.time())-start} seconds)")

DBAcademyHelper.monkey_patch(do_validate)
DBAcademyHelper.monkey_patch(validate_datasets)

# COMMAND ----------

def _init_mlflow_as_job():
    """
    Used to initialize MLflow with the job ID when ran under test. Because this is not user-facing, we do not monkey-patch it into DBAcademyHelper.
    """
    import mlflow
    
    job_experiment_id = sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
        dbutils.entry_point.getDbutils().notebook().getContext().tags())["jobId"]

    if job_experiment_id:
        mlflow.set_experiment(f"/Curriculum/Experiments/{job_experiment_id}")

    init_mlflow_as_job()

    None # Suppress output
