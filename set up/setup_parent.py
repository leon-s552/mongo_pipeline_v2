# Databricks notebook source
from datetime import datetime, timedelta
from databricks.sdk import *
from databricks.sdk.service.jobs import *
import json
from shared_functions import *
from multiprocessing.pool import ThreadPool
import json
import toml

# COMMAND ----------

root_path = '/'.join(json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())['extraContext']['notebook_path'].split('/')[:-2]).strip('/')

config = toml.load(f"/Workspace/{root_path}/config/config.toml")
storage_account_name = config["ingest_parameters"]["storage_account"]

config = toml.load(f"/Workspace/{root_path}/config/config.toml")

source_name = storage_account_name = config["ingest_parameters"]["source"]
storage_account_name = config["ingest_parameters"]["storage_account"]
storage_container = config["ingest_parameters"]["storage_container"]
database_name = config["ingest_parameters"]["source_database"]

storage_scope = config["ingest_parameters"]["storage_scope"]
storage_key = config["ingest_parameters"]["storage_key"]


# COMMAND ----------

compute_cluster_id = '0321-223127-h7a2kk'

table_list = json.loads((open(f"/Workspace/{root_path}/config/table_master.json", "r").read()))

pool = ThreadPool(len(table_list) * 4)
timeout_seconds = 1000

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    dbutils.secrets.get(scope=storage_scope, key=storage_key))

# COMMAND ----------

display(table_list)

# COMMAND ----------

# DBTITLE 1,schema generation/refresh
day_delta = 60
schema_upper_range = datetime.now().strftime("%FT%T.%fZ")
schema_lower_range = (datetime.now() - timedelta(minutes = 1440 * day_delta)).strftime("%FT%T.%fZ")
exclude_fields = ['credentials','api_keys','password','firstname','lastname']
overwrite_existing = True

schema_sample_size = 10000

#schema_lower_range = '2023-01-01T00:00:00.000Z'
#schema_upper_range = '2023-05-11T00:00:00.000Z'
pool.map(lambda x: x.update((
                    {'schema_lower_range' : schema_lower_range,
                    'schema_upper_range' : schema_upper_range,
                    'schema_sample_size': schema_sample_size,
                    'exclude_fields': exclude_fields,
                    'overwrite_existing': overwrite_existing }
                    )), table_list)
notebook = "schema_child"

pool.map(lambda x:  notebook_invocation(notebook, timeout_seconds, x), table_list)


# COMMAND ----------

# DBTITLE 1,bulk load - backcaptures historic data outside of the 2 week change capture window
upper_day_delta = 13.5  #stream backcapture will return up to 14 days
lower_day_delta = 720

batch_lower_range = (datetime.now() - timedelta(minutes = 1440 * lower_day_delta)).strftime("%FT%T.%fZ")
batch_upper_range = (datetime.now() - timedelta(minutes = 1440 * upper_day_delta)).strftime("%FT%T.%fZ")

#batch_lower_range = '2023-01-01T00:00:00.000Z'
#batch_upper_range = '2023-05-11T00:00:00.000Z'

pool.map(lambda x: x.update((
                    {'batch_lower_range' : batch_lower_range,
                    'batch_upper_range' : batch_upper_range}
                    )), table_list)

notebook = f"/{root_path}/ingest bulk/batch_child"
pool.map(lambda x: notebook_invocation(notebook, timeout_seconds, x), table_list)



# COMMAND ----------

# DBTITLE 1,process structured data
notebook = f"/{root_path}/ingest stream/stream_structured_child"
pool.map(lambda x: notebook_invocation(notebook, timeout_seconds, x), table_list)

# COMMAND ----------

# DBTITLE 1,initiate stream - sets the initial checkpoint for future streams
notebook = f"/{root_path}/ingest stream/stream_raw_child"
pool.map(lambda x: notebook_invocation(notebook, timeout_seconds, x), table_list)

# COMMAND ----------

# DBTITLE 1,get historic change capture data (up to 2 weeks). stream must be set up first
##winds back latest checkpoint file by 2 weeks. If the query is outside of the retention period it will error. automatically triggers the stream_raw_child script after so will capture data. Doesnt run the structured notebook however this can be added too

notebook = f"/{root_path}/ingest bulk/stream_backcapture_child"

backcapture_period = 1203600  #13 days 23 hours = 1203600

pool.map(lambda x: x.update((
                    {'backcapture_period' : backcapture_period}
                    )), table_list)

pool.map(lambda x: notebook_invocation(notebook, timeout_seconds, x), table_list)

# COMMAND ----------

# DBTITLE 1,process structured data
notebook = f"/{root_path}/ingest stream/stream_structured_child"
pool.map(lambda x: notebook_invocation(notebook, timeout_seconds, x), table_list)

# COMMAND ----------

# DBTITLE 1,pipeline attributes
from databricks.sdk import *
from databricks.sdk.service.jobs import *

timezone = 'Pacific/Auckland'
cron = "0 0 6 * * ?" #every day as X time
#cron = "0 0 0/1 * * ?" #every hour

job_name            = f'{source_name}_pipeline'
description         = 'ELT pipeline from ingest->structured->enriched zones'

notebook_list = [
        {
            'notebook_name': 'stream_parent',
            'notebook_path': f'/{root_path}/ingest stream/stream_parent',
            'dependancies': ''
        }
        ,{
            'notebook_name': 'enriched_parent',
            'notebook_path': f'/{root_path}/enriched/enriched_parent',
            'dependancies': [TaskDependency(task_key="stream_parent")]
        }
]
task_list = []

for x in notebook_list:
    notebook_name = x['notebook_name']
    notebook_path = x['notebook_path']
    dependancies = x['dependancies']
    task_list.append(
        Task(
            task_key = f"{notebook_name}",
            depends_on = dependancies,
            description = f"{description}",
            existing_cluster_id = f"{compute_cluster_id}",
            notebook_task = NotebookTask(
                                        base_parameters = dict(""),
                                        notebook_path = f"{notebook_path}",
                                        source = Source("WORKSPACE")
                                        )
        
  ))


# COMMAND ----------

# DBTITLE 1,creates pipeline
from databricks.sdk import *
from databricks.sdk.service.jobs import *

w = WorkspaceClient()

j = w.jobs.create(
  name = job_name,
  max_retries = 1,
  timeout_seconds = 0,
  webhook_notifications = {},
  schedule = CronSchedule(quartz_cron_expression = cron,
                        timezone_id = timezone,
                        pause_status = PauseStatus("UNPAUSED")
                    ),
  email_notifications = JobEmailNotifications(
            on_failure  = ["leon.mcewen@9spokes.com"],
            no_alert_for_skipped_runs = True
  ),

  tasks = task_list
)
