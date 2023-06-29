# Databricks notebook source
#dbutils.widgets.text('table_data', '{"table_name": "connection_data","UID_field": "_id","update_field": "modified","batch_lower_range": "2023-01-01T00:00:00.000Z","batch_upper_range": "2023-05-11T00:00:00.000Z"}')

# COMMAND ----------

from azure.storage.blob import ContainerClient, BlobClient, BlobServiceClient
from datetime import datetime
from shared_functions import notebook_invocation
import re
import toml
import json

# COMMAND ----------

root_path = '/'.join(json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())['extraContext']['notebook_path'].split('/')[:-2]).strip('/')

parent_payload = eval(dbutils.widgets.getArgument("table_data"))
config = toml.load(f"/Workspace/{root_path}/config/config.toml")

storage_account_name = config["ingest_parameters"]["storage_account"]
storage_container = config["ingest_parameters"]["storage_container"]
database_name = config["ingest_parameters"]["source_database"]

source_name = config["ingest_parameters"]["source"]
source_scope = config["ingest_parameters"]["source_scope"]
source_key = config["ingest_parameters"]["source_key"]
storage_scope = config["ingest_parameters"]["storage_scope"]
storage_key = config["ingest_parameters"]["storage_key"]

table = parent_payload['table_name']
#table = 'logins'
zone = 'raw'
backcapture_period = parent_payload['backcapture_period']
#backcapture_period = 1200000    ##in seconds

print(table)


# COMMAND ----------

connectionString = dbutils.secrets.get(scope=storage_scope, key=storage_key)

connectionString = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={connectionString};EndpointSuffix=core.windows.net"

# COMMAND ----------

blob_service_client = BlobServiceClient.from_connection_string(connectionString)

container_client = blob_service_client.get_container_client(container=storage_container)

blob_list = container_client.list_blobs(f'/{zone}/{table}/_checkpoint/offsets/')


sorted_blob_list = sorted(blob_list,key=lambda x: x.creation_time, reverse=True)
blob_name = sorted_blob_list[0]['name']

blob_client = blob_service_client.get_blob_client(container=storage_container, blob=blob_name)

downloader = blob_client.download_blob(max_concurrency=1, encoding='UTF-8')

blob_text = downloader.readall()

newunixtimestamp = str(datetime.now().timestamp() - backcapture_period)[:10]

oldunixtimestamp = str(blob_text[-10:])

blob_text = re.sub(oldunixtimestamp+'$', newunixtimestamp,blob_text)

blob_client.upload_blob(blob_text, overwrite=True)




 

# COMMAND ----------

notebook_invocation(f"/{root_path}/ingest stream/stream_raw_child", 1000, parent_payload)

# COMMAND ----------

dbutils.notebook.exit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().toString())
