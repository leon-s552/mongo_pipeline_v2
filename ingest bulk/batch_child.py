# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

#dbutils.widgets.text('table_data', '''{'table_name': 'users', 'UID_field': '_id', 'update_field': 'metadata__logins__last', 'refresh_time': '60', 'change_capture': 'True', 'schema_lower_range': '2023-04-29T00:49:52.474261Z', 'schema_upper_range': '2023-06-28T00:49:52.474135Z', 'schema_sample_size': 10000, 'exclude_fields': ['credentials', 'api_keys', 'password', 'firstname', 'lastname'], 'overwrite_existing': True, 'batch_lower_range': '2021-07-08T00:50:16.779220Z', 'batch_upper_range': '2023-06-14T12:50:16.779297Z'}''')

# COMMAND ----------



# COMMAND ----------

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.streaming import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.window import Window
from pyspark.sql import Row, functions as F
from multiprocessing.pool import ThreadPool
from datetime import datetime, timedelta
import shared_functions
import uuid
import toml
import json
import re

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

source_table = parent_payload['table_name']
UID_field = parent_payload['UID_field']
update_field = parent_payload['update_field']
change_capture = parent_payload['change_capture']

batch_lower_range = parent_payload['batch_lower_range']
batch_upper_range = parent_payload['batch_upper_range']

#batch_lower_range = '2023-01-01T00:00:00.000Z'
#batch_upper_range = '2023-05-11T00:00:00.000Z'

source_zone = 'raw'
destination_zone = 'structured'

source_table = parent_payload['table_name']
destination_table = f'stage_{source_table}_10'

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    dbutils.secrets.get(scope=storage_scope, key=storage_key))

connectionString = dbutils.secrets.get(scope=source_scope, key=source_key)


# COMMAND ----------

#rawSchema = eval(spark.read.text(f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{source_zone}/schema_store/raw_{source_table}_schema.txt").collect()[0][0])

rawSchema = open(f"/Workspace/{root_path}/config/schema/{source_name}/{source_table}/raw_{source_table}_schema.txt", 'r').read().strip('"')

rawconnectionSchema = eval(rawSchema).add('clusterTime', TimestampType(), True).add('source_file', StringType(), True).add('processing_time', TimestampType(), True)


# COMMAND ----------


batch_checkpoint_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{source_zone}/{source_table}/_checkpoint_batch"

raw_checkpoint_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{source_zone}/{source_table}/_checkpoint"

raw_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{source_zone}/{source_table}"

batch_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{source_zone}/manual_batch_data/{source_table}"

structured_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{destination_zone}/{destination_table}"

print(source_table)

# COMMAND ----------

pipeline = ''
for field in eval(rawSchema).fields:
    pipeline = pipeline + f'"{field.name}":1,'


pipeline = '{'+pipeline.strip(',')+'}'

stream_pipeline = '''{"$project": '''+pipeline + '''}'''
pipeline = '''[{"$project": '''+pipeline + '''},{$match:{
  "$expr":{
    "$and":[
      {"$gte":[{"$toDate":"$_id"}, ISODate("''' + batch_lower_range + '''")]},
      {"$lte":[{"$toDate":"$_id"},ISODate("''' + batch_upper_range + '''")]}
    ]
  }
}}]'''

rawSchema1 = eval(rawSchema)

if '__' in update_field:
    cluster_time_code = 'to_timestamp(F.split(F.split(re.split(\'__\', update_field)[0], re.split(\'__\', update_field)[-1])[1],\'"\')[2])'
else:
    cluster_time_code = 'to_timestamp(col(f"{update_field}"))'

(spark.read.format("mongodb")
    .option('connection.uri', connectionString) 
    .option('database', database_name)
    .option('collection', source_table)
    .option('aggregation.pipeline', pipeline)
    .schema(rawSchema1)
    .load().select("*", current_timestamp().alias("ingest_time"))
    .withColumn('clusterTime', eval(cluster_time_code))
    .withColumn('ingest_method', lit("batch"))
    .filter('_id is not null')
    .write
    .format("delta")
    .option("mergeSchema", "true")
    .mode("append")
    .save(raw_path))

    


# COMMAND ----------

dbutils.notebook.exit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().toString())
