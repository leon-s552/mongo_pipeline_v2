# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Set the storage account name

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('table_data', "{'table_name': 'connection_data', 'UID_field': '_id', 'update_field': 'modified', 'schema_lower_range': '2023-05-18T00:40:55.276177Z', 'schema_upper_range': '2023-06-01T00:40:55.276106Z', 'schema_sample_size': 10000, 'change_capture': 'True'}")

# COMMAND ----------

dbutils.widgets.text('table_data', """{
        "table_name": "logins", 
        "UID_field": "_id",
        "update_field": "timestamp",
        "refresh_time": "60",
        "change_capture": "False"
    }""")


# COMMAND ----------

from pyspark.sql.streaming import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from delta.tables import *
from pyspark.sql.window import Window
from pyspark.sql import Row, functions as F
from shared_functions import *
import toml
import json

# COMMAND ----------

root_path = '/'.join(json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())['extraContext']['notebook_path'].split('/')[:-2]).strip('/')

parent_payload = eval(dbutils.widgets.getArgument("table_data"))
config = toml.load(f"/Workspace/{root_path}/config/config.toml")

raw_table = parent_payload['table_name']
UID_field = parent_payload['UID_field']
update_field = parent_payload['update_field']
change_capture = parent_payload['change_capture']

storage_account_name = config["ingest_parameters"]["storage_account"]
storage_container = config["ingest_parameters"]["storage_container"]
database_name = config["ingest_parameters"]["source_database"]
source_name = config["ingest_parameters"]["source"]

source_scope = config["ingest_parameters"]["source_scope"]
source_key = config["ingest_parameters"]["source_key"]
storage_scope = config["ingest_parameters"]["storage_scope"]
storage_key = config["ingest_parameters"]["storage_key"]

raw_zone = 'raw'
structured_zone = 'structured'

raw_table = parent_payload['table_name']
structured_table = f'stage_{raw_table}_10'

print(raw_table)
print(change_capture)


# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    dbutils.secrets.get(scope=storage_scope, key=storage_key))

raw_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{raw_zone}/{raw_table}"

structured_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{structured_zone}/{structured_table}"
structured_checkpoint_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{structured_zone}/{structured_table}/_checkpoint"
dbutils.fs.mkdirs(structured_path)



# COMMAND ----------

#flatstructuredSchema = (spark.read.text(f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{structured_zone}/schema_store/flat_structured_{raw_table}_schema.txt").collect()[0][0])

flatstructuredSchema = open(f"/Workspace/{root_path}/config/schema/{source_name}/{raw_table}/flat_structured_{raw_table}_schema.txt", 'r').read().strip('"')

#structuredSchema = (spark.read.text(f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{structured_zone}/schema_store/structured_{raw_table}_schema.txt").collect()[0][0])

structuredSchema = open(f"/Workspace/{root_path}/config/schema/{source_name}/{raw_table}/structured_{raw_table}_schema.txt", 'r').read().strip('"')

flatstructuredconnectionSchema = eval(flatstructuredSchema).add('clusterTime', TimestampType(), True).add('ingest_time', TimestampType(), True).add('ingest_method', StringType(), True).add('source_file', StringType(), True).add('processing_time', TimestampType(), True)

structuredconnectionSchema = eval(structuredSchema).add('clusterTime', TimestampType(), True).add('ingest_time', TimestampType(), True).add('ingest_method', StringType(), True).add('source_file', StringType(), True).add('processing_time', TimestampType(), True)

# COMMAND ----------

DeltaTable.createIfNotExists(spark).location(structured_path).addColumns((flatstructuredconnectionSchema)).execute()

deltaTable = DeltaTable.forPath(spark, structured_path)

def batchprocess(microBatchOutputDF, batchId):
    microBatchOutputDF = schemaconversion(microBatchOutputDF, structuredconnectionSchema, batchId)
    microBatchOutputDF = dataFlatten(microBatchOutputDF, batchId)
    microBatchOutputDF = removebatchduplicates(microBatchOutputDF, UID_field, update_field, change_capture, batchId)
    microBatchOutputDF = upsertToDelta(microBatchOutputDF, deltaTable, UID_field, update_field, change_capture, batchId)
    
    

(spark.readStream
    .format("delta")
    .load(raw_path)
    .select("*", input_file_name().alias("source_file"), current_timestamp().alias("processing_time"))
    .writeStream
    .option("checkpointLocation", structured_checkpoint_path)
    .trigger(availableNow=True)
    .option("mergeSchema", "true")
    .outputMode("append")
    .foreachBatch(batchprocess)
    .start()
    )


# COMMAND ----------

dbutils.notebook.exit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().toString())
