# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Set the storage account name

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('table_data', '''{'table_name': 'data.bank_accounts', 'UID_field': '_id', 'update_field': 'updated', 'refresh_time': '60', 'change_capture': 'True', 'schema_lower_range': '2023-06-03T00:34:52.158336Z', 'schema_upper_range': '2023-07-03T00:34:52.158246Z', 'schema_sample_size': 10000, 'exclude_fields': ['credentials', 'api_keys', 'password', 'firstname', 'lastname'], 'overwrite_existing': True, 'batch_lower_range': '2021-07-13T00:41:48.791992Z', 'batch_upper_range': '2023-06-19T12:41:48.792085Z'}''')


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

raw_table = parent_payload['table_name']
UID_field = parent_payload['UID_field']
update_field = parent_payload['update_field']

raw_zone = 'raw'
structured_zone = 'structured'

raw_table = parent_payload['table_name']
structured_table = f'stage_{raw_table}_10'

print(raw_table)



# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    dbutils.secrets.get(scope=storage_scope, key=storage_key))

connectionString = dbutils.secrets.get(scope=source_scope, key=source_key)

raw_checkpoint_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{raw_zone}/{raw_table}/_checkpoint"
raw_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{raw_zone}/{raw_table}"
dbutils.fs.mkdirs(raw_checkpoint_path)


# COMMAND ----------

#rawSchema = eval(spark.read.text(f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{raw_zone}/schema_store/{raw_table}_schema.txt").collect()[0][0])

rawSchema = open(f"/Workspace/{root_path}/config/schema/{source_name}/{raw_table}/raw_{raw_table}_schema.txt", 'r').read().strip('"')

rawconnectionSchema = eval(rawSchema).add('ingest_time', TimestampType(), True).add('clusterTime', TimestampType(), True).add('source_file', StringType(), True).add('processing_time', TimestampType(), True)


# COMMAND ----------

pipeline = ''
for field in eval(rawSchema).fields:
    pipeline = pipeline + f'{field.name}_tag:"$fullDocument.{field.name}",'

pipeline = '{'+pipeline+'clusterTime:1}'

stream_pipeline = '''{"$project": '''+pipeline + '''}'''
pipeline = '''[{"$project": '''+pipeline + '''}]'''


# COMMAND ----------


rawSchema_tag = eval(rawSchema.replace("',", "_tag',")).add('clusterTime', TimestampType(), True)

(spark.readStream
    .format("mongodb")
    .option('connection.uri', connectionString) 
    .option('database', database_name) 
    .option('collection', raw_table)
    .option('aggregation.pipeline', pipeline)
    .schema((rawSchema_tag))
    #.option("spark.mongodb.change.stream.publish.full.document.only", "false")
    #.option("spark.mongodb.change.stream.publish.full.document.only.tombstone.on.delete", "true")
    .load()
    .toDF(*list(map(lambda x: re.sub("_tag$", "", x), [f.name for f in rawSchema_tag.fields])))
    .filter('_id is not null')
    .select("*", current_timestamp().alias("ingest_time"))
    .withColumn('ingest_method', lit("stream"))
    .writeStream 
    .format("delta") 
    .option('checkpointLocation', raw_checkpoint_path)
    .trigger(availableNow=True)
    .option("mergeSchema", "true")
    #.trigger(processingTime = '1 second')
    .outputMode("append")
    .start(raw_path)
    )




# COMMAND ----------

dbutils.notebook.exit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().toString())
