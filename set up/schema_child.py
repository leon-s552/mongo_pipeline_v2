# Databricks notebook source
from pyspark.sql.streaming import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, ArrayType
from shared_functions import *
import os
import json
import toml

# COMMAND ----------

root_path = '/'.join(json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())['extraContext']['notebook_path'].split('/')[:-2]).strip('/')

config = toml.load(f"/Workspace/{root_path}/config/config.toml")
print(config)

# COMMAND ----------


parent_payload = eval(dbutils.widgets.getArgument("table_data"))
table_name = parent_payload['table_name']

config = toml.load(f"/Workspace/{root_path}/config/config.toml")

storage_account_name = config["ingest_parameters"]["storage_account"]
storage_container = config["ingest_parameters"]["storage_container"]
database_name = config["ingest_parameters"]["source_database"]

source_name = config["ingest_parameters"]["source"]
source_scope = config["ingest_parameters"]["source_scope"]
source_key = config["ingest_parameters"]["source_key"]
storage_scope = config["ingest_parameters"]["storage_scope"]
storage_key = config["ingest_parameters"]["storage_key"]

root_path = '/'.join(json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())['extraContext']['notebook_path'].split('/')[:-2]).strip('/')

raw_zone = 'raw'
structured_zone = 'structured'

schema_lower_range = parent_payload['schema_lower_range']
schema_upper_range = parent_payload['schema_upper_range']
schema_sample_size = parent_payload['schema_sample_size']
exclude_fields = parent_payload['exclude_fields']
overwrite_existing = parent_payload['overwrite_existing']

print(table_name)

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    dbutils.secrets.get(scope=storage_scope, key=storage_key))

connectionString = dbutils.secrets.get(scope=source_scope, key=source_key)

path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{raw_zone}/{table_name}"
dbutils.fs.mkdirs(path)

# COMMAND ----------

def schemaFlatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        #if isinstance(dtype, ArrayType):
        #    dtype = dtype.elementType

        if isinstance(dtype, StructType):
            fields += schemaFlatten(dtype, prefix=name)

        else:
            fields.append(col(name).alias(name.replace('.','__')))
    return fields

def dataFlatten(microBatchOutputDF):
    flattened_schema = schemaFlatten(microBatchOutputDF.schema)
    microBatchOutputDF = microBatchOutputDF.select(flattened_schema)
    return microBatchOutputDF

# COMMAND ----------

if overwrite_existing == True:
    pipeline = ''

    for field in exclude_fields:
        pipeline = pipeline + f'"{field}":0,'

    pipeline = '{'+pipeline.strip(',')+'}'
    pipeline = '''[{"$project": '''+pipeline + '''}, {"$sort": {"timestamp" : -1.0}},{$match:{
    "$expr":{
    "$and":[
      {"$gte":[{"$toDate":"$_id"}, ISODate("'''+ schema_lower_range +'''")]},
      {"$lte":[{"$toDate":"$_id"},ISODate("'''+ schema_upper_range +'''")]}
    ]
    }
    }}]'''

    query = (spark.read.format("mongodb")
        .option('connection.uri', connectionString) 
        .option('database', database_name) 
        .option('collection', table_name)
        .option('aggregation.pipeline', pipeline)
        .option('sampleSize', schema_sample_size)
        .option('inferSchema', True)
        .option('sql.inferSchema.mapTypes.minimum.key.size', 10000)
        .load()
        ).limit(10000)
   

    flatstructuredconnectionSchema = dataFlatten(query).schema

    structuredconnectionSchema = query.schema
    
    
    os.makedirs(os.path.dirname(f"/Workspace/{root_path}/config/schema/mongodb/{table_name}/structured_{table_name}_schema.txt"), exist_ok=True)

    open(f"/Workspace/{root_path}/config/schema/mongodb/{table_name}/structured_{table_name}_schema.txt", "w").write(str(repr(structuredconnectionSchema)))



    os.makedirs(os.path.dirname(f"/Workspace/{root_path}/config/schema/mongodb/{table_name}/flat_structured_{table_name}_schema.txt"), exist_ok=True)

    open(f"/Workspace/{root_path}/config/schema/mongodb/{table_name}/flat_structured_{table_name}_schema.txt", "w").write(str(repr(flatstructuredconnectionSchema)))


    rawconnectionSchema = "StructType(["
    for col in query.columns:
        rawconnectionSchema = rawconnectionSchema + f"StructField('{col}', StringType(), True),"

    rawconnectionSchema = (rawconnectionSchema.strip(',') + "])")


    os.makedirs(os.path.dirname(f"/Workspace/{root_path}/config/schema/mongodb/{table_name}/raw_{table_name}_schema.txt"), exist_ok=True)

    open(f"/Workspace/{root_path}/config/schema/mongodb/{table_name}/raw_{table_name}_schema.txt", "w").write(str(repr(rawconnectionSchema)))


# COMMAND ----------

dbutils.notebook.exit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().toString())
