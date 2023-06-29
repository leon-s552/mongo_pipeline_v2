# Databricks notebook source
# MAGIC %md
# MAGIC dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils.widgets.text('table_data', """{
# MAGIC         "table_name": "DIM_CONNECTION", 
# MAGIC         "structured_table": "stage_connection_data_10",
# MAGIC         "UID_field": "connection_id",
# MAGIC         "UID_code": "connection",
# MAGIC         "update_field": "modified_datetime",
# MAGIC         "update_code": "modified",
# MAGIC         "created_field": "created_datetime",
# MAGIC         "refresh_time": "60",
# MAGIC         "table_metadata": [
# MAGIC         {
# MAGIC         "source_code": "connection"
# MAGIC         ,"destination_name": "connection_id"
# MAGIC         ,"destination_datatype": "string"
# MAGIC         ,"change_tracked": "True"
# MAGIC         }
# MAGIC         ,{
# MAGIC         "source_code": "user"
# MAGIC         ,"destination_name": "user_id"
# MAGIC         ,"destination_datatype": "string"
# MAGIC         ,"change_tracked": "True"
# MAGIC         }
# MAGIC         ,{
# MAGIC         "source_code": "company"
# MAGIC         ,"destination_name": "company_id"
# MAGIC         ,"destination_datatype": "string"
# MAGIC         ,"change_tracked": "True"
# MAGIC         }
# MAGIC         ,{
# MAGIC         "source_code": "osp"
# MAGIC         ,"destination_name": "application"
# MAGIC         ,"destination_datatype": "string"
# MAGIC         ,"change_tracked": "True"
# MAGIC         }        
# MAGIC         ,{
# MAGIC         "source_code": "platform"
# MAGIC         ,"destination_name": "platform"
# MAGIC         ,"destination_datatype": "string"
# MAGIC         ,"change_tracked": "True"
# MAGIC         }
# MAGIC         ,{
# MAGIC         "source_code": "usage"
# MAGIC         ,"destination_name": "usage"
# MAGIC         ,"destination_datatype": "string"
# MAGIC         ,"change_tracked": "True"
# MAGIC         }
# MAGIC         ,{
# MAGIC         "source_code": "status"
# MAGIC         ,"destination_name": "connection_status"
# MAGIC         ,"destination_datatype": "string"
# MAGIC         ,"change_tracked": "True"
# MAGIC         }
# MAGIC         ,{
# MAGIC         "source_code": "created"
# MAGIC         ,"destination_name": "created_datetime"
# MAGIC         ,"destination_datatype": "timestamp"
# MAGIC         ,"change_tracked": "True"
# MAGIC         }
# MAGIC         ,{
# MAGIC         "source_code": "modified"
# MAGIC         ,"destination_name": "modified_datetime"
# MAGIC         ,"destination_datatype": "timestamp"
# MAGIC         ,"change_tracked": "False"
# MAGIC         }
# MAGIC     ]
# MAGIC }""")

# COMMAND ----------

from delta.tables import *
from pyspark.sql.window import *
import pyspark.sql.functions as F
from shared_functions import cctableupdate
import json
import toml

# COMMAND ----------

root_path = '/'.join(json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())['extraContext']['notebook_path'].split('/')[:-2]).strip('/')

parent_payload = eval(dbutils.widgets.getArgument("table_data"))

enriched_table = parent_payload['table_name']

table_metadata = json.loads((open(f"/Workspace/{root_path}/config/enriched_table_metadata/{enriched_table}.json", "r").read()))

config = toml.load(f"/Workspace/{root_path}/config/config.toml")
storage_account_name = config["ingest_parameters"]["storage_account"]

config = toml.load(f"/Workspace/{root_path}/config/config.toml")

source_name = storage_account_name = config["ingest_parameters"]["source"]
storage_account_name = config["ingest_parameters"]["storage_account"]
storage_container = config["ingest_parameters"]["storage_container"]

storage_scope = config["ingest_parameters"]["storage_scope"]
storage_key = config["ingest_parameters"]["storage_key"]

structured_zone = 'structured'
enriched_zone = 'enriched'

structured_table = table_metadata['structured_table']

UID_field = table_metadata['UID_field']
UID_code = table_metadata['UID_code']
update_field = table_metadata['update_field']
update_code = table_metadata['update_code']
created_field = table_metadata['created_field']

table_metadata = table_metadata['table_metadata']

print(enriched_table)


# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    dbutils.secrets.get(scope=storage_scope, key=storage_key))

# COMMAND ----------

enriched_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{enriched_zone}/{enriched_table}"
structured_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{structured_zone}/{structured_table}"
enriched_checkpoint_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{enriched_zone}/{enriched_table}/_checkpoint"
structured_checkpoint_path = f"abfss://{storage_container}@{storage_account_name}.dfs.core.windows.net/{structured_zone}/{structured_table}/_checkpoint"

initial_table_schema_script =''
for field in table_metadata:
    initial_table_schema_script = (initial_table_schema_script+","+field["destination_name"]+"::"+field["destination_datatype"]+"\n") 

initial_table_schema_script_full = (initial_table_schema_script.strip(',')
                        +",CURRENT_RECORD_FLAG::boolean\n"
                        +",VERSION_START_DATE::timestamp\n"
                        +",VERSION_END_DATE::timestamp\n"
                        +",VERSION_NUMBER::int").strip("\n,")

initial_table_schema_script_trim = initial_table_schema_script.strip("\n,")


transformation_script = ""
for field in table_metadata:
    transformation_script = transformation_script+","+field["source_code"]+"::"+field["destination_datatype"]+ " as " + field["destination_name"] + '\n'

initial_transformation_script = transformation_script.strip("\n,")

transformation_script = (transformation_script
                        +',NULL::boolean as CURRENT_RECORD_FLAG\n'
                        +',NULL::timestamp as VERSION_START_DATE\n'
                        +',NULL::timestamp as VERSION_END_DATE\n'
                        +',NULL::int as VERSION_NUMBER').strip("\n,").replace(f',{update_code}::', f',max({update_code})::')


initial_cc_script = ''
for field in table_metadata:
    if field['change_tracked'].casefold() == "True".casefold():
        initial_cc_script = (initial_cc_script+",coalesce("+field["source_code"]+"::string,'')\n") 
initial_cc_script = initial_cc_script.strip("\n,")


cc_script = ""
for field in table_metadata:
    if field['change_tracked'].casefold() == "True".casefold() and field['destination_name'] != UID_field and field['destination_name'] != update_field:
        cc_script = (cc_script+"or coalesce(a."+field["destination_name"]+",'') <> coalesce(b."+field["destination_name"]+",'')\n") 
cc_script = cc_script.strip('or ')


# COMMAND ----------


df_structured = spark.read.load(structured_path)
try:
    destination_length = spark.read.load(enriched_path).count()
except:
    destination_length = 0

if destination_length == 0:
    df_structured.createOrReplaceTempView('view_source')

    DeltaTable.createIfNotExists(spark).location(enriched_path).execute()

    df_initial_insert = spark.sql(f'''
                                    select distinct
                                        {initial_table_schema_script_full}
                                    from
                                        (with CTE as 
                                                (select distinct
                                                    {initial_transformation_script}

                                                ,upper(concat(
                                                {initial_cc_script}
                                                )) as lead_change
                                                        
                                                ,upper(lag(concat(
                                                {initial_cc_script}
                                                ),1,'NA') over (partition by {UID_code} 
                                                order by {update_code}
                                                )) as lag_change

                                            from 
                                                view_source

                                            QUALIFY 
                                                lag_change <> lead_change 
                                            )
                                        
                                    SELECT distinct
                                        {initial_table_schema_script_trim}

                                        ,case when row_number() over (partition by {UID_field} order by {update_field} desc) = 1 
                                                --and <<deleted flag>> ilike "FALSE" 
                                            then 1 
                                            else 0 
                                            end as CURRENT_RECORD_FLAG

                                        ,case when row_number() over (partition by {UID_field} order by {update_field} asc) = 1 
                                            then {created_field} 
                                            else {update_field} 
                                            end as VERSION_START_DATE
                                    
                                        ,lag({update_field} ,1,'2999-12-31T00:00:00.000+0000') over (partition by {UID_field} order by {update_field} desc) as VERSION_END_DATE
                                    
                                        ,row_number() over (partition by {UID_field} order by {update_field} asc) as VERSION_NUMBER

                                    from 
                                        CTE

                                    order by connection_id, modified_datetime
                                    );
                    ''')




    (df_initial_insert.write.option("checkpointLocation", enriched_checkpoint_path)
                            .mode("overwrite")
                            .option("mergeSchema", "true")
                            .save(enriched_path)
                            )

# COMMAND ----------

def batchprocess(microBatchOutputDF, batchId):
    cctableupdate(microBatchOutputDF, enriched_path, UID_field, update_field, created_field, transformation_script,cc_script, batchId)

(spark.readStream
    .format("delta")
    .load(structured_path)
    .writeStream
    .option("checkpointLocation", enriched_checkpoint_path)
    .trigger(availableNow=True)
    .option("mergeSchema", "true")
    .outputMode("append")
    .foreachBatch(batchprocess)
    .start()
    )


# COMMAND ----------

dbutils.notebook.exit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().toString())
