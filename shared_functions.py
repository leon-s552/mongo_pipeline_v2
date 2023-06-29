from multiprocessing.pool import ThreadPool
import json
from pyspark.sql.streaming import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from delta.tables import *
from pyspark.sql.window import Window
from pyspark.sql import Row, functions as F
from databricks.sdk.runtime import *





def notebook_invocation(notebook, timeout, argument):
    try:
        run_id = dbutils.notebook.run(
            path = notebook,
            timeout_seconds=timeout,
            arguments={'table_data': str(argument)}
        ).replace('Some(','').replace(')','')
        print (str(argument['table_name'])+ f': Success  --  Job# {run_id}\n')
    except:
        run_id = dbutils.notebook.run(
            path = notebook,
            timeout_seconds=timeout,
            arguments={'table_data': str(argument)}
        ).replace('Some(','').replace(')','')
        print (str(argument['table_name'])+ f': Failed  --  Job# {run_id}\n')
    return argument['table_name']

def upsertToDelta(source, target,UID, update, CD, batchId):
    if CD == "True":
        (target.alias("t").merge(source.alias("s"),f"s.{UID} = t.{UID} and s.{update} = t.{update}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    else:
        (target.alias("t").merge(source.alias("s"),f"s.{UID} = t.{UID}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


def schemaFlatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, StructType):
            fields += schemaFlatten(dtype, prefix=name)
        else:
            fields.append(col(name).alias(name.replace('.','__')))
    return fields

def dataFlatten(microBatchOutputDF, batchId):
    flattened_schema = schemaFlatten(microBatchOutputDF.schema)
    microBatchOutputDF = microBatchOutputDF.select(flattened_schema)
    return microBatchOutputDF

def removebatchduplicates(microBatchOutputDF, UID, update, CD,  batchId):
    if CD == "True":
        window = Window.partitionBy([col(UID), col(update)]).orderBy(col(update).desc())
        microBatchOutputDF = (microBatchOutputDF.withColumn('row', F.row_number().over(window)).filter(col('row') == 1).drop('row'))
    else:
        window = Window.partitionBy(col(UID)).orderBy(col(update).desc())
        microBatchOutputDF = (microBatchOutputDF.withColumn('row', F.row_number().over(window)).filter(col('row') == 1).drop('row'))
    return microBatchOutputDF

def schemaconversion(microBatchOutputDF, schema, batchId):
    for col in microBatchOutputDF.columns:
        column_datatype = schema[col].jsonValue()['type']
        if str(column_datatype)[0:1] == '{':
            column_datatype = schema[col]
            microBatchOutputDF = microBatchOutputDF.withColumn(col,from_json(microBatchOutputDF[col],column_datatype.dataType))
        else:
            microBatchOutputDF = microBatchOutputDF.withColumn(col,F.col(col).cast(column_datatype))
    return microBatchOutputDF



def cctableupdate(source, destination, UID_field, update_field,created_field, transformation_script, cc_script, batchId):

    if source.count() > 0:

        df_destination_table = DeltaTable.forPath(spark, destination)
        df_source = spark.createDataFrame(source.collect(), source.schema)
        df_source.createOrReplaceTempView('view_stream')

        spark.read.load(destination).createOrReplaceTempView('view_destination')

        (spark.sql(f'''
                    select distinct
                    {transformation_script}
                    from view_stream
                    --where
                    group by all
        ''')).createOrReplaceTempView("view_transformed_stream")

        spark.sql(f'''
                    select distinct
                    a.*
                    from view_destination a
                        inner join view_transformed_stream b on a.{UID_field} = b.{UID_field}
                            and a.CURRENT_RECORD_FLAG = True
                    where
                        b.{UID_field} is not null
        ''').createOrReplaceTempView("view_transformed_destination")

        df_update = spark.sql(f'''
                            select distinct
                            a.*
                            ,b.VERSION_NUMBER as VERSION_NUMBER_dest
                            from view_transformed_stream a
                            inner join view_transformed_destination b on a.{UID_field} = b.{UID_field}
                                    and a.{update_field} > b.{update_field}
                                    and ({cc_script})
                            ''')

        df_insert = spark.sql(f'''
                            select distinct
                            a.*
                            from view_transformed_stream a
                            left join view_transformed_destination b 
                            on a.connection_id == b.connection_id
                                where b.connection_id is null
                            ''')

        df_update = df_update.withColumn('CURRENT_RECORD_FLAG', F.row_number().over(Window.partitionBy(UID_field).orderBy(F.desc(update_field))) == 1)

        df_update = df_update.withColumn('VERSION_START_DATE', F.col(update_field))

        df_update = df_update.withColumn('VERSION_END_DATE', F.when(df_update.CURRENT_RECORD_FLAG == True
                                                                    , F.to_timestamp(F.lit('2999-12-31T00:00:00.000+0000')))
                                                                    .otherwise(F.lead(update_field,1).over(Window.partitionBy(UID_field).orderBy(update_field))))

        df_update =(df_update.withColumn("VERSION_NUMBER",df_update.VERSION_NUMBER_dest+(F.row_number().over(Window.partitionBy(UID_field).orderBy(update_field))))
                            .drop('VERSION_NUMBER_dest'))

        df_insert = df_insert.withColumn("VERSION_NUMBER", F.row_number().over(Window.partitionBy(UID_field).orderBy(update_field) ))

        df_insert = df_insert.withColumn('CURRENT_RECORD_FLAG', F.row_number().over(Window.partitionBy(UID_field).orderBy(F.desc(update_field))) == 1)

        df_insert = df_insert.withColumn('VERSION_START_DATE', F.when(df_insert.VERSION_NUMBER == 1
                                                                    , F.col(created_field))
                                                                    .otherwise(F.col(update_field)))

        df_insert = df_insert.withColumn('VERSION_END_DATE', F.when(df_insert.CURRENT_RECORD_FLAG == True
                                                                    , F.to_timestamp(F.lit('2999-12-31T00:00:00.000+0000')))
                                                                    .otherwise(F.lead(update_field,1).over(Window.partitionBy(UID_field).orderBy(update_field))))

        df_merge =    (df_update.withColumn('update_insert', F.lit('U'))
                .union(df_update.withColumn('update_insert', F.lit('I')))
                .union(df_insert.withColumn('update_insert', F.lit('I')))
        )

        df_merge = df_merge.withColumn('oldest_record', F.row_number().over(Window.partitionBy(UID_field, 'update_insert').orderBy(F.asc(update_field))))

        (df_destination_table.alias("t").merge(df_merge.alias("s")
                                                            ,f"""s.{UID_field} = t.{UID_field} 
                                                            and t.CURRENT_RECORD_FLAG = True 
                                                            and s.OLDEST_RECORD = 1 
                                                            and s.UPDATE_INSERT = 'U'""")
                                .whenMatchedUpdate(set =
                                                            {
                                                            f"t.VERSION_END_DATE": f"s.{update_field}",
                                                            "t.CURRENT_RECORD_FLAG": 'False'
                                                            })
                                .whenNotMatchedInsertAll("s.UPDATE_INSERT = 'I'")
                                .execute())
