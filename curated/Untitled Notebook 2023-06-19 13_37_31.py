# Databricks notebook source
from delta.tables import *
from pyspark.sql.window import *
import pyspark.sql.functions as F

storage_account_name = "stdataplatform2303dev"
container_name = 'data'
structured_zone = 'structured'
enriched_zone = 'enriched'
enriched_table = 'DIM_CONNECTION'

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    dbutils.secrets.get(scope="key-vault-secrets", key="storage-account-accesskey"))

# COMMAND ----------

enriched_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{enriched_zone}/{enriched_table}"

enriched_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{enriched_zone}/{enriched_table}/_checkpoint"




# COMMAND ----------

spark.read.load(enriched_path).createOrReplaceTempView("view_enriched_connection")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_date

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from view_enriched_connection
# MAGIC select distinct application, platform, usage from view_enriched_connection 

# COMMAND ----------

display(
spark.sql('''
          with cte as (
          select distinct yyyymm,  connection_id, month,year, connection_status, application

          from view_enriched_connection a
          left join dim_date b
          on date_add(last_day(add_months(b.date, -1)),1) between
          date_add(last_day(add_months(version_start_date, -1)),1) 
          and  date_add(last_day(add_months(version_end_date, -1)),1)  
          where yyyymm < '202307'   

          qualify row_number() over (partition by connection_id, yyyymm order by
          case when connection_status = 'ACTIVE' then 1 
              when connection_status = 'NOT_CONNECTED' then 2 
              else row_number() over (partition by connection_id
                                                    , yyyymm
                                    order by modified_datetime ) + 2 end
          ) = 1
          )
          select yyyymm, count( distinct connection_id), application,month,year, connection_status
          from cte
            group by 
          month,year, connection_status
          ,yyyymm, application
            order by year desc, month desc, connection_status
          ''')
)


# COMMAND ----------


