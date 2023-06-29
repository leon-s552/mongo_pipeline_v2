# Databricks notebook source
from multiprocessing.pool import ThreadPool
from shared_functions import *
import json
root_path = '/'.join(json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())['extraContext']['notebook_path'].split('/')[:-2]).strip('/')

table_list = json.loads((open(f"/Workspace/{root_path}/config/enriched_table_master.json", "r").read()))

pool = ThreadPool(len(table_list))
timeout_seconds = 1000


# COMMAND ----------

notebook = "enriched_child"
pool.map(lambda x: notebook_invocation(notebook, timeout_seconds, x), table_list)
