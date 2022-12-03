from airflow import DAG
import datetime
import os
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_dag_args={
    'owner': "Renuka",
    'start_date': datetime.datetime(2022,9,6),
    'email_on_failure':False,
    'project_id':'ingestion',
    'depends_on_past':False,
    'retries':2
}

dag = DAG(
dag_id="ingestion",
schedule_interval= datetime.timedelta(days=1),
max_active_runs=1,
catchup=False,
default_args=default_dag_args,
tags=['ingestion_project']
)

def get_jars(path: str):
    list_Dir=os.listdir(path)
    list_dirs=[]
    for jar_file in list_Dir:
        if jar_file.split('.')[-1] =='jar':
            list_dirs.append(path+jar_file)
    return ",".join(list_dirs)

config={
    "spark.jars":get_jars('/opt/airflow/jars/jars/')
 }

spark_op=SparkSubmitOperator(
 dag=dag,
 task_id='spark_di',
 conn_id='spark_default',
 application='/opt/airflow/new_fol/test_sc.py',
 conf=config
)

spark_op

if __name__ == "__main__":
    dag.cli()