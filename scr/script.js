# [START tutorial]
from datetime import timedelta

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import sqlalchemy as sa
from airflow.utils.dates import days_ago

import pandas as pd
import pymssql
import boto3
import csv


# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    'script-vindi',
    default_args=default_args,
    description='Script-vindi',
    schedule_interval=None
)

engine=sa.create_engine("mssql+pymssql://admin:M8Y8hoy1qRwACaogFRq4@auditybera.ctycq26qua6l.us-east-2.rds.amazonaws.com:1433/teste_ybera?charset=utf8")


query = 'SELECT * from Vindi'
dt = pd.read_sql_query(query,engine)
print(dt)



s3 = boto3.client(
    's3',
    aws_access_key_id='AKIAYLEZF2QCMIPTQS67',
    aws_secret_access_key='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',
    region_name='us-east-1'
) #1

obj = s3.get_object(Bucket='etlpydata', Key='sergio_teste/Vindi.csv') #2
data = obj['Body'].read().decode('utf-8').splitlines() #3
records = csv.reader(data) #4
headers = next(records) #5
print('headers: %s' % (headers)) 
for eachRecord in records: #6
    print(eachRecord)
