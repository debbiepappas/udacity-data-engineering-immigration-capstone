# import libraries for create table dag
import datetime
import logging
from datetime import timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.create_table import CreateTableOperator

from helpers.sql_queries import SqlQueries
# from helpers import SqlQueries

# the following credentials were used in DAG connections to pull in AWS access and secret key
# for the airflow_redshift_user created in IAM users.
aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()

# the access and secret keys are set in the variables below
AWS_KEY = credentials.access_key
AWS_SECRET = credentials.secret_key

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 2,
    'catchup': False,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG('capstone_create_table_dag',
          default_args=default_args,
          start_date = datetime.datetime.now() - datetime.timedelta(days=1),
          description='Create Tables in Redshift',
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
    
create_staging_immigr_table = CreateTableOperator(
    task_id='Stage_immigr_create',
    dag=dag,
    table_name = 'staging_immigr',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.staging_immigr_table_create, 
    provide_context = True
)

create_staging_demo_table = CreateTableOperator(
    task_id='Stage_demo_create',
    dag=dag,
    table_name = 'staging_demo',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.staging_demo_table_create,
    provide_context = True
)


create_pleasurevisits_table = CreateTableOperator(
    task_id='Pleasurevisits_create',
    dag=dag,
    table_name = 'pleasurevisits',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.pleasurevisits_table_create,
    provide_context = True
)

create_flights_table = CreateTableOperator(
    task_id='Flights_create',
    dag=dag,
    table_name = 'flights',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.flights_table_create,
    provide_context = True
)

create_cities_table = CreateTableOperator(
    task_id='Cities_create',
    dag=dag,
    table_name = 'cities',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.cities_table_create,
    provide_context = True
)

create_visitors_table = CreateTableOperator(
    task_id='Visitors_create',
    dag=dag,
    table_name = 'visitors',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.visitors_table_create,
    provide_context = True
)

create_arrival_table = CreateTableOperator(
    task_id='Arrival_create',
    dag=dag,
    table_name = 'arrival',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.arrival_table_create,
    provide_context = True
)

create_departure_table = CreateTableOperator(
    task_id='Departure_create',
    dag=dag,
    table_name = 'departure',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.departure_table_create,
    provide_context = True
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)    
    
start_operator >> create_staging_immigr_table >> end_operator    
start_operator >> create_staging_demo_table >> end_operator     
start_operator >> create_pleasurevisits_table >> end_operator     
start_operator >> create_flights_table >> end_operator     
start_operator >> create_cities_table >> end_operator     
start_operator >> create_visitors_table >> end_operator     
start_operator >> create_arrival_table >> end_operator 
start_operator >> create_departure_table >> end_operator 

       