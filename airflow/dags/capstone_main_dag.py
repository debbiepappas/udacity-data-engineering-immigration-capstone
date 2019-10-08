import datetime
import logging
from datetime import timedelta
#from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator 

from helpers.sql_queries import SqlQueries

# the following credentials were used in DAG connections to pull in AWS access and secret key
# for the airflow_redshift_user created in IAM users.
aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()

# the access and secret keys are set in the variables below
AWS_KEY = credentials.access_key
AWS_SECRET = credentials.secret_key


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 2,
    'catchup': False,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG('capstone_main_dag',
          default_args=default_args,
          start_date = datetime.datetime.now() - datetime.timedelta(days=1),
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_immigr_to_redshift = StageToRedshiftOperator(
    task_id='Stage_immigr',
    dag=dag,
    table_name = 'staging_immigr',
    redshift_conn_id = 'redshift',
    s3_bucket = 'capstone-bucket-immigr',
    s3_key = 'staging_immigr.csv',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    provide_context = True
)

stage_demo_to_redshift = StageToRedshiftOperator(
    task_id='Stage_demo',
    dag=dag,
    table_name = 'staging_demo',
    redshift_conn_id = 'redshift',
    s3_bucket = 'capstone-bucket-demo',
    s3_key = 'demo',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1'
)    

load_pleasurevisits_table = LoadFactOperator(
    task_id='Load_pleasurevisits_fact_table',
    dag=dag,
    source_table = 'pleasurevisits',
    target_table = 'pleasurevisits',
    redshift_conn_id = 'redshift',
    append_data = True,
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.pleasurevisits_table_insert, 
    provide_context = True
)

load_flights_dimension_table = LoadDimensionOperator(
    task_id='Load_flights_dim_table',
    dag=dag,
    target_table = 'flights',
    redshift_conn_id = 'redshift',
    append_data = False,
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.flights_table_insert, 
    provide_context = True
)


load_cities_dimension_table = LoadDimensionOperator(
    task_id='Load_cities_dim_table',
    dag=dag,
    target_table = 'cities',
    redshift_conn_id = 'redshift',
    append_data = False,
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.cities_table_insert, 
    provide_context = True
)

load_visitors_dimension_table = LoadDimensionOperator(
    task_id='Load_visitors_dim_table',
    dag=dag,
    target_table = 'visitors',
    redshift_conn_id = 'redshift',
    append_data = False,
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.visitors_table_insert, 
    provide_context = True
)


load_arrival_dimension_table = LoadDimensionOperator(
    task_id='Load_arrival_dim_table',
    dag=dag,
    target_table = 'arrival',
    redshift_conn_id = 'redshift',
    append_data = False,
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.arrival_table_insert, 
    provide_context = True
)

load_departure_dimension_table = LoadDimensionOperator(
    task_id='Load_departure_dim_table',
    dag=dag,
    target_table = 'departure',
    redshift_conn_id = 'redshift',
    append_data = False,
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-east-1',
    sql_statement = SqlQueries.departure_table_insert, 
    provide_context = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    provide_context = True,
    dq_checks = [
        { 'check_sql': "SELECT COUNT(*) FROM pleasurevisits WHERE pleasurevisit_id is null", 'expected_result': 0},
        { 'check_sql': "SELECT COUNT(*) FROM flights WHERE flight_num is null", 'expected_result': 0},
        { 'check_sql': "SELECT COUNT(*) FROM cities WHERE city is null", 'expected_result': 0},
        { 'check_sql': "SELECT COUNT(*) FROM visitors WHERE adm_num is null", 'expected_result': 0},
        { 'check_sql': "SELECT COUNT(*) FROM arrival WHERE arrival_date is null", 'expected_result': 0},
        { 'check_sql': "SELECT COUNT(*) FROM departure WHERE dep_date is null", 'expected_result': 0}
    ]
)



end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#start_operator >> stage_demo_to_redshift >> load_pleasurevisits_table
#start_operator >> stage_immigr_to_redshift >> load_pleasurevisits_table
start_operator >> load_pleasurevisits_table
load_pleasurevisits_table >> load_flights_dimension_table >> run_quality_checks  
load_pleasurevisits_table >> load_cities_dimension_table >> run_quality_checks 
load_pleasurevisits_table >> load_visitors_dimension_table >> run_quality_checks
load_pleasurevisits_table >> load_departure_dimension_table >> run_quality_checks
load_pleasurevisits_table >> load_arrival_dimension_table >> run_quality_checks
run_quality_checks >> end_operator   

