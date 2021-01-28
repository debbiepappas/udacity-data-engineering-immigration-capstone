# udacity-data-engineering-immigration-capstone
# IMMIGRATION CAPSTONE PROJECT

### In this project two datasets are accessed to create a star schema with fact and dimension tables. An ETL pipeline is built using python, postgresql , redshift and airflow with an analytic focus. The star schema is optimized for immigrants who came to the US for a temporary stay for pleasure. 

## <u>Datasets</u>

### Immigration

1. The immigration data set in SAS format from April 2016 titled *'i94_apr16_sub.sas7bdat'* consists of data for temporary visits to the US from all over the world. The file titled *'I94_SAS_Labels_Descriptions.SAS'* has an explaination of codes used in the immigration dataset. The immigration dateset has around a million rows. The original dataset has the following columns with the number of rows listed for each column. 

![immigration data](https://capstone-bucket-jpeg.s3.amazonaws.com/immigration_data.jpg)

### Demographics

2. The file titled *'us-cities-demographics.csv'* consists of data in csv format. It lists cities and various demographics such as city, state, median age, male population, female population, total population, veterans, foreign-born, average house size, state code, race, count

![demographics](https://capstone-bucket-jpeg.s3.amazonaws.com/us_demo3.jpg)

## <u>List of python scripts</u>

- In "/home/workspace"
    - etl.py
    - dl.cfg
- In "/home/workspace/airflow/dags"
    - capstone_create_table_dag.py
    - capstone_main_dag.py
- In "/home/workspace/airflow/plugins/helpers"
    - sql_queries.py
- In "/home/workspace/airflow/plugins/operators"    
    - create_table.py
    - data_quality.py
    - load_dimension.py
    - load_fact.py
    - stage_redshift.py


## <u>Staging Tables and Star Schema for Pleasure Visits Analysis</u>

Fist the **staging tables** were created from the **immigration** and **demographics** datasets. 

![staging-tables](https://capstone-bucket-jpeg.s3.amazonaws.com/immigration+staging.png)



The star schema with **fact** table ***pleasurevisits*** and **dimension** tables ***arrival, departure, visitors, flights, and cities*** shown below is optimized for analysis on immigrants visiting the US for pleasure.

![schema](https://capstone-bucket-jpeg.s3.amazonaws.com/schema2.png)


## <u>ETL Process</u>

#### 1. The ***etl.py*** file shown below is run to gather and clean the immigration and demographics data sets and prepare staging files for Redshift. 

- The *'dl.cfg'*  file is created with the secret and access keys to load files in S3.


- **The following is done for the immigration dataset :** 

    A. The dataset is converted to CSV format from SAS using python.
    
    B. The columns that are not relevant to the pleasure visits data model are dropped. The remaining columns are renamed to become more understandable.
    
    C. The null values are dropped from all columns.
    
    D. The columns are converted to their appropriate data types and the SAS  date formats are converted to pandas date fromats.
    
    E. The final *'staging_immigr.csv'* is loaded in the *'capstone-bucket-immigr'* S3 bucket.
    
    
- **The following is done for the demographics dataset :** 

    A. The csv dataset is set to the correct deliminator (,). 
    
    B. The null values are dropped from all columns.
    
    C. The columns are converted to their appropriate data types
    
    D. Each row in the column is converted to a json file with prefix *'demo'* and loaded in the *'capstone-bucket-demo'* S3 bucket.
    
- The 'staging_immigr.csv' staging file on S3 contains the following columns : *     

---

```
""" import all necessary pandas libraries for capstone project """ 
import sys
import glob
import pandas as pd
import numpy as np
import json
import boto3
import configparser
import datetime
import os
from os import path


""" Use the 'dl.cfg' file to get the access and secret key for reading and writing to S3 """
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def process_immigration_data():
    
    """
    The process_immigration_data function will import the immigration
    dataset in SAS data format, run a data quality test by dropping null values, convert dataset to appropriate
    data types and create a staging_immigr csv file which is then uploaded to S3.
    """
    
    """ Read immigration data for april in SAS format """
    fname = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    df_immig_apr = pd.read_sas(fname, 'sas7bdat', encoding="ISO-8859-1")
    
    """ convert SAS immigration data to csv and do not include a column for index """
    df_immig_apr.to_csv(r'/home/workspace/immig_apr.csv', index=False)
    
    """ read immigration csv file and replace 'not available' values with empty string """
    df_immig_all = pd.read_csv('/home/workspace/immig_apr.csv', na_values='', dtype=str, low_memory = False)


    """ drop columns ( i94cit','count','dtadfile','visapost','occup','entdepa', 'entdepd', 'entdepu',
    'matflag','biryear', 'dtaddto', 'insnum') that are not being used for the data model. """
    df_immig2 = df_immig_all.drop(['i94cit','count','dtadfile','visapost','occup','entdepa', 'entdepd', 'entdepu',
                              'matflag','biryear', 'dtaddto', 'insnum'], axis=1)


    """ rename the columns in the immigration dataframe """
    df_immig2.rename(columns = {'cicid' : 'cic_id', 'i94yr' : 'year', 'i94mon':'month','i94res':'country','i94port':'us_port',
                            'arrdate': 'arrival_date', 'i94mode' : 'travel_mode', 'i94addr':'us_state', 'depdate':'dep_date',
                            'i94bir': 'age', 'i94visa':'visa_code',
                            'biryear':'birth_year','admnum':'adm_num',
                            'fltno':'flight_num'}, inplace=True)


    """ list the count of rows of all columns before dropping null values """
    before_immig_count_list = df_immig2.count().tolist()
    print("Number of rows in each column before dropping null values in \
    immigration dataset = \n {}".format(before_immig_count_list))
    
    """ count rows with null values in immigration dataframe """
    df_immig2_isnull = df_immig2[df_immig2.isnull().any(axis=1)]
    df_immig2_isnull_list = df_immig2_isnull.count().tolist()
    print(" \n Number of rows in each column with null values in immigration \
    dataset = \n {}".format(df_immig2_isnull_list))


    """ drop all null values in the immigration dataframe """
    df_immig2.dropna(inplace=True)
    df_immig2.reset_index(drop=True, inplace=True)

    """ list the count of rows of all columns after dropping null values """
    after_immig_count_list = df_immig2.count().tolist()
    print(" \n Number of rows in each column after dropping null values in immigration \
    dataset = \n {}".format(after_immig_count_list))

    """ list the original datatypes of all columns """ 
    before_immig_datatype_list = df_immig2.dtypes.tolist()
    print(" \n List the original datatype of columns in immigration dataset = \n {}".format(before_immig_datatype_list))


    """ list the columns to be converted to integer datatype """
    cols = ['cic_id', 'year', 'month', 'country', 'arrival_date', 'dep_date', 'age', 'adm_num', 'travel_mode', 'visa_code' ]


    """ first convert columns to float """
    df_immig2[cols] = df_immig2[cols].applymap(np.float64)


    """ convert columns to integer """
    df_immig2[cols] = df_immig2[cols].applymap(np.int64)


    """ Convert arrival date from SAS to pandas date datatype format """
    df_immig2['arrival_date'] = pd.to_timedelta(df_immig2['arrival_date'], unit='D') + pd.Timestamp('1960-1-1')


    """ Convert departure date from SAS to pandas date datatype format """
    df_immig2['dep_date'] = pd.to_timedelta(df_immig2['dep_date'], unit='D') + pd.Timestamp('1960-1-1')


    """ replace visa_code and and travel_mode with the proper code in string value """
    replace_values_code = { 1.0 : 'Business', 2.0 : 'Pleasure', 3.0 : 'Student' }
    replace_values_mode = {1.0 : 'Air', 2.0 : 'Sea' , 3.0 : 'Land', 9.0 : 'Not reported'}
    df_immig3 = df_immig2.replace({"visa_code": replace_values_code, "travel_mode" : replace_values_mode})                   


    """ list the datatypes of all columns in immigration dataset after the datatype conversion """
    after_immig_datatype_list = df_immig3.dtypes.tolist()
    print(" \n List the dataypes in the immigration dataset after datatype conversion = \n {}".format(after_immig_datatype_list))

    df_immig3.to_csv(r'/home/workspace/immig_apr_final.csv', index=False)

    """ load the immigration data file to s3 bucket """
    s3 = boto3.client('s3')
    s3.upload_file("/home/workspace/immig_apr_final.csv", 'capstone-bucket-immigr', 'staging_immigr.csv')

    
def process_demographics_data():
    
    """
    The process_demographics_data function will import the demographics
    dataset in csv data format, run a data quality test by dropping null values, convert dataset to appropriate
    data types and create a staging_dem csv file. Each row will then be converted into a json file
    and then all json files will be uploaded to S3.
    """
    
    
    df_dem1 = pd.read_csv('/home/workspace/us-cities-demographics.csv')

    #split the header with semicolon deliminator to columns
    df_dem2 = df_dem1['City;State;Median Age;Male Population;Female Population;Total Population;\
Number of Veterans;Foreign-born;Average Household Size;State Code;Race;Count'].str.split(';', 12, expand=True)\
.rename(columns={0:'city', 1:'state_long',  2:'median_age', 3:'male_pop', 4:'female_pop', 5:'total_pop', 6:'veterans',\
                7:'foreign_born', 8:'house_size', 9:'state', 10:'race', 11:'race_count'})
    
    # drop 'state_long' which will not be used in data model
    df_dem2 = df_dem2.drop(columns=['state_long'], axis=1)

    df_dem2.to_csv(r'/home/workspace/dem.csv', index=False)

    # read demographics csv file with 'not available' values as empty and datatype as string
    df_dem2 = pd.read_csv('/home/workspace/dem.csv', na_values='', dtype=str)                    

    # list the number of rows of all columns before dropping null values
    before_dem2_count_list = df_dem2.count().tolist()
    print(" \n Number of rows in each column before dropping null values in demographics dataset = \n {}".format(before_dem2_count_list))

    # count rows with null values in demographics dataframe
    df_dem2_isnull = df_dem2[df_dem2.isnull().any(axis=1)]
    df_dem2_isnull_list = df_dem2_isnull.count().tolist()
    print(" \n Number of rows in each column with null values in demographics dataset = \n {}".format(df_dem2_isnull_list))

    # drop all null values in the demographics dataframe    
    df_dem2 = df_dem2.dropna()
    
    # list the count of rows of all columns after dropping null values
    after_dem2_count_list = df_dem2.count().tolist()
    print(" \n Number of rows in each column after dropping null values in immigration dataset = \n {}".format(after_dem2_count_list))

    # list the original datatypes of all columns in demographics dataframe
    before_dem2_datatype_list = df_dem2.dtypes.tolist()
    print(" \n List the original datatype of columns in demographics dataset = \n {}".format(before_dem2_datatype_list))

    # list columns that should be integer
    cols = ['median_age', 'male_pop','female_pop','total_pop', 'veterans','foreign_born', 'house_size','race_count']

    # first convert column datatypes to float 
    df_dem2[cols] = df_dem2[cols].applymap(np.float64)

    # convert columns datatypes to integer
    df_dem2[cols] = df_dem2[cols].applymap(np.int64)

    # list the datatypes of all columns in the demographics dataset after the datatype conversion
    after_dem2_datatype_list = df_dem2.dtypes.tolist()
    print(" \n List the dataypes in the demographics dataset after datatype conversion = \n {}".format(after_dem2_datatype_list))

    df_dem2.to_json('/home/workspace/dem_final.json')

    staging_dem = pd.read_json('/home/workspace/dem_final.json')
    
    # create demographics directory if it does not already exist:

    if not os.path.exists("/home/workspace/demographics")
        os.mkdir("/home/workspace/demographics")
    
    # use for loop to write each row to a json file in demographics direcotory on local machine
    s3 = boto3.client('s3')
    for i in staging_dem.index:
        staging_dem.loc[i].to_json("demographics/demo{}.json".format(i))

    # use for loop to load json files to bucket on s3
    for i in staging_dem.index:
        s3.upload_file("/home/workspace/demographics/demo{}.json".format(i), 'capstone-bucket-demo', 'demo{}.json'.format(i))


def main():
    """
    The main function will call the process_immigration_data and process_demographics_data functions.  
    """
    
    process_immigration_data()    
    process_demographics_data()


if __name__ == "__main__":
    main()
```
---

- The output generated from running ***etl.py*** is csv and json data sources for the staging_immigr and staging_demo tables in Redshift. The staging_immigr.csv file and a single demo0.json file is shown below. 

![staging_immigr](https://capstone-bucket-jpeg.s3.amazonaws.com/staging_immigr.jpg)

![staging_demo](https://capstone-bucket-jpeg.s3.amazonaws.com/staging_demo.jpg)


#### 2. At the AWS console the airflow IAM role with the policies shown below is created. 

![IamRole](https://capstone-bucket-jpeg.s3.amazonaws.com/iam_role.jpg)


#### 3. The Redshift cluster is launched which will be used by Airflow to analyze the datasets. Redshift is a data warehouse technology which uses Postgresql. After clicking on 'quick launch cluster' the information shown below is entered. 


![redshift](https://capstone-bucket-jpeg.s3.amazonaws.com/redshift.jpg)

#### 4. In a system where **Airflow** is installed the ***capstone_create_table_dage.py*** file is created in directory '/home/workspace/airflow/dags'. This DAG does the following:

- The class **CreateTableOperator** which is defined in the  ***create_table.py*** file is used to create the following tables :  *staging_demo, staging_immigr, pleasurevisits, visitors, arrival, departure, flights, and cities* .

- For example, the code for the task to call the *staging_immigr* CreateTableOperataor is :
---
`create_staging_immigr_table = CreateTableOperator(`
    `task_id='Stage_immigr_create',`    
    `dag=dag,`    
    `table_name = 'staging_immigr',`
    `redshift_conn_id = 'redshift',`    
    `aws_credentials = {`    
     `'key' : AWS_KEY,`    
     `'secret' : AWS_SECRET`    
    `},`    
    `region = 'us-east-1',`
    `sql_statement = SqlQueries.staging_immigr_table_create,` 
    `provide_context = True)`        
    
- the tables are created by passing the sql_statement variable :
`sql_statement = SqlQueries.table_create` from the DAG.
Example of the create table code is :
---
```flights_table_create = ("""
        DROP TABLE IF EXISTS public.flights;
        CREATE TABLE IF NOT EXISTS flights (     
            flight_num varchar(256) NOT NULL,
	        airline varchar(256),
            us_port varchar(256),
            us_state varchar(256),
            year int4,
	        CONSTRAINT flights_pkey PRIMARY KEY (flight_num) )
    """)    
```
---
    
#### 5. The command */opt/airflow/start.sh* is entered at the terminal to start airflow and *launch airflow* button is clicked.

#### 6. First create Airflow user at AWS console and copy the access and secret keys.

![airflow_user](https://capstone-bucket-jpeg.s3.amazonaws.com/airflow_user.png)

#### 7. Then, at the Airflow GUI two connection ids are added with the corresponding values

***('Admin' -> 'Connections' -> 'Create')***

![airflow_connect](https://capstone-bucket-jpeg.s3.amazonaws.com/airflow_connect.jpg)

- Connection Id : aws_credentials    
  Connection Type : Amazon Web Services    
  login : ***your access key ID from airflow user***    
  password : ***your secret key from airflow user***   
  
  
- Connection Id: redshift    
  Connection Type : Postgres    
  Host : ***the host endpoint for the cluster without port at end***    
  Schema : dev    
  Login : awsuser    
  Password : ***password for awsuser***    
  Port : 5439

#### 7. The *capstone_create_table_dag* is started manually with no schedule to create the fact and dimenstion tables.

***(switch to 'on' -> click 'start' under 'links')***

![dag_home](https://capstone-bucket-jpeg.s3.amazonaws.com/airflow_home.jpg)

#### 8. The *capstone_main_dag* is started manually to do the following :
- **"airflow/plugins/operators/stage_redshift.py"** is used to copy S3 csv and json data into the staging tables in Redshift. 
Example code :
---
``` def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Loading stage table {self.table_name}')
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket,rendered_key)
        print(s3_path)
        copy_statement_immigr = """
                    copy {}
                    from '{}'
                    access_key_id '{}'
                    secret_access_key '{}'
                    region as '{}'
                    CSV
                    IGNOREHEADER 1
                    dateformat 'auto'
              """.format(self.table_name,s3_path,self.aws_credentials.get('key'),
                         self.aws_credentials.get('secret'),self.region)
        copy_statement_demo = """
                    copy {}
                    from '{}'
                    access_key_id '{}'
                    secret_access_key '{}'
                    region as '{}'
                    JSON 'auto' 
```
---

 -  **"load_dimension.py"** and **"load_fact.py"** is used to insert data into the **Fact** ***(pleasurevisits)*** and **Dimenstion** ***(visits, flights, arrival, departure,cities)*** tables from the **Staging** ***(staging_immigr, staging_demo)*** tables. 
For example the following code shows insert query for the flight table with primary key as **'distinct'** to eliminate duplicate rows. 
---
```
flights_table_insert = ("""
        TRUNCATE TABLE public.flights ;
        INSERT INTO public.flights
        SELECT distinct flight_num, airline, us_port, us_state, year
        FROM staging_immigr
        WHERE visa_code = 'Pleasure'
    """)
```
--- 

- **"data_quality.py"** checks for any primary keys that are null in the fact and dimension tables. 
    For example this will check the primary keys for null values for **pleasurevisits** table and the script will print **"passed"** or **"failed"** message on the screen. 
---    
```    
dq_checks = [
        { 'check_sql': "SELECT COUNT(*) FROM pleasurevisits WHERE pleasurevisit_id is null", 'expected_result': 0},    
```    
---    

#### 9. The diagram below shows the **graph view** of the tasks involved in **capstone_main_dag**.

![graph](https://capstone-bucket-jpeg.s3.amazonaws.com/capstone_graph.jpg)

#### 10. After **capstone_main_dag** DAG successfully finishes the **Fact** and **Dimension** tables will contain data where sql queries can be run.

For example :

- QUERY 1 : List the **most populated cities** in the US where immigrants visit for **pleasure**.
   
![query1](https://capstone-bucket-jpeg.s3.amazonaws.com/query1.jpg)    
    
## <u>Solutions for Different Scenarios</u>

#### 1. The data was increased by 100x

- For large dataset Spark can be used to write parquet files to S3. The parquet files can then be copied over to Redshift. 

##### Spark code for writing parquet files : 
---
```
# write staging_immigr table to parquet files partitioned by by year and us_state to #directory named 'immigr'
output_bucket = '/home/workspace/'
output_bucket = 's3a://capstone-bucket2019/'
output_immigr = os.path.join(output_bucket, 'immigr')
staging_immigr.write.partitionBy('year', 'us_state').parquet(output_immigr)
```
---

- For large dataset also use Redshift Spectrum which enables you to run queries against exabytes of data in Amazon S3. There is no loading or ETL required. 


#### 2. The pipelines would be run on daily basis by 7am every day.

- The **capstone_main_dag** will contain the following entry : 
---
```
dag = DAG('capstone_main_dag',
          default_args=default_args,
          start_date = datetime.datetime.now() - datetime.timedelta(days=1),
          description='Load and transform data in Redshift with Airflow',
          schedule_interval = '7 0 * * *'
        )
```
---

#### 3. The database needed to be accessed by 100+ people. 
    
- If there are many users for a database, Redshift can be used for faster queries . The following is a list of the Redshift benefits :     
    - Redshift uses column-based technology which requires far fewer I/Os, greatly improving query performance    
    - Redshift automatically distributes data and query load across all nodes (massively paralled processing)    
    - Redshift performs high-quality data compression. Columnar data stores can be compressed much more than row-based data stores.     
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
