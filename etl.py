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

