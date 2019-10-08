from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 
                 redshift_conn_id = '',
                 source_table = '',
                 target_table = '',
                 append_data = '',
                 sql_statement = '',
                 aws_credentials = {},
                 region = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.source_table = source_table
        self.target_table = target_table
        self.append_data = append_data
        self.aws_credentials = aws_credentials
        self.region = region
        self.sql_statement = sql_statement

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f'Loading dimension table: {self.target_table}')
        
        if self.append_data == True:
            #sql_statement = f'INSERT INTO {self.target_table} {self.sql_load}'
            self.log.info(f'INSERT INTO {self.target_table}')
            sql = self.sql_statement
            redshift.run(sql)
        else:
            self.log.info(f'DELETE FROM {self.source_table}')
            self.log.info(f'INSERT INTO {self.target_table}')
            sql = self.sql_statement
            redshift.run(sql)        

        self.log.info(f'Dimension table {self.target_table} load finished')
                 
        
