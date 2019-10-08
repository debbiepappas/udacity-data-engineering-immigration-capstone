from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class CreateTableOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 
                 redshift_conn_id = '',
                 table_name = '',
                 sql_statement = '',
                 aws_credentials = {},
                 region = '',
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.aws_credentials = aws_credentials
        self.region = region
        self.sql_statement = sql_statement

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f'Creating table: {self.table_name}')
        sql = self.sql_statement
        redshift.run(sql)
        self.log.info(f'Table {self.table_name} creation finished')