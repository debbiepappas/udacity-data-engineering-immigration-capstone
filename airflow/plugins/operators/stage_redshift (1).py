from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
                       
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table_name = '',
                 s3_bucket = '',
                 s3_key = '',
                 aws_credentials = {},
                 region = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials = aws_credentials
        self.region = region
        

    def execute(self, context):
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
              """.format(self.table_name,s3_path,self.aws_credentials.get('key'),self.aws_credentials.get('secret'),self.region)
        if self.table_name == 'staging_immigr':
            self.log.info('Starting to copy data from S3 to staging_immigr')
            redshift.run(copy_statement_immigr)
            self.log.info('Staging for staging_immigr done')
        if self.table_name == 'staging_demo':
            self.log.info('Starting to copy data from S3 to staging_demo')
            redshift.run(copy_statement_demo)
            self.log.info('Staging for staging_demo') 
                       
                       
                       