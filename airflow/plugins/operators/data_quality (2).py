from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 dq_checks='',
                 aws_credentials = {},
                 region = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.region = region
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info(f'Starting data quality check on Fact and Dimension tables')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        count_error = 0
        not_passed_check = []
        for line in self.dq_checks:
            sql = line.get('check_sql')
            expected_result = line.get('expected_result')
            records = redshift.get_records(sql)[0]
            if expected_result != records[0]:
                count_error = count_error + 1
                not_passed_check.append(sql)
        if count_error > 0:
            self.log.info(f'failing tests: {" ".join(str(x) for x in not_passed_check)}')
            raise ValueError('Data quality checks failed')
        else:
            self.log.info('Data quality checks passed!')
            
            
            
                    