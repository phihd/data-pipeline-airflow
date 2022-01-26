from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'
    # a templated field that allows the operator to load timestamped files from S3 based on the execution time and run backfills
    template_fields = ('s3_key',)
    copy_sql = '''
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {} REGION '{}'
    '''
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 table='',
                 data_format='',
                 truncate=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket =  s3_bucket
        self.s3_key = s3_key 
        self.region = region 
        self.table = table
        self.data_format = data_format
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        if self.truncate:
            self.log.info('Truncating Redshift table')
            redshift.run(f'TRUNCATE {self.table}')
            
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_key}'
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.data_format,
            self.region
        )
        redshift.run(formatted_sql)

