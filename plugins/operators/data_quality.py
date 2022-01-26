from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=[],
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.test_class = tests[0]
        self.test_names = tests[1]

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for test in self.tests:
            test.records = redshift_hool.get_records(test.sql)
            test.validate()
            
        