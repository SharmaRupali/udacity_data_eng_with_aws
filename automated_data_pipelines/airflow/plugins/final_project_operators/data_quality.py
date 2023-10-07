from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        tests=None,
        *args, **kwargs
        ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        self.log.info("Executing data quality checks")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.tests:
            sql_query = test['sql_query']
            expected_result = test['expected_result']

            self.log.info(f"Running data quality check: {sql_query}")
            records = redshift.get_records(sql_query)

            if len(records) < 1 or len(records[0]) < 1:
                raise AirflowException(f"Data quality check failed. No results returned for query: {sql_query}")

            result = records[0][0]
            if result != expected_result:
                raise AirflowException(f"Data quality check failed. Expected result: {expected_result}, Got: {result}")

            self.log.info(f"Data quality check passed for query: {sql_query}")
