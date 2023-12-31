from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        target_table="",
        sql_query="",
        append_only=True,
        *args, **kwargs
        ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql_query = sql_query
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_only:
            self.log.info(f"Deleting Data from the Destination Fact Table {self.target_table}...")
            redshift.run(f"DELETE FROM {self.target_table};")
        
        self.log.info(f"Inserting Data from Staging Tables into the Fact Table {self.target_table}...")

        insert_sql = f"INSERT INTO {self.target_table} \n{self.sql_query};"
        self.log.info(f"Insert Query: \n{insert_sql}")
        redshift.run(insert_sql)
        self.log.info(f"Successfully Inserted Data into the Fact Table {self.target_table}")
