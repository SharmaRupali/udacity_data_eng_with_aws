from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        target_table="",
        sql_query="",
        insert_type="",
        *args, **kwargs
        ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql_query = sql_query
        self.insert_type = insert_type

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Insert Type: {self.insert_type}")

        if self.insert_type == "append":
            self.log.info(f"Appending Data from the Staging Table into the Dimension Table {self.target_table}...")
        elif self.insert_type == "insert-delete":
            self.log.info(f"Deleting Data from the Destination Dimension Table {self.target_table}...")
            redshift.run(f"DELETE FROM {self.target_table};")
            self.log.info(f"Inserting Data from the Staging Table into the Dimension Table {self.target_table}...")
        
        insert_sql = f"INSERT INTO {self.target_table} \n{self.sql_query};"
        self.log.info(f"Insert Query: \n{insert_sql}")
        redshift.run(insert_sql)
        self.log.info(f"Successfully Inserted Data into the Dimension Table {self.target_table}")
