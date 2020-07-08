from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    dim_sql_template = """
        INSERT INTO {table}
        {insert_sql}
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="redshift",
                 table="",
                 insert_sql="",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        truncate_table = self.truncate_table
        if (truncate_table):
            self.log.info(f"Truncating data from {self.table} [destination Redshift dimension table]")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info(f"Loading data to {self.table} [destination Redshift dimension table]")
        dim_sql = LoadDimensionOperator.dim_sql_template.format(
            table=self.table,
            insert_sql=self.insert_sql
        )
        redshift.run(dim_sql)