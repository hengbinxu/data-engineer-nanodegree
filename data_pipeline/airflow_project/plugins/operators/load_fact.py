from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    stmt_template = "INSERT INTO {table_name} ({query})"

    @apply_defaults
    def __init__(self,
                conn_id: str,
                table: str,
                query: str,
                *args,
                **kwargs):
        '''
        Load data to specific fact table

        args:
            conn_id(str): the connection id that you set in airflow metadb.
            table(str): the table that you load data
            query(str): the SQL query describe what kind of data load to specific table.
        '''
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        load_stmt = self.stmt_template.format(
            table_name=self.table,
            query=self.query
        )
        redshift.run(load_stmt)
        self.info('Successfully load data to fact table: {}'.format(self.table))

        
