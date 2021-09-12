from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    stmt_template = "INSERT INTO {table_name} ({query})"

    @apply_defaults
    def __init__(self,
                conn_id: str,
                table: str,
                query: str,
                truncate: bool,
                *args,
                **kwargs):
        '''
        Load data to specific dimension table

        args:
            conn_id(str): the connection id that you set in airflow metadb.
            table(str): the table that you load data
            query(str): the SQL query describe what kind of data load to specific table.
            truncate(bool): truncate table or not before loading data.
        '''
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.query = query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        if self.truncate:
            truncate_stmt = 'truncate table {table_name}'.format(self.table)
            redshift.run(truncate_stmt)
            self.log.info('Successfully truncate {}'.format(self.table))

        load_stmt = self.stmt_template.format(
            table_name=self.table,
            query=self.query
        )
        redshift.run(load_stmt)
        self.info('Successfully load data to dimension table: {}'.format(self.table))

