from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, *args, conn_id, table, select_statement, append=False, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.select_statement = select_statement
        self.append_only = append

    def execute(self, context):
        self.log.info('Loading fact table: %s', self.table)
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        if not self.append_only:
            self.log.info('Deleting all records in %s', self.table)
            redshift.run(f'DELETE FROM {self.table}')
        new_query = f'''
         INSERT INTO {self.table}
         {self.select_statement}
        '''
        self.log.info('Running query: %', new_query)
        redshift.run(new_query)
        self.log.info(
            'Successfully loaded data into fact table: %s', self.table)
