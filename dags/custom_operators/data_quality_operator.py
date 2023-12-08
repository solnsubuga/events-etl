from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, *args, conn_id, tables, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        for table in self.tables:
            records = redshift_hook.get_records(
                f'SELECT COUNT(*) from {table}')

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f'Data quality check failed. {table} return no results')

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(
                    'Data quality check failed for table {table}')
            self.log.info(
                'Data quality checks on table %s passed with %s records', table, records[0][0])
