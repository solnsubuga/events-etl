from airflow.hooks.postgres_hook import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

STAGING_TABLES_SQL = '''
    COPY {} FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION 'us-west-2'
    FORMAT AS JSON '{}'
'''


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#EEE8AA'

    @apply_defaults
    def __init__(self, *args, redshift_conn_id, aws_credentials_id, s3_path, table, json_path,  **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_path = s3_path
        self.table = table
        self.json_path = json_path

    def execute(self, context):
        self.log.info(
            'StageToRedshiftOperator staging data into table: %s', self.table)
        metastore_backend = MetastoreBackend()
        aws_connection = metastore_backend.get_connection(
            self.aws_credentials_id)

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = STAGING_TABLES_SQL.format(
            self.table,
            self.s3_path,
            aws_connection.login,
            aws_connection.password,
            self.json_path
        )
        redshift.run(formatted_sql)
        self.log.info("Staged data successfully in %s", self.table)
