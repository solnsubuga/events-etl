from datetime import timedelta
import pendulum
import logging
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

from custom_operators.stage_redshift_operator import StageToRedshiftOperator
from custom_operators.load_fact_operator import LoadFactOperator
from custom_operators.load_dimension_operator import LoadDimensionOperator
from custom_operators.data_quality_operator import DataQualityOperator

from common.sql_statements import SqlQueries, create_table_sql


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'email_on_failure': False,
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    bucket = Variable.get('s3_final_bucket')
    log_prefix = Variable.get('s3_log_data_prefix')
    event_prefix = Variable.get('s3_song_data_prefix')
    aws_credential_id = 'aws_credentials'

    song_data_s3 = f's3://{bucket}/{event_prefix}/'
    log_data_s3 = f's3://{bucket}/{log_prefix}/'
    metadata_path = f's3://{bucket}/log_json_path.json'

    start_operator = DummyOperator(task_id='Begin_execution')
    end_operator = DummyOperator(task_id='End_execution')

    @task
    def create_all_tables():
        redshift_hook = PostgresHook('redshift')
        redshift_hook.run(create_table_sql)
        logging.info('Create all required tables in redshift')

    create_tables_in_redshift = create_all_tables()

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id=aws_credential_id,
        s3_path=log_data_s3,
        json_path=metadata_path,
        table='staging_events'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id=aws_credential_id,
        s3_path=song_data_s3,
        json_path='auto',
        table='staging_songs'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        conn_id='redshift',
        table='songplays',
        select_statement=SqlQueries.songplay_table_insert,

    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        conn_id='redshift',
        table='users',
        select_statement=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        conn_id='redshift',
        table='song',
        select_statement=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        conn_id='redshift',
        table='artist',
        select_statement=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        conn_id='redshift',
        table='time',
        select_statement=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        conn_id='redshift',
        tables=['songplays', 'users', 'song', 'artist', 'time']
    )

    start_operator >> create_tables_in_redshift >> [
        stage_events_to_redshift,
        stage_songs_to_redshift
    ]

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()
