from datetime import datetime, timedelta
import pendulum
import os

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.helpers import chain

from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

from udacity.common import final_project_sql_statements, final_project_create_statements


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():

    start_operator = DummyOperator(task_id='begin_execution')

    create_stage_events = PostgresOperator(
        task_id='create_staging_events_table',
        postgres_conn_id='redshift',
        sql=final_project_create_statements.SqlQueries.staging_events_table_create,
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_path='s3://udacity.rugo.automated.pipelines/log-data',
        json_path='s3://udacity.rugo.automated.pipelines/log_json_path.json',
        region='us-west-2',
    )

    create_stage_songs = PostgresOperator(
        task_id='create_staging_songs_table',
        postgres_conn_id='redshift',
        sql=final_project_create_statements.SqlQueries.staging_songs_table_create,
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_path='s3://udacity.rugo.automated.pipelines/song-data',
        json_path='auto',
        region='us-west-2',
    )

    create_songplays = PostgresOperator(
        task_id='create_songplays_table',
        postgres_conn_id='redshift',
        sql=final_project_create_statements.SqlQueries.songplays_table_create,
    )

    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_fact_table',
        redshift_conn_id='redshift',
        target_table='songplays',
        sql_query=final_project_sql_statements.SqlQueries.songplay_table_insert,
        append_only=True,
    )

    create_users = PostgresOperator(
        task_id='create_users_table',
        postgres_conn_id='redshift',
        sql=final_project_create_statements.SqlQueries.users_table_create,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dim_table',
        redshift_conn_id='redshift',
        target_table='users',
        sql_query=final_project_sql_statements.SqlQueries.user_table_insert,
        insert_type='insert-delete',
    )

    create_songs = PostgresOperator(
        task_id='create_songs_table',
        postgres_conn_id='redshift',
        sql=final_project_create_statements.SqlQueries.songs_table_create,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song_dim_table',
        redshift_conn_id='redshift',
        target_table='songs',
        sql_query=final_project_sql_statements.SqlQueries.song_table_insert,
        insert_type='insert-delete',
    )

    create_artists = PostgresOperator(
        task_id='create_artists_table',
        postgres_conn_id='redshift',
        sql=final_project_create_statements.SqlQueries.artist_table_create,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dim_table',
        redshift_conn_id='redshift',
        target_table='artists',
        sql_query=final_project_sql_statements.SqlQueries.artist_table_insert,
        insert_type='insert-delete',
    )

    create_time = PostgresOperator(
        task_id='create_time_table',
        postgres_conn_id='redshift',
        sql=final_project_create_statements.SqlQueries.time_table_create,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='load_time_dim_table',
        redshift_conn_id='redshift',
        target_table='time',
        sql_query=final_project_sql_statements.SqlQueries.time_table_insert,
        insert_type='insert-delete',
    )

    run_quality_checks = DataQualityOperator(
        task_id='run_data_quality_checks',
        redshift_conn_id="redshift",
        tests=[
            {
                'sql_query': "SELECT COUNT(*) FROM songplays WHERE sessionid IS NULL;",
                'expected_result': 0
            },
            {
                'sql_query': "SELECT COUNT(*) FROM users WHERE level IS NULL;",
                'expected_result': 0
            },
            {
                'sql_query': "SELECT COUNT(*) FROM artists;",
                'expected_result': 0
            },
            {
                'sql_query': "SELECT COUNT(*) FROM songs;",
                'expected_result': 0
            }
        ],
    )

    end_operator = DummyOperator(task_id='end_execution')

    chain(
        start_operator,
        [create_stage_events, create_stage_songs],
        [stage_events_to_redshift, stage_songs_to_redshift],
        create_songplays, load_songplays_table,
        [create_songs, create_users, create_artists, create_time],
        [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table],
        run_quality_checks,
        end_operator
    )

final_project_dag = final_project()
