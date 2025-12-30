from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum
import config
import snowflake_sql

default_args = {
    'owner': 'student',
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=1),
}

@dag(
    dag_id='airline_snowflake_elt_v2',
    default_args=default_args,
    schedule='@daily',
    start_date=pendulum.today('UTC'),
    catchup=False,
    tags=['snowflake', 'elt']
)
def airline_snowflake_elt():

    @task
    def upload_to_stage():
        hook = SnowflakeHook(snowflake_conn_id=config.SNOWFLAKE_CONN_ID)
        hook.run(snowflake_sql.SQL_UPLOAD_TO_STAGE)

    @task
    def copy_into_raw():
        hook = SnowflakeHook(snowflake_conn_id=config.SNOWFLAKE_CONN_ID)
        hook.run(snowflake_sql.SQL_COPY_INTO_RAW)

    @task
    def transform_core():
        hook = SnowflakeHook(snowflake_conn_id=config.SNOWFLAKE_CONN_ID)
        hook.run(snowflake_sql.SQL_CALL_CORE_PROC, autocommit=True)

    @task
    def transform_marts():
        hook = SnowflakeHook(snowflake_conn_id=config.SNOWFLAKE_CONN_ID)
        hook.run(snowflake_sql.SQL_CALL_MARTS_PROC, autocommit=True)

    upload_to_stage() >> copy_into_raw() >> transform_core() >> transform_marts()

dag_instance = airline_snowflake_elt()