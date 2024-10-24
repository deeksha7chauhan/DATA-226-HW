from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
import snowflake.connector
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Default arguments for the DAG with owner info and retries info
default_args = {
    'owner': 'Deeksha',
    'email_on_failure': False,
    'start_date': days_ago(1),
    'retries': 1,
}

create_tables = """
CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'
);

CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp
);
"""

create_blob_stage = """
CREATE OR REPLACE STAGE dev.raw_data.blob_stage
url = 's3://s3-geospatial/readonly/'
file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
"""


load_data_into_tables= """
COPY INTO dev.raw_data.user_session_channel
FROM @dev.raw_data.blob_stage/user_session_channel.csv;

COPY INTO dev.raw_data.session_timestamp
FROM @dev.raw_data.blob_stage/session_timestamp.csv;
"""

# Define the DAG with the Snowflake tasks
with DAG(
    dag_id='etl_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:

    # Task to create the user_session_channel table
    create_tables_task = SnowflakeOperator(
        task_id='create_tables',
        snowflake_conn_id='snowflake_conn',
        sql=create_tables,
    )

   

    # Task to create the stage to access S3
    create_stage_task = SnowflakeOperator(
        task_id='create_stage',
        snowflake_conn_id='snowflake_conn',
        sql=create_blob_stage,
    )

    # Task to copy data into user_session_channel from S3
    load_data_into_tables_task = SnowflakeOperator(
        task_id='load_data_into_tables',
        snowflake_conn_id='snowflake_conn',
        sql=load_data_into_tables,
    )

    # Set task dependencies
    create_tables_task >> create_stage_task >> load_data_into_tables_task
