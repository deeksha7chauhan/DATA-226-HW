from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Default arguments for the DAG
default_args = {
    'owner': 'Deeksha',
    'start_date': days_ago(1),
    'retries': 1,
}

#To create the session_summary table with a JOIN and To check the duplicates
create_session_summary = """
CREATE TABLE IF NOT EXISTS dev.analytics.session_summary AS
WITH ranked_sessions AS (
    SELECT 
        usc.userId, 
        usc.sessionId, 
        usc.channel, 
        st.ts,
        ROW_NUMBER() OVER (PARTITION BY usc.sessionId ORDER BY st.ts DESC) AS row_num
    FROM dev.raw_data.user_session_channel usc
    JOIN dev.raw_data.session_timestamp st
    ON usc.sessionId = st.sessionId
)
SELECT
    userId,
    sessionId,
    channel,
    ts
FROM ranked_sessions
WHERE row_num = 1;
"""

#Define the DAG
with DAG(
    dag_id='session_summary',
    default_args=default_args,
    schedule_interval='@once', 
    catchup=False
) as dag:

    create_session_summary_task = SnowflakeOperator(
        task_id='create_session_summary_table',
        snowflake_conn_id='snowflake_conn',
        sql=create_session_summary
    )
        

    #Directly call the session summary
    create_session_summary_task
