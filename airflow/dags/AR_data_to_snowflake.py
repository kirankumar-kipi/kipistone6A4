from airflow import DAG
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from datetime import datetime, timedelta
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from helper_functions import ingest_metadata
from airflow.operators.python_operator import PythonOperator
import yaml

with open("plugins/config.yml", "r") as f:
    config = yaml.safe_load(f)
    print(config)

DAG_ID = "AR_DATA_TO_SNOWFLAKE"
SNOWFLAKE_DB = "PROD_DWH"
SNOWFLAKE_SCHEMA = "STAGING_SCHEMA"
SNOWFLAKE_STAGE = "S3_STAGE"
SNOWFLAKE_DEST_TABLE = "AR_DETAILS"
SNOWFLAKE_ROLE = "PROD_DEVELOPER_FR"
SNOWFLAKE_WAREHOUSE = "PROD_WH"


DAG_DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    DAG_ID,
    default_args=DAG_DEFAULT_ARGS,
    description="Load AR data to snowflake from ARdata s3 files",
    start_date=datetime(2023, 5, 5),
    tags=["APP"],
    schedule="@weekly",
    catchup=False,
) as dag:
    # 1. Metadata entry
    metadata_create = PythonOperator(
        task_id="metadata_entry",
        python_callable=ingest_metadata,
        dag=dag,
        op_kwargs={"TYPE": "AR_DATA"},
    )

    # 2. snowflake ingestion
    copy_json_into_table = S3ToSnowflakeOperator(
        dag=dag,
        task_id="s3_copy_into_table",
        snowflake_conn_id="app_snowflake",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        table=SNOWFLAKE_DEST_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        role=SNOWFLAKE_ROLE,
        pattern=".*ar_details_.*[.]json",
        file_format="JSON_FILE_FORMAT",
    )

    # 3. Audit Logging
    audit = TriggerDagRunOperator(
        trigger_dag_id="audit_dag",
        task_id="audit",
        dag=dag,
        conf={
            "dagId": DAG_ID,
            "sourceTable": "S3",
            "destTable": SNOWFLAKE_DEST_TABLE,
            "desc": "AR data json file load - {{task_instance.xcom_pull('s3_copy_into_table')}}rows",
        },
    )


metadata_create >> copy_json_into_table >> audit

# copy_json_into_table >> audit
