from airflow.operators.email_operator import EmailOperator
import yaml, random
from airflow.operators.python import get_current_context
from airflow.models import DagRun
from airflow.models import DagBag
from airflow.models import TaskInstance
from airflow.utils.db import create_session as session

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook


def test_audit_fun():
    print("Hi this is audit function")


def push_audit_log_snowflake(log_data):
    conn = None
    try:
        sf_hook = SnowflakeHook(snowflake_conn_id="app_snowflake_audit")
        conn = sf_hook.get_conn()
        cursor = conn.cursor()
        insert_query = """INSERT INTO PROD_AUDIT_DB.AUDIT_SCHEMA.AIRFLOWPIPELINEAUDIT ("sourceTableName", "DestinationTableName", "jobRunId", "jobName", "status", "message", "jobStartTime", "jobEndTime", "desc") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cursor.execute(insert_query, log_data)
        conn.commit()
        conn.close()
    except Exception as e:
        print(str(e))
        if conn:
            conn.close()
        raise Exception(str(e))


def audit_log(**context):
    print(context)
    dagId = context["params"]["dagId"] if context["params"] else ""
    print(">>>>>>>>>>>>>>>>>>>>>>>", dagId)
    if dagId=="":
        print("--------------------")
    else:
        dagRuns = DagRun.find(dag_id=dagId)
        source_table = context["params"]["sourceTable"]
        dest_table = context["params"]["destTable"]
        desc = context["params"]["desc"]
        
        if len(dagRuns) >= 1:
            random_number = random.randint(0, 100)
            jobRunId = f"{dagId}_{random_number}"
            jobName = dagId
            dagRun = dagRuns[-1]
            status = dagRun.state
            if status == "success":
                message = "DAG succeeded"
            elif status == "failed":
                message = "DAG failed"
            else:
                message = f"DAG is in {status} state"
            startDate = str(dagRun.start_date)
            endDate = str(dagRun.end_date)
            print(jobName, jobRunId, message, startDate, endDate)
            push_audit_log_snowflake(
                (
                    source_table,
                    dest_table,
                    jobRunId,
                    jobName,
                    status,
                    message,
                    startDate,
                    endDate,
                    desc
                )
            )
            print("auditing complete")
