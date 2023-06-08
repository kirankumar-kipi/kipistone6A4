from airflow import DAG
from datetime import timedelta, datetime
from audit import test_audit_fun, audit_log
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


DAG_DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="audit_dag",
    description="Auditing for airflow pipeline",
    default_args=DAG_DEFAULT_ARGS,
    start_date=datetime(2023, 5, 5),
    catchup=False,
    tags=["APP","AUDIT"],
) as dag:
    task = PythonOperator(
        task_id="audit_task",
        python_callable=audit_log
    )
