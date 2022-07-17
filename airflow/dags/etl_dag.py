from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule




from scripts import current
from scripts import api
from scripts import municipalities

DEFAULT_ARGS = {
    "owner": "ipardogranillo",
    "retries": 1 ,
}

@task(trigger_rule=TriggerRule.ONE_FAILED, retries=1)
def watcher():
    raise AirflowException("Failing task because one or more upstream tasks failed.")


with DAG(
    dag_id="example_etl",
    description="extract data from an API an enrich it with the  most recent local data",
    schedule_interval="@hourly",
    start_date=datetime(2022, 5, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["weather", "municipalities"]
) as dag:

    start = DummyOperator(task_id="start")

    api_get = PythonOperator(
        task_id="get_weather",
        python_callable=api.get_api,
    )

    api_dump = PythonOperator(
        task_id="dump_weather",
        python_callable=api.dump_api,
    )
    
    mun_data = PythonOperator(
        task_id="append_muns",
        python_callable=municipalities.append_muns,
    )

    curr_data = PythonOperator(
        task_id="enrich_current",
        python_callable=current.enrich_current,
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'finished execution'",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> api_get >> api_dump >> mun_data >> curr_data >> end

    list(dag.tasks) >> watcher()