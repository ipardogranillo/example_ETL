from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from scripts import fetch_local
from scripts import fetch_api
from scripts import format_conversion
from scripts import parquet


@task(trigger_rule=TriggerRule.ONE_FAILED, retries=5)
def watcher():
    raise AirflowException("Failing task because one or more upstream tasks failed.")


with DAG(
    dag_id="weather_ETL",
    description="extract data from an API an enrich it with the  most recent local data",

    schedule_interval="@hourly",
    start_date=datetime(2022, 5, 1),
    catchup=False,
    tags=["extract", "local"]
) as dag:

    fetch_gz=PythonOperator(
        task_id="fetch_api",
        python_callable=fetch_api.fetchGz,
    )

    fetch_csv=PythonOperator(
        task_id="fetch_local",
        python_callable=fetch_local.fetchCsv,
    )
    
    gz2json=PythonOperator(
        task_id="gz2json",
        python_callable=format_conversion.gz2json,
    )

    csv2parquet=PythonOperator(
        task_id="csv2parquet",
        python_callable=format_conversion.csv2parquet,
    )

    json2parquet=PythonOperator(
        task_id="json2parquet",
        python_callable=format_conversion.json2parquet,
    )

    tempAvg=PythonOperator(
        task_id="tempAvg",
        python_callable=parquet.tempAvg,
    )

    enrich=PythonOperator(
        task_id="enrich",
        python_callable=parquet.enrich,
    )

    dumpAPI=PythonOperator(
        task_id="dumpAPI",
        python_callable=parquet.dumpAPI
    )

    dumpEnriched=PythonOperator(
        task_id="dumpEnriched",
        python_callable=parquet.dumpEnriched
    )

    teardown=BashOperator(
        task_id="teardown",
        bash_command="echo 'finished execution'",
        trigger_rule=TriggerRule.ALL_DONE,
    )


    fetch_csv >> csv2parquet >> enrich
    fetch_gz >> gz2json >> json2parquet >> dumpAPI
    json2parquet >> enrich >> dumpEnriched >> teardown
    list(dag.tasks) >> watcher()