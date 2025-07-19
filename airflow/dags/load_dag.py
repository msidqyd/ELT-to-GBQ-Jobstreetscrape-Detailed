import pytz
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import get_current_context

from resources.scripts.Scrape_Jobstreet.load import load
#If there's a value of config parameter from main DAG, it would follow the main DAG config parameter
def get_config():
    context = get_current_context()
    return context["dag_run"].conf if context.get("dag_run") else context["params"]
# load dag has the own config parameter for run independently.
def create_load_dag():
    @dag(
        dag_id="load_dag",
        start_date=datetime(2025, 6, 7),
        catchup=False,
        tags=["To_GBQ"],
        params={  # Fallback for manual runs
            "table_date": Param(datetime.today().strftime('%Y-%m-%d'), description="Choose Date Table for ingestion"),
            "load_type": "incremental",
            "Project": "citric-bee-460614-c9",
            "source_data": "Source_Data_Jobstreet_Indonesia"
        }
    )
    def load_task():
        start_task = EmptyOperator(task_id="start_task")
        end_task = EmptyOperator(task_id="end_task")
        #Create DAG for load with passing config parameter either from these dag or main dag
        @task(task_id="load_to_gbq")
        def run_load_to_gbq():
            conf = get_config()
            return load(
                conf["table_date"],
                conf["load_type"],
                conf["Project"],
                conf["source_data"]
            )

        load_task = run_load_to_gbq()

        start_task >> load_task >> end_task

    return load_task()
#Ensuring the DAG appear at the airflow.
globals()["scrape_ingest_dag"] = create_load_dag()

