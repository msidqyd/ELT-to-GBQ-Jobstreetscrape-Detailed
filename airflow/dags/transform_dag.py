import pytz
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.models.param import Param
from resources.scripts.Scrape_Jobstreet.TRANSFORM2 import transformation
#Get data from parent DAG if running triggered
def get_config():
    context = get_current_context()
    return context["dag_run"].conf if context.get("dag_run") else context["params"]
# Create DAG with independent config parameter
def create_transform_dag():
    @dag(
        dag_id="transformation_dag",
        start_date=datetime(2025, 6, 7),
        catchup=False,
        tags=["Transform"],
        params={
        "source_data": Param("Source_Data_Jobstreetscrape_Indonesia", description="Source name"),
        "Project": Param("citric-bee-460614-c9", description="GCP Project ID")
    }
    )
    def transform_task():
        start_task = EmptyOperator(task_id="start_task")
        end_task = EmptyOperator(task_id="end_task")
        #Get config for context needed.
        @task(task_id="transform")
        def run_transform():
            conf = get_config()
            return transformation(
                conf["source_data"],
                conf["Project"]
            )

        transform = run_transform()

        start_task >> transform >> end_task

    return transform_task()

globals()["transformation_dag"] = create_transform_dag()
