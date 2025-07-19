import pytz
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator, get_current_context, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator

from resources.scripts.Scrape_Jobstreet.csv_extract import csv_extract
from resources.scripts.Scrape_Jobstreet.json_extract import json_extract

#Config for the config parameter value from main DAG.
def get_config():
    context = get_current_context()
    return context["dag_run"].conf if context.get("dag_run") else context["params"]


#Get the max time from gbq database
def fetch_last_processed_time(**kwargs):
    conf = kwargs["dag_run"].conf if kwargs.get("dag_run") else kwargs["params"]
    
    if conf["load_type"] == "full":
        return ""
    
    project_id = conf["Project"]
    source_data = conf["source_data"]
    source_name = source_data.replace("Source_Data_", "").lower()

    query = f"""
        SELECT MAX(publish_time) as max_time
        FROM `{project_id}.bronze.{source_name}_detailed`
        LIMIT 1
    """

    df = pd.read_gbq(query, project_id=project_id)
    ts = df["max_time"].iloc[0]
    
    if pd.isnull(ts):
        return ""
    try:
        ts = pd.to_datetime(ts)
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Failed to convert timestamp: {ts}, error: {e}")
        return ""

#The dag have same parameter, but can run independently
def create_scrape_ingest_dag():
    @dag(
        dag_id="scrape_ingest_dag",
        start_date=datetime(2025, 6, 7),
        catchup=False,
        tags=["Scrape", "Ingestion"],
        params={ 
            "table_date": Param(datetime.today().strftime('%Y-%m-%d'), description="Choose Date Table for ingestion"),
            "source_type": "CSV",
            "load_type": "incremental",
            "source_data": "Source_Data_Jobstreet_Indonesia"
        }
    )
    def scrape_ingest_task():
        wait_scrape_task = EmptyOperator(task_id="wait_scrape_task", trigger_rule=TriggerRule.ONE_SUCCESS)
        wait_extract_task = EmptyOperator(task_id="wait_extract_task", trigger_rule=TriggerRule.ONE_SUCCESS)
        end_task = EmptyOperator(task_id="end_task")

        fetch_last_time = PythonOperator(
            task_id="fetch_last_time",
            python_callable=fetch_last_processed_time,
            provide_context=True
        )



        #Run scraping with input conext of source_data, load_type, and fetch_last_time
        scrape = BashOperator(
            task_id="scrape",
        bash_command="""
            python /opt/airflow/dags/resources/scripts/Scrape_Jobstreet/scrape2.py \
            --country_choosen {{ (dag_run.conf['source_data'] if dag_run else params.source_data).split('_')[-1] }} \
            --load_type {{ dag_run.conf['load_type'] if dag_run else params.load_type }} \
            {% if (dag_run.conf['load_type'] if dag_run else params.load_type) == 'incremental' and ti.xcom_pull(task_ids='fetch_last_time') %} \
            --last_processed_time "{{ ti.xcom_pull(task_ids='fetch_last_time') }}" {% endif %}
            """
        )

        fetch_last_time >> scrape >> wait_scrape_task
        
        #Branch operator to choose use CSV or JSON.
        def choose_branch_extract():
            conf = get_config()
            source_type = conf.get("source_type", "csv")
            return "csv_extract" if source_type.lower() == "csv" else "json_extract"

        branch = BranchPythonOperator(
            task_id="choose_branch",
            python_callable=choose_branch_extract
        )

        wait_scrape_task >> branch
        
        @task(task_id="csv_extract")
        def run_csv_extract():
            conf = get_config()
            return csv_extract(conf["source_data"], conf["table_date"])

        @task(task_id="json_extract")
        def run_json_extract():
            conf = get_config()
            return json_extract(conf["source_data"], conf["table_date"])

        csv_extract_task = run_csv_extract()
        json_extract_task = run_json_extract()
        #control flow branch operator
        branch >> csv_extract_task >> wait_extract_task
        branch >> json_extract_task >> wait_extract_task
        wait_extract_task >> end_task

    return scrape_ingest_task()
#trigger the dag to appear at airflow UI.
globals()["scrape_ingest_dag"] = create_scrape_ingest_dag()
