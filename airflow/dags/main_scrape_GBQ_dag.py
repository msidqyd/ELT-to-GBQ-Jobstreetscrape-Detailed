import pytz
from datetime import datetime
import yaml
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator, get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from resources.scripts.Scrape_Jobstreet.extract_csv import extract_csv
from resources.scripts.Scrape_Jobstreet.extract_json import extract_json
from resources.scripts.Scrape_Jobstreet.load import load
from resources.scripts.Scrape_Jobstreet.transform import transformation
from resources.scripts.Scrape_Jobstreet.validation import validation_data
from airflow.sensors.external_task import ExternalTaskSensor

folder_staging = "/opt/airflow/dags/Staging_Data"
Project = 'citric-bee-460614-c9'

# Load source list
with open("dags/resources/dynamic-dag/list_source.yaml") as f:
    list_data_source = yaml.safe_load(f)

# Create dag with schedule at 21:15 everyday, also i add the config parameter for reducing manual intervention.
def create_elt_dag(source_data):
    @dag(
        dag_id=f"Main_DAG_ELT_GBQ_{source_data}",
        schedule_interval="15 21 * * *",
        start_date=datetime(2025, 6, 7, tzinfo=pytz.timezone("Asia/Jakarta")),
        catchup=False,
        tags=["ELT_Scrape", "To_GBQ"],
        params={
            "source_type": "CSV",
            "load_type": Param("incremental", description="incremental/full", enum=["full", "incremental"]),
            "table_date": Param(datetime.today().strftime('%Y-%m-%d'), description="Choose Date Table for ELT To GBQ"),
        }
    )
    def elt_task():
        #For passing config parameter to child dag.
        shared_conf = {
            "table_date": "{{ params.table_date }}",
            "load_type": "{{ params.load_type }}",
            "source_type": "{{ params.source_type }}",
            "Project": "citric-bee-460614-c9",
            "source_data": source_data
        }
        #trigger DAG with check 60 second for finish task.
        trigger_scrape_ingest = TriggerDagRunOperator(
            task_id='trigger_scrape_ingest',
            trigger_dag_id='scrape_ingest_dag',
            conf=shared_conf,
            wait_for_completion=True,
            poke_interval=60,
            reset_dag_run=True  
        )

        trigger_load = TriggerDagRunOperator(
            task_id='trigger_load',
            trigger_dag_id='load_dag',
            conf=shared_conf,
            wait_for_completion=True,
            poke_interval=30,
            reset_dag_run=True
        )

        trigger_transform = TriggerDagRunOperator(
            task_id = 'trigger_transform',
            trigger_dag_id = 'transformation_dag',
            conf=shared_conf,
            wait_for_completion=True,
            poke_interval=30,
            reset_dag_run=True              
        )

        trigger_gold = TriggerDagRunOperator(
            task_id = 'trigger_gold_layer',
            trigger_dag_id = 'gold_aggregation_dag',
            conf=shared_conf,
            wait_for_completion=True,
            poke_interval=30,
            reset_dag_run=True              
        )

        #Control flow for every DAG triggered
        trigger_scrape_ingest >> trigger_load >> trigger_transform>> trigger_gold
    return elt_task()

# Create DAGs dynamically from source list folder.
for source_data in list_data_source:
    dag_id = f"ELT_{source_data}"
    globals()[dag_id] = create_elt_dag(source_data)
