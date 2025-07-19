from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
from google.cloud import bigquery
from datetime import datetime

@dag(
    dag_id="gold_aggregation_dag",
    start_date=datetime(2025, 6, 7),
    catchup=False,
    schedule=None,
    tags=["Gold", "Analytics"]
)
def gold_aggregation():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Run gold layer task 
    @task(task_id="generate_gold_word_frequency")
    def transform_gold():
        # Get current context for get the conf value from the DAG parent.
        context = get_current_context()
        conf = context["dag_run"].conf if context.get("dag_run") else context["params"]
        source_data = conf["source_data"]
        project_id = conf["Project"]
        source_name = source_data.replace("Source_Data_", "").lower()
        silver_table = f"`{project_id}.silver.{source_name}_detailed`"
        client = bigquery.Client(project=project_id)

        # Only query for word_frequency table using your latest SQL
        query = f"""
        CREATE OR REPLACE TABLE `{project_id}.gold.word_frequency` AS
        WITH distinct_jobs AS (
            SELECT DISTINCT job_id, job_desc
            FROM {silver_table}
            WHERE job_id IS NOT NULL
        ),
        tokenized AS (
            SELECT
                job_id,
                ARRAY(
                    SELECT DISTINCT LOWER(word)
                    FROM UNNEST(SPLIT(REGEXP_REPLACE(job_desc, r'[^a-zA-Z0-9\\s]', ''), ' ')) AS word
                    WHERE word != ''
                ) AS unique_words
            FROM distinct_jobs
        ),
        exploded AS (
            SELECT DISTINCT
                job_id,
                word
            FROM tokenized,
            UNNEST(unique_words) AS word
        ),
        filtered AS (
            SELECT word
            FROM exploded
            WHERE word NOT IN (
                -- stop words (English + Bahasa + job filler)
                'the','and','or','to','in','of','a','an','is','it','for','on','with',
                'this','that','at','as','by','are','be','you','we','from','was','will',
                'have','has','can','if','your','our','they','he','she','their','them',
                'but','about','more','other','may','not','which','who','what','how','do',
                'dan','yang','dengan','dalam','untuk','atau','pada','dari','sebagai',
                'job','role','description','requirement','requirements','responsibilities',
                'position','candidate','candidates','opportunity','team','teams','client',
                'clients','responsibility','communication','skills','skill','tools','tool',
                'knowledge','year','years','experience','experiences','understanding',
                'familiarity','strong','good','great','minimum','preferred','plus',
                'working','work','ability','ensure','related','such','build','develop',
                'development','projects','project','engineering','engineer','bachelor',
                'degree','excellent','background','including','preferred','detail',
                'science','computer','management','information','field','level','provide',
                'must','should','across','also','etc','new','join','within','using','help',
                'teams','per','needed','pengalaman','seperti','anda','kualifikasi','lainnya',
                '1','2','3','4','5','12','90','58','56','54','52','51','49','46','43','41','39','38','37',
                'us','any','its','youll','please','product','company','service',
                'helping','apply','closely','make','create','develop','needed',
                'jakarta','indonesia','team','teams','based','join','support',
                'help','looking','ensure','able','must','should','well','part',
                'success','value','perform','responsible','provide','deliver'
            )
        ),
        word_counts AS (
            SELECT
                word,
                COUNT(DISTINCT job_id) AS frequency
            FROM exploded
            WHERE word IN (SELECT word FROM filtered)
            GROUP BY word
        )
        SELECT *
        FROM word_counts
        ORDER BY frequency DESC
        LIMIT 300
        """

        client.query(query).result()
        return "Gold word_frequency table created."

    start >> transform_gold() >> end

gold_aggregation_dag = gold_aggregation()
