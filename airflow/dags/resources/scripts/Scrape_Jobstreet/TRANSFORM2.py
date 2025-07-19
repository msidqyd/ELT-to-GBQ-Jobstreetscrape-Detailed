def transformation(source_data, Project):
    from google.cloud import bigquery
    import logging

    source_name = source_data.replace("Source_Data_", "").lower()

    # Cleaning for duplicates, salary parsing, role formatting, work_type normalization
    query = f"""
    CREATE OR REPLACE TABLE silver.{source_name}_detailed AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY job_id_platform DESC) AS job_id,
        job_id_platform,

        -- Fix role casing for "Data Engineer"/"Data Engineering"
        CASE
            WHEN LOWER(role) LIKE '%data engineer%' THEN REGEXP_REPLACE(role, r'(?i)data engineer', 'Data Engineer')
            WHEN LOWER(role) LIKE '%data engineering%' THEN REGEXP_REPLACE(role, r'(?i)data engineering', 'Data Engineering')
            ELSE role
        END AS role,

        company,
        location,
        posted_time,
        url,
        job_desc,
        salary,

        -- Normalize work_type
        CASE
            WHEN LOWER(work_type) LIKE '%kontrak%' AND LOWER(work_type) LIKE '%full%' THEN 'Kontrak/Temporer'
            WHEN work_type IS NULL OR work_type = '' THEN 'The Advertiser did not show the work type'
            ELSE work_type
        END AS work_type,

        country,
        -- Transform Salary
        SAFE_CAST(
        REGEXP_EXTRACT(REGEXP_REPLACE(LOWER(salary), r'[^0-9–-]+', ''), r'^(\\d+)') AS NUMERIC
        ) AS salary_min_cleaned,

        -- salary_max_cleaned
        SAFE_CAST(
        REGEXP_EXTRACT(REGEXP_REPLACE(LOWER(salary), r'[^0-9–-]+', ''), r'[–-](\\d+)$') AS NUMERIC
        ) AS salary_max_cleaned

    FROM (
        SELECT
            TRIM(CAST(Job_ID AS STRING)) AS job_id_platform,
            TRIM(CAST(Role AS STRING)) AS role,
            COALESCE(TRIM(CAST(Company AS STRING)), "Advertiser didn't show the company") AS company,
            TRIM(CAST(Location AS STRING)) AS location,
            CASE
                WHEN publish_time IS NULL THEN TIMESTAMP '1900-01-01 00:00:00'
                ELSE PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(CAST(publish_time AS STRING)))
            END AS posted_time,
            TRIM(CAST(URL AS STRING)) AS url,
            TRIM(CAST(job_desc AS STRING)) AS job_desc,
            TRIM(CAST(salary AS STRING)) AS salary,
            TRIM(CAST(work_type AS STRING)) AS work_type,
            TRIM(CAST(country AS STRING)) AS country,
            ROW_NUMBER() OVER (PARTITION BY Job_ID ORDER BY publish_time ASC) AS dup
        FROM bronze.{source_name}_detailed
        WHERE Job_ID IS NOT NULL
          AND Role IS NOT NULL
          AND URL IS NOT NULL
          AND publish_time IS NOT NULL
    )
    WHERE dup = 1
    """

    client = bigquery.Client(project=Project)
    transform_gcp = client.query(query)
    transform_gcp.result()
    print("Transformation Success !!!")

    # Validation regarding to job_id_platform
    query_1 = f"""
    SELECT COUNT(*) as null_count
    FROM silver.{source_name}_detailed
    WHERE job_id_platform IS NULL
      OR role IS NULL
      OR company IS NULL
      OR posted_time IS NULL
      OR url IS NULL
    """

    query_2 = f"""
    SELECT job_id_platform
    FROM silver.{source_name}_detailed
    GROUP BY job_id_platform
    HAVING COUNT(*) > 1
    """

    result_null = client.query(query_1).result()
    result_dup = client.query(query_2).result()

    null_count = [row["null_count"] for row in result_null][0]
    dup_list = [row["job_id_platform"] for row in result_dup]
    dup_count = len(dup_list)

    logging.info(f"{null_count} null values detected and {dup_count} duplicate job_id_platform detected.")
