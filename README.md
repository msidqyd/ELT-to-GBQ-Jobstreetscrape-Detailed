# Jobstreet Scraper ELT Pipeline (Airflow + BigQuery)

This project implements a scalable ELT (Extract, Load, Transform) pipeline using Apache Airflow to scrape job listings from Jobstreet, process the data, and store it in Google BigQuery following a Bronze-Silver-Gold architecture.

## Overview

This pipeline automates the collection and transformation of job data for analytical use cases. It uses parameterized Airflow DAGs to orchestrate scraping, ingestion, transformation, and gold-layer aggregation. It is designed to support both full and incremental loads with modular DAGs and multithreaded web scraping.

## Architecture
<img width="1988" height="538" alt="image" src="https://github.com/user-attachments/assets/a4e22911-c7e7-426c-bc14-f739028dd863" />

### DAG Graph
<img width="1285" height="132" alt="image" src="https://github.com/user-attachments/assets/9433fa7f-0760-4e68-a28a-ed478ef94cf9" />
Main DAG triggers 4 child DAGs in sequence:
   
- Scrape & Ingest DAG
- Load DAG (into Bronze layer)
- Transform DAG (to Silver layer)
- Gold Aggregation DAG (dimension and fact tables)
1. Job data is scraped using Selenium and saved in CSV/JSON format.
2. Data is ingested into BigQuery tables in a structured way.
3. Gold tables include word frequency metrics and dimensional modeling.

## Key Features

- Modular and scalable ELT pipeline using Airflow with parent-child DAGs
- Configurable parameters passed from main DAG to each child (e.g. full vs incremental)
- Bronze, Silver, and Gold schema layers for structured data modeling
- Web scraping with Selenium and multithreading for performance


## Configuration
<img width="1530" height="444" alt="image" src="https://github.com/user-attachments/assets/39ba383f-9a4d-4c61-9ac7-403e9655f0da" />
Parameters are configurable in the `params` block:

- table_date: target table with date
- load_type: "full" or "incremental"
- source_type: CSV or JSON

These parameters can be set in the main DAG or overridden at runtime.


## Gold Layer Outputs
- jobstreetscrape_singapore_detailed : clean table at silver layer.
- word_frequency: top 300 most common meaningful words per job description, counted once per job.

## Stack

- Apache Airflow (orchestration)
- Google BigQuery (cloud warehouse)
- Python + Pandas (processing)
- Selenium (scraping)
- ThreadPoolExecutor (parallelism)

## Example Use Cases

- Analyze job postings across roles and locations
  <img width="725" height="545" alt="image" src="https://github.com/user-attachments/assets/ca322573-bd69-4b8b-81d5-92cde0e32e94" />
  
- Monitor keyword trends in job descriptions
  <img width="813" height="614" alt="image" src="https://github.com/user-attachments/assets/fbbbbb9f-6932-4e9a-9444-bb5e0dcb21b9" />
- Updated job list for job seeker
  <img width="778" height="574" alt="image" src="https://github.com/user-attachments/assets/7430f4f0-be8e-48ac-8c16-ce9f22fc1a9f" />
 

- Build dashboards with salary distribution and job demand
- Feed data into machine learning models for job classification or recommendations





