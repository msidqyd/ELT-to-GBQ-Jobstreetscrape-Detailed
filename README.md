# Jobstreet Scraper ELT Pipeline (Airflow + BigQuery)

This project implements a scalable ELT (Extract, Load, Transform) pipeline using Apache Airflow to scrape job listings from Jobstreet, process the data, and store it in Google BigQuery following a Bronze-Silver-Gold architecture.

## Overview

This pipeline automates the collection and transformation of job data for analytical use cases. It uses parameterized Airflow DAGs to orchestrate scraping, ingestion, transformation, and gold-layer aggregation. It is designed to support both full and incremental loads with modular DAGs and multithreaded web scraping.

## Architecture

1. Main DAG triggers 4 child DAGs in sequence:
   - Scrape & Ingest DAG
   - Load DAG (into Bronze layer)
   - Transform DAG (to Silver layer)
   - Gold Aggregation DAG (dimension and fact tables)

2. Job data is scraped using Selenium and saved in CSV/JSON format.
3. Data is ingested into BigQuery tables in a structured way.
4. Gold tables include word frequency metrics and dimensional modeling.

## Key Features

- Modular and scalable ELT pipeline using Airflow with parent-child DAGs
- Configurable parameters passed from main DAG to each child (e.g. full vs incremental)
- Bronze, Silver, and Gold schema layers for structured data modeling
- Web scraping with Selenium and multithreading for performance


## Configuration

Parameters are configurable in the `params` block:

- source_data: source folder (e.g. Source_Data_Jobstreet_Indonesia)
- table_date: target partition date
- load_type: "full" or "incremental"
- run_scrape: "run scrape" or "skip scrape"
- source_type: CSV or JSON

These parameters can be set in the main DAG or overridden at runtime.

## Gold Layer Outputs

- dim_company: unique companies
- dim_location: unique locations and countries
- dim_role: unique job roles
- dim_job_detail: job description and work type
- fact_job_posting: core fact table with dimensional keys and salary range
- word_frequency: top 300 most common meaningful words per job description, counted once per job

## Stack

- Apache Airflow (orchestration)
- Google BigQuery (cloud warehouse)
- Python + Pandas (processing)
- Selenium (scraping)
- ThreadPoolExecutor (parallelism)

## Example Use Cases

- Analyze job postings across roles and locations
- Monitor keyword trends in job descriptions
- Build dashboards with salary distribution and job demand
- Feed data into machine learning models for job classification or recommendations





