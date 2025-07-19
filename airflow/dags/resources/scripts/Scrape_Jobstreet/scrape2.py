import argparse
import os
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
from datetime import datetime, timedelta
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from itertools import count
from concurrent.futures import ThreadPoolExecutor, as_completed

#Open headless windows
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument("user-agent=Mozilla/5.0")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
#Get the detail page first before further scraping.
def fetch_detail_page(url):
    detail_driver = webdriver.Chrome(options=chrome_options)
    detail_driver.get(url)
    job_desc = salary = work_type = ""

    try:
        WebDriverWait(detail_driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-automation="jobAdDetails"]')))
        job_desc = " ".join([
            x.text.strip().replace('\n', '; ')
            for x in detail_driver.find_elements(By.CSS_SELECTOR, 'div[data-automation="jobAdDetails"]')
        ])
    except:
        pass

    try:
        work_type = "; ".join([
            x.text.strip()
            for x in detail_driver.find_elements(By.CSS_SELECTOR, 'span[data-automation="job-detail-work-type"] a')
        ])
    except:
        pass

    try:
        salary = "; ".join([
            x.text.strip()
            for x in detail_driver.find_elements(By.CSS_SELECTOR, 'span[data-automation="job-detail-salary"]')
        ])
    except:
        pass

    detail_driver.quit()
    return job_desc, salary, work_type
#Get Publish time data
def parse_publish_time(element, now):
    try:
        time_str = element.find_element(By.CSS_SELECTOR, 'span[data-automation="jobListingDate"]').text
        num = int(''.join(filter(str.isdigit, time_str)))
        #Get the detail of pubish time data so it could be timestamp.
        if "m" in time_str:
            return (now - timedelta(minutes=num)).strftime('%Y-%m-%d %H:%M:%S')
        elif "h" in time_str:
            return (now - timedelta(hours=num)).strftime('%Y-%m-%d %H:%M:%S')
        elif "d" in time_str:
            return (now - timedelta(days=num)).strftime('%Y-%m-%d %H:%M:%S')
    except:
        pass
    return now.strftime('%Y-%m-%d %H:%M:%S')
#Data input for scraping process
def jobstreet_scrape(keyword, country_choosen, Keywords_match, load_type="full", last_processed_time=None):
    print(f"===> Starting scrape for: {keyword} | Type: {load_type}", flush=True)
    driver = webdriver.Chrome(options=chrome_options)
    #Link for initial open
    country_map = {
        'indonesia': 'https://www.jobstreet.co.id/en',
        'malaysia': 'https://www.jobstreet.com.my/en',
        'philippines': 'https://www.jobstreet.com.ph/en',
        'singapore': 'https://www.jobstreet.com.sg/en',
    }

    driver.get(country_map[country_choosen])
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[placeholder='Enter keywords']"))).send_keys(keyword)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))).click()

    now = datetime.now()
    search_url = driver.current_url + "?page="
    job_list = []

    last_dt = datetime.strptime(last_processed_time, '%Y-%m-%d %H:%M:%S') if (load_type == "incremental" and last_processed_time) else None
    ##Load page untill found one page with none scrape result 
    for page in count(start=1):
        driver.get(f"{search_url}{page}")
        #the others element will find element inside each job_id structure, so the data will allign to job_id.
        job_elements = driver.find_elements(By.CSS_SELECTOR, 'div[data-search-sol-meta]')
        if not job_elements:
            break

        for element in job_elements:
            try:
                role = element.find_element(By.CSS_SELECTOR, 'a[data-automation="jobTitle"]').text
            except:
                continue

            words = set(re.findall(r'\w+', role.lower()))
            matches = words.intersection(set(k.lower() for k in Keywords_match))
            #Filter title with keywords
            if len(matches) < 2:
                continue
            
            job_id = element.find_element(By.CSS_SELECTOR, 'article[data-job-id]').get_attribute("data-job-id") if element.find_elements(By.CSS_SELECTOR, 'article[data-job-id]') else None
            if not job_id:
                continue

            publish_time_str = parse_publish_time(element, now)
            publish_time = datetime.strptime(publish_time_str, '%Y-%m-%d %H:%M:%S')
            #Filter publish time if incremental
            if last_dt and publish_time <= last_dt:
                continue
            #Safe the result for further scrape
            url = f"https://id.jobstreet.com/id/job/{job_id}"
            job_list.append({
                "Job_ID": job_id,
                "Role": role,
                "Company": element.find_element(By.CSS_SELECTOR, 'a[data-automation="jobCompany"]').text if element.find_elements(By.CSS_SELECTOR, 'a[data-automation="jobCompany"]') else None,
                "Location": element.find_element(By.CSS_SELECTOR, 'a[data-automation="jobLocation"]').text if element.find_elements(By.CSS_SELECTOR, 'a[data-automation="jobLocation"]') else None,
                "Publish_Time": publish_time_str,
                "URL": url,
                "country": country_choosen
            })

    driver.quit()
    #Scraping each page of filtered URL.
    print(f"==> Scraping detail pages for {len(job_list)} jobs...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_job = {
            executor.submit(fetch_detail_page, job['URL']): job for job in job_list
        }
        for future in as_completed(future_to_job):
            job = future_to_job[future]
            try:
                job['job_desc'], job['salary'], job['work_type'] = future.result()
            except Exception as e:
                job['job_desc'], job['salary'], job['work_type'] = "", "", ""
                print(f"Detail page failed for {job['URL']}: {e}")
    #Load the data to be CSV and JSON
    df = pd.DataFrame(job_list)
    if not df.empty:
        print(f"==> Saving {len(df)} jobs for keyword: {keyword}")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_base = f"/opt/airflow/dags/Source_Data_jobstreetscrape_{country_choosen}/jobstreetscrape_{keyword.replace(' ','')}_{country_choosen}_{timestamp}"
        df.to_csv(path_base + ".csv", index=False)
        df.to_json(path_base + ".json", orient='records', lines=True)
#Initiate scrape with data needed and multithreading.
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--country_choosen", required=True, help="Country name")
    parser.add_argument("--load_type", choices=["full", "incremental"], default="full", help="Load type")
    parser.add_argument("--last_processed_time", required=False, help="For incremental: format YYYY-MM-DD HH:MM:SS")
    args = parser.parse_args()
    #Data passed from DAG
    country_choosen = args.country_choosen
    load_type = args.load_type
    last_processed_time = args.last_processed_time

    Search = [
    "Big Data Engineer", 
    "Data Engineer",
    "Data Platform Engineer",
    "ETL Developer",
    "Data Pipeline Developer",
    "Data Infrastructure Engineer",
    "Data Integration Engineer",
    "DataOps Engineer"]
    Keywords_match = [
        "Big", "Data", "Engineer", "Cloud", "Integration", "ETL", "Infrastructure", "Pipeline",
        "DataOps", "Developer", "Platform", "Warehouse", "Airflow", "Spark", "Kafka", "Hadoop",
        "Databricks", "Snowflake", "Redshift", "BigQuery", "Fivetran", "dbt", "PostgreSQL", "MySQL",
        "SQL", "NoSQL", "DeltaLake", "ML Engineer", "ETL Developer", "Data Architect",
        "Analytics Engineer", "Platform Engineer", "BI Engineer", "Data Specialist", "Data Manager",
        "AWS", "GCP", "Azure", "Streaming", "Batch", "Lakehouse", "Data Lake", "Data Warehouse",
        "Ingestion", "Transformation", "Data Modeling", "Data Governance", "Orchestration",
        "Web Scraping", "Scraper", "Extraction", "Collection", "Web Crawler", "Architect"
    ]
    #Apply multithreading for open & scrape URL
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [
            executor.submit(jobstreet_scrape, keyword, country_choosen, Keywords_match, load_type, last_processed_time)
            for keyword in Search
        ]
        for future in as_completed(futures):
            future.result()

if __name__ == "__main__":
    main()
