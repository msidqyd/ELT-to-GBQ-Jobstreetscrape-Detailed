def json_extract(source_data,table_date):
    import os
    import glob
    import pandas as pd
    from datetime import datetime
    import re

    filtered_files = []
    source_name = source_data.replace("Source_Data_", "").lower()
    folder_source = Fr"/opt/airflow/dags/{source_data}"
    #Read all json file at the folder targeted.
    json_files = glob.glob(os.path.join(folder_source, "*.json"))
    #For the table in folder fulfill the table date criteria, will store to filtered_files.
    for file in json_files :
        
        base = os.path.basename(file)
        match = re.search(r'_(\d{8})_\d{6}', base)
        if match:
                file_date_str = match.group(1)
                file_date = datetime.strptime(file_date_str, "%Y%m%d").strftime('%Y-%m-%d')
                if file_date == table_date:
                    filtered_files.append(file)
    
    if not filtered_files:
        raise ValueError(f"No json files found within date {table_date}")
    #Combine all table with table date criteria.
    df_all = pd.concat([pd.read_json(f,lines=True) for f in filtered_files], ignore_index=True)
    #Clean duplicate 
    before = len(df_all)
    df_all = df_all.drop_duplicates(subset = [col for col in df_all.columns if col !='Publish_Time'])
    after = len(df_all)
    print(f"Removed {before - after} duplicate rows")

    df_all['Publish_Time'] = pd.to_datetime(df_all['Publish_Time'], errors = 'coerce')
    if df_all['Publish_Time'].dt.tz is not None:
        df_all['Publish_Time'] = df_all['Publish_Time'].dt.tz_convert('UTC').dt.tz_localize(None)

    df_all['Publish_Time'] = df_all['Publish_Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    #Load to staging as parquet
    os.makedirs("/opt/airflow/dags/Data_to_Transform", exist_ok=True)
    parquet_path = f"/opt/airflow/dags/Data_to_Transform/{source_name}_combine_{table_date}.parquet"
    df_all.to_parquet(parquet_path)
    return f"{source_name} extracted successfully with {len(df_all)} records"
