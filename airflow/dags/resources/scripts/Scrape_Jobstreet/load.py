def load(table_date, load_type , Project, source_data) :

    import pandas as pd
    #Replace the source data name with sapace.
    source_name = source_data.replace("Source_Data_", "").lower()
    parquet_path = f"/opt/airflow/dags/Data_to_Transform/{source_name}_combine_{table_date}.parquet"
    df = pd.read_parquet(parquet_path)
    #perform incremenal load    
    if load_type == 'incremental' :
        df.to_gbq(destination_table=f'bronze.{source_name}_detailed',project_id=Project, if_exists="append")
    #perform full load to GBQ
    else :    
        df.to_gbq(destination_table=f'bronze.{source_name}_detailed',project_id=Project, if_exists="replace")      

