import opendatasets as od
import pandas as pd
from pathlib import Path
import os
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_to_local(url: str) -> pd.DataFrame:
    """Download the dataset from kaggle and put in a DataFrame"""
    url = 'https://www.kaggle.com/datasets/garrickhague/world-earthquake-data-from-1906-2022/download?datasetVersionNumber=2'
    path = Path(f"./data")
    od.download(url, path)
    csv_file_path = f"data/world-earthquake-data-from-1906-2022/Global_Earthquake_Data.csv"
    df = pd.read_csv(csv_file_path)
    parquet_file_path = f"./data/world-earthquake-data-from-1906-2022/Global_Earthquake_Data.parquet"
    df.to_parquet(parquet_file_path, compression="gzip")
    return df

@task(log_prints=True)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["time"] = pd.to_datetime(df['time'])
    df["updated"] = pd.to_datetime(df["updated"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_to_local(df: pd.DataFrame) -> Path: 
    """Write the transformed DataFrame out locally as a parquet file"""
    path = Path(f"earthquake_data/Global_Earthquake_Data.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task(log_prints=True)
def write_to_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=Path(path).as_posix())
    return


@flow()
def etl_web_to_gcs():
    url = "https://www.kaggle.com/datasets/garrickhague/world-earthquake-data-from-1906-2022/download?datasetVersionNumber=2"
    df = extract_to_local(url)
    df_clean = transform_data(df)
    path = write_to_local(df_clean)
    write_to_gcs(path)


    





if __name__ == "__main__":
    etl_web_to_gcs()
