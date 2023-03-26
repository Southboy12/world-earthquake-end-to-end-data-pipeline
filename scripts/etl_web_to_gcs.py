import opendatasets as od
import pandas as pd
from pathlib import Path
import os


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

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["time"] = pd.to_datetime(df['time'])
    df["updated"] = pd.to_datetime(df["updated"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

def write_to_local(df: pd.DataFrame) -> Path: 
    """Write the transformed DataFrame out locally as a parquet file"""
    path = Path(f"transformed_data/Global_Earthquake_Data.parquet")
    df.to_parquet(path, compression="gzip")
    return path

   



def etl_web_to_gcs():
    url = "https://www.kaggle.com/datasets/garrickhague/world-earthquake-data-from-1906-2022/download?datasetVersionNumber=2"
    df = extract_to_local(url)
    df_clean = transform_data(df)
    write_to_local(df_clean)

def write_to_gcs():
    





if __name__ == "__main__":
    etl_web_to_gcs()
