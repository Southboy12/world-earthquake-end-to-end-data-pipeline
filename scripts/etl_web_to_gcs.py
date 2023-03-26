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

def write_to_gcs()
   



def etl_web_to_gcs():
    url = "https://www.kaggle.com/datasets/garrickhague/world-earthquake-data-from-1906-2022/download?datasetVersionNumber=2"
    df = extract_to_local(url)
    transform_data(df)







if __name__ == "__main__":
    etl_web_to_gcs()
