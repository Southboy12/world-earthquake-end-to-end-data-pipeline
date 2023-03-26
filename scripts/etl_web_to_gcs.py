import opendatasets as od
import pandas as pd
from pathlib import Path
import os


def extract_to_local():
    url = 'https://www.kaggle.com/datasets/garrickhague/world-earthquake-data-from-1906-2022/download?datasetVersionNumber=2'
    path = Path(f"./data")
    od.download(url, path)
    file_path = f"data/world-earthquake-data-from-1906-2022/Global_Earthquake_Data.csv"
    df = pd.read_csv(file_path)
    df.to_parquet("./data/world-earthquake-data-from-1906-2022/Global_Earthquake_Data.parquet", compression="gzip")
    print(df.info())
   












if __name__ == "__main__":
    extract_to_local()