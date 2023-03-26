from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True, retries=3)
def extract_from_gcs() -> Path:
    """Download earthquake data from GCS"""
    gcs_path = f"earthquake_data/Global_Earthquake_Data.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path)
    return Path(f"{gcs_path}")

@task(log_prints=True)
def read(path: Path) -> pd.DataFrame:
    """Read file"""
    df = pd.read_parquet(path)
    print(df.shape)
    return df

@task(log_prints=True)
def write_to_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="world_earthquake.earthquake",
        project_id="sincere-office-375210",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="replace"
    )
    return len(df)

@flow(log_prints=True)
def el_parent_gcs_to_bq():
    path = extract_from_gcs()
    df = read(path)
    row_count = write_to_bq(df)
    return row_count


if __name__ == "__main__":
    el_parent_gcs_to_bq()
