import pandas as pd
from src.storage import StorageManager
from pathlib import Path
import yaml


def main():
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)
        config = config["data"]

    csv_base_path = Path("data/raw/")
    dataset_id = config["exchange"]

    storage = StorageManager("")
    for pair in config["pairs"]:        
        table_id = f"{pair.replace('-', '_')}"

        df = pd.read_csv(csv_base_path / f"{dataset_id}_{table_id}.csv")
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        storage.upload_to_bq(df, f"{dataset_id}/{table_id}")


if __name__ == "__main__":
    main()
