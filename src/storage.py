import pandas as pd
import pandas_gbq
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os


class StorageManager:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)

    def save_csv(self, df: pd.DataFrame, filename: str):
        path = self.base_path / filename
        if path.exists():
            old = pd.read_csv(path, parse_dates=["timestamp"])
            df = pd.concat([old, df]).drop_duplicates("timestamp", keep="last")
        df.sort_values(by="timestamp").to_csv(path, index=False)

    def save_parquet(self, df: pd.DataFrame, filename: str):
        path = self.base_path / filename
        if path.exists():
            old = pd.read_csv(path, parse_dates=["timestamp"])
            df = pd.concat([old, df]).drop_duplicates("timestamp", keep="last")
        df.sort_values(by="timestamp").to_parquet(path, index=False)

    def prune_old_data(self, df: pd.DataFrame, filename: str, rolling_days: int) -> pd.DataFrame:
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(days=rolling_days)

        # 古いデータを削除
        pruned_df = df[df["timestamp"] >= cutoff].copy()

        # 上書き保存
        path = self.base_path / filename
        pruned_df.to_csv(path, index=False)

        return pruned_df
    
    def upload_to_bq(self, df: pd.DataFrame, table: str):
        now = datetime.now(timezone.utc)

        # 今週の 09:19
        cutoff_end = now.replace(hour=9, minute=20, second=59, microsecond=0)
        # 1 週間前の 09:20
        cutoff_start = cutoff_end - timedelta(days=7) - timedelta(minutes=1)

        mask = (df["timestamp"] >= cutoff_start) & (df["timestamp"] <= cutoff_end)
        upload_df = df.loc[mask].copy()

        if not upload_df.empty:
            upload_df = upload_df.iloc[:-1]

        if upload_df.empty:
            print("No data to upload to BQ.")
            return
        
        upload_columns = [
            "timestamp",
            "open", "high", "low", "close",
            "volumeto", "volumefrom"
        ]

        load_dotenv()
        PROJECT_ID = os.getenv("GCP_PROJECT_ID")
        pandas_gbq.to_gbq(
            upload_df[upload_columns].sort_values(by="timestamp"),
            table.replace("/", "."),
            project_id=PROJECT_ID,
            if_exists="append"
        )
