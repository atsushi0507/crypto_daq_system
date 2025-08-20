import requests
import pandas as pd
import sys


class CryptoCompareFetcher:
    BASE_URL = "https://min-api.cryptocompare.com/data/v2"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def fetch_ohlcv(self, pair: str, exchange: str, interval: str, limit: int = 2000) -> pd.DataFrame:
        """
        OHLCV データを取得し、DataFrame で返す
        columns: [timestamp, open, high, low, close, volumeto, volumefrom]
        """
        if interval == "1min":
            url = f"{self.BASE_URL}/histominute"
        elif interval == "1hour":
            url = f"{self.BASE_URL}/histohour"
        elif interval == "1day":
            url = f"{self.BASE_URL}/histoday"
        else:
            print("interval は '1min', '1hour', '1day' のいずれかを設定してください。")
            print("処理を終了します")
            sys.exit()

        params = {
            "fsym": pair.split("-")[0],
            "tsym": pair.split("-")[1],
            "e": exchange,
            "limit": limit,
            "api_key": self.api_key
        }

        try:
            res = requests.get(url, params=params)
            data = res.json()["Data"]["Data"]
            df = pd.DataFrame(data)
            df["timestamp"] = pd.to_datetime(
                df["time"], unit="s", utc=True
            )
            return df
        except Exception as e:
            print(f"Failed data taking because of {str(e)}")
            return None
