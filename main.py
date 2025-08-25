import yaml
from src.fetcher import CryptoCompareFetcher
from src.storage import StorageManager
from src.analyzer import Analyzer
from src.notifier import Notifier

import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
crypto_api_key = os.getenv("CRYPTOCOMPARE_API_KEY")
channel_token = os.getenv("LINE_CHANNEL_TOKEN")
user_id = os.getenv("LINE_USER_ID")

def build_signal_message(df: pd.DataFrame) -> str:
    latest = df.iloc[-1]
    prev = df.iloc[-2]

    price = latest["close"]
    rsi = latest.get("RSI", None)
    macd_val = latest.get("MACD_12_26_9", None)
    macd_signal = latest.get("MACDs_12_26_9", None)

    # GC/DC 判定
    macd_prev = prev.get("MACD_12_26_9", None)
    macd_signal_prev = prev.get("MACDs_12_26_9", None)

    cross = "-"
    if macd_prev is not None and macd_signal_prev is not None:
        if macd_prev < macd_signal_prev and macd_val > macd_signal:
            cross = "GC"
        elif macd_prev > macd_signal_prev and macd_val < macd_signal:
            cross = "GC"
    
    message = (
        f"📈 Signal Update\n"
        f"Price: {price:,.0f} JPY\n"
        f"RSI: {rsi:.2f}\n"
        f"MACD: {macd_val:.2f}\n"
        f"Signal: {macd_signal:.2f}\n"
        f"Cross: {cross}"
    )
    return message

def main():
    with open("config/config.yaml") as f:
        config = yaml.safe_load(f)

    fetcher = CryptoCompareFetcher(api_key=crypto_api_key)
    storage = StorageManager(config["storage"]["path"])

    for pair in config["data"]["pairs"]:
        df = fetcher.fetch_ohlcv(
            pair=pair,
            exchange=config["data"]["exchange"],
            interval=config["data"]["interval"],
            limit=config["data"]["limit"]
        )

        if config["storage"]["format"] == "csv":
            filename = f"{config['data']['exchange']}_{pair.replace('-', '_')}.csv"
            storage.save_csv(df, filename)
        elif config["storage"]["format"] == "parquet":
            filename = f"{config['data']['exchange']}_{pair.replace('-', '_')}.parquet"
            storage.save_parquet(df, filename)

        df = storage.prune_old_data(df, filename, config["storage"]["rolling_days"])

    analyzer = Analyzer(df, config)
    df = analyzer.add_indicators()
    if config["analysis"]["plot"]:
        output_path = config["analysis"]["output_path"]
        os.makedirs(output_path, exist_ok=True)
        analyzer.plot(filename=f"{output_path}/{filename.replace('.csv', '.png')}")

    notifier = Notifier(
        channel_token=channel_token,
        user_id=user_id
    )
    if config["notify"]["line"]["enabled"]:
        if notifier.check_trigger(df, config["notify"]):
            msg = build_signal_message(df)
            notifier.send_line_push(message=msg)
        else:
            print("Trigger is not fired")


if __name__ == "__main__":
    main()