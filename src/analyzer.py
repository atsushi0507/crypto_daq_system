import pandas as pd
import pandas_ta as ta
from matplotlib import pyplot as plt


class Analyzer:
    def __init__(self, df: pd.DataFrame, config: dict, timezone: str = "Asia/Tokyo"):
        self.config = config["analysis"]
        self.indicators_list = self.config["indicators"]

        # タイムスタンプを timezone 変換
        df["timestamp"] = pd.to_datetime(
            df["time"], unit="s", utc=True
        ).dt.tz_convert(timezone).dt.tz_localize(None)

        # interval / lookback の適用
        interval = self.config.get("interval", None)   # 例: "15min"
        self.lookback = self.config.get("lookback", None)   # 例: 200

        if interval:
            df = self._resample_ohlcv(df, interval)

        self.df = df

    def _resample_ohlcv(self, df: pd.DataFrame, interval: str) -> pd.DataFrame:
        """OHLCV を指定 interval にリサンプリング"""
        df = df.set_index("timestamp")
        ohlcv = df.resample(interval).aggregate({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volumeto": "sum",
            "volumefrom": "sum"
        }).dropna()
        ohlcv = ohlcv.reset_index()
        return ohlcv

    def _apply_lookback(self, df: pd.DataFrame, lookback: int) -> pd.DataFrame:
        """直近 lookback 件に制限"""
        return df.tail(lookback)

    def add_indicators(self) -> pd.DataFrame:
        if self.indicators_list.get("EMA", False):
            self.df["EMA_12"] = ta.ema(self.df["close"], length=12)
            self.df["EMA_26"] = ta.ema(self.df["close"], length=26)
        if self.indicators_list.get("RSI", False):
            self.df["RSI"] = ta.rsi(self.df["close"], length=14)
        if self.indicators_list.get("MACD", False):
            macd = ta.macd(self.df["close"], fast=12, slow=26, signal=9)
            self.df = pd.concat([self.df, macd], axis=1)

            # ゴールデンクロス / デッドクロス 判定
            cross_buy = (self.df["MACD_12_26_9"] > self.df["MACDs_12_26_9"]) & \
                        (self.df["MACD_12_26_9"].shift(1) <= self.df["MACDs_12_26_9"].shift(1))
            cross_sell = (self.df["MACD_12_26_9"] < self.df["MACDs_12_26_9"]) & \
                        (self.df["MACD_12_26_9"].shift(1) >= self.df["MACDs_12_26_9"].shift(1))
            self.df["MACD_cross_buy"] = cross_buy
            self.df["MACD_cross_sell"] = cross_sell
        if self.indicators_list.get("BB", False):
            bb = ta.bbands(self.df["close"], length=20, std=2)
            self.df = pd.concat([self.df, bb], axis=1)
        return self.df
    
    def plot(self, filename: str = "plot.png"):
        df_plot = self._apply_lookback(self.df, self.lookback) if self.lookback else self.df
        indicators = self.indicators_list
        subplot_count = 1

        if indicators.get("RSI", False):
            subplot_count += 1
        if indicators.get("MACD", False):
            subplot_count += 1

        fig, axes = plt.subplots(
            subplot_count, 1, figsize=(12, 8), sharex=True,
            gridspec_kw={"height_ratios": [2] + [1] * (subplot_count - 1)}
        )

        # subplot が 1 つだけなら axes をリスト化
        if subplot_count == 1:
            axes = [axes]

        ax_price = axes[0]

        # ---- Price Chart ----
        ax_price.plot(df_plot["timestamp"], df_plot["close"], label="Close")
        if indicators.get("EMA", False):
            ax_price.plot(df_plot["timestamp"], df_plot["EMA_12"], label="EMA12")
            ax_price.plot(df_plot["timestamp"], df_plot["EMA_26"], label="EMA26")
        if indicators.get("BB", False):
            if {"BBL_20_2.0", "BBM_20_2.0", "BBU_20_2.0"} <= set(self.df.columns):
                ax_price.plot(df_plot["timestamp"], df_plot["BBL_20_2.0"], label="BB Lower", linestyle="--", color="gray")
                ax_price.plot(df_plot["timestamp"], df_plot["BBM_20_2.0"], label="BB Middle", linestyle="--", color="red")
                ax_price.plot(df_plot["timestamp"], df_plot["BBU_20_2.0"], label="BB Upper", linestyle="--", color="gray")
        ax_price.legend(loc="upper left")
        ax_price.set_title("Price with Indicators")

        # ---- RSI ----
        idx = 1
        if indicators.get("RSI", False):
            ax_rsi = axes[idx]
            ax_rsi.plot(df_plot["timestamp"], df_plot["RSI"], label="RSI", color="purple")
            ax_rsi.axhline(70, linestyle="--", color="red", alpha=0.5)
            ax_rsi.axhline(30, linestyle="--", color="green", alpha=0.5)
            ax_rsi.set_title("RSI")
            ax_rsi.legend(loc="upper left")
            idx += 1

        # ---- MACD ----
        if indicators.get("MACD", False):
            ax_macd = axes[idx]
            if {"MACD_12_26_9", "MACDs_12_26_9", "MACDh_12_26_9"} <= set(self.df.columns):
                ax_macd = axes[idx]
                ax_macd.plot(df_plot["timestamp"], df_plot["MACD_12_26_9"], label="MACD", color="blue")
                ax_macd.plot(df_plot["timestamp"], df_plot["MACDs_12_26_9"], label="Signal", color="orange")
                # Histogram を棒グラフで描画
                ax_macd.plot(df_plot["timestamp"], df_plot["MACDh_12_26_9"], label="Histogram", color="gray")
                # クロスシグナルを scatter で描画
                ax_macd.scatter(
                    df_plot.loc[df_plot["MACD_cross_buy"], "timestamp"],
                    df_plot.loc[df_plot["MACD_cross_buy"], "MACD_12_26_9"],
                    marker="^", color="green", label="Golden Cross", zorder=5
                )
                ax_macd.scatter(
                    df_plot.loc[df_plot["MACD_cross_sell"], "timestamp"],
                    df_plot.loc[df_plot["MACD_cross_sell"], "MACD_12_26_9"],
                    marker="v", color="red", label="Dead Cross", zorder=5
                )
            ax_macd.axhline(0, linestyle="--", color="pink")
            ax_macd.set_title("MACD")
            ax_macd.legend(loc="upper left")

        plt.tight_layout()
        plt.savefig(filename)
        plt.close()

