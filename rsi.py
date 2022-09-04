import pandas as pd
import numpy as np
import json
import bisect
from binance.client import Client
from binance.helpers import date_to_milliseconds, interval_to_milliseconds
import os
from os import path, listdir
from os.path import isfile
import plotly.express as px
import plotly.graph_objects as go



class Rsi:

    def __init__(self, symbol, interval, file_path="historical_ohlcv/"):

        self.symbol = symbol
        self.interval = interval
        self.file_name = file_path + "Binance_{}_{}.csv".format(
                symbol,
                interval)
        
        self.start_ms = None
        self.end_ms = None
        self.df = None
        self.rsi = None


    def read_historical_klines(self,start_str, end_str):
        start_ms = date_to_milliseconds(start_str)
        end_ms = date_to_milliseconds(end_str)
        if self.df is not None and (start_ms in self.df["_id"].values and end_ms in self.df["_id"].values):
            return self.df.loc[(df["_id"] >= start_ms) & (df["_id"] <= end_ms)]
        else:
            column_names = ["_id","open","high","low","close","volume","close_time","quote_asset_volume","number_of_trades","taker_buy_base_asset_volume",
                    "taker_buy_quote_asset_volume","ignore"]
            client = Client("api_key", "api_secret")
            if path.exists(self.file_name):
                    df = pd.read_csv(self.file_name, index_col=False)
                    if start_ms not in df["_id"].values or end_ms not in df["_id"].values:
                        output_data = client.get_historical_klines(self.symbol, self.interval, start_str, end_str)
                        json_data = json.dumps(output_data)
                        df2 = pd.read_json(json_data)
                        df2 = df2.set_axis(column_names, axis=1, inplace=False)
                        df2 = df2.iloc[:,:-3]
                        df = pd.concat([df2,df]).drop_duplicates()
                        df.sort_values(["_id"])
                        df.to_csv(file_name, index =False) 
            else:
                output_data = client.get_historical_klines(self.symbol, self.interval, start_str, end_str)
                json_data = json.dumps(output_data)
                df = pd.read_json(json_data)
                df = df.set_axis(column_names, axis=1, inplace=False)
                df = df.iloc[:,:-3]
                df.to_csv(self.file_name, index=False) 
        
        self.df = df
        return df.loc[(df["_id"] >= start_ms) & (df["_id"] <= end_ms)]

    def calculate_rsi(self, start_str, end_str, periods=14):
        # if not enough data points for period, throw error 
        self.start_ms = date_to_milliseconds(start_str)
        self.end_ms = date_to_milliseconds(end_str)

        if self.end_ms <= self.start_ms:
            raise Exception("RSI start and end time are not right!")

        df = self.read_historical_klines(start_str, end_str)
        
        if periods >= df.shape[0]:
            raise Exception("Not enough data points for period!")
        close_delta = df["close"].diff()
        up = close_delta.clip(lower=0)
        down = -1*close_delta.clip(upper=0)
        ma_up = up.ewm(com=periods-1, adjust=True, min_periods = periods).mean()
        ma_down = down.ewm(com=periods-1, adjust=True, min_periods = periods).mean()
        rs = ma_up/ma_down
        rsi = 100 - (100/(1+rs))
        self.rsi = rsi
        return rsi


    def correlation(self):
        if self.df is None:
            raise Exception("DataFrame is not initialized")
        if self.rsi is None:
            raise Exception("RSI has to be calculated first!")
        df_ = self.df.loc[(self.df["_id"] >= self.start_ms) & (self.df["_id"] <= self.end_ms)]
        if df_.shape[0] == 0:
            raise Exception("RSI start and end time are not right!")

        return self.rsi.corr(df_["volume"])

    def rsi_value_frequencies(self,lower=30, higher=70):
        if self.rsi is None:
            raise Exception("RSI has to be calculated first!")
        lo = self.rsi.loc[self.rsi < lower].count()/self.rsi.shape[0]
        hi = self.rsi.loc[self.rsi > higher].count()/self.rsi.shape[0]
        return lo, hi

    def plot_trades_histogram(self,nbins=100):
        if self.df is None:
            raise Exception("DataFrame is not initialized")
        fig = px.histogram(self.df,x="number_of_trades",nbins=nbins)
        fig.show()


    def plot_rsi(self):
        fig = px.scatter(x=self.rsi.index,y=self.rsi.values)
        fig.show()

    def plot_price(self):
        fig = go.Figure(data=[go.Candlestick(x=self.df["_id"],
                open=self.df["open"],
                high=self.df["high"],
                low=self.df["low"],
                close=self.df["close"])])

        fig.show()



symbol = "ETHBTC"
start = "1 Nov, 2017"
end = "1 Dec, 2017"
interval = Client.KLINE_INTERVAL_30MINUTE

cc = Rsi(symbol, interval)
#print(cc.symbol)
rsi = cc.calculate_rsi(start, end)
corr = cc.correlation()
frequenceies = cc.rsi_value_frequencies()
#print(rsi)
#print(corr)
#print(frequenceies)
cc.plot_price()
