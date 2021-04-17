from self_finance_database_tool import *
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from math import sqrt
from pyrb import EqualRiskContribution
from .strategy_interface import *
import sys

class Momentum(Strategy):
	def reset(self):
		self.strategy_name = "Momentum"
		self.leverage = 1

	def cal_weight(self):
		change_date_list = self._get_change_tradeDate_list(self.freq)
		price_df = self.universe_data_df_dict["adjclose"]
		price_df = self._slice_df(self.start_date, self.end_date, price_df, rolling_period=100)
		ticker_list = price_df.columns		
		weight_df = pd.DataFrame(index=change_date_list, columns=ticker_list)

		for index, change_date in enumerate(change_date_list, 1):
			percentage = 100*(index / len(change_date_list))
			sys.stdout.write('\r'+self.strategy_name+" 權重計算完成度{percentage:.2f}%".format(percentage=percentage))
			cur_price_df = price_df[price_df.index <= change_date][:-1]
			cur_price = cur_price_df.iloc[-1]
			last_price = cur_price_df.iloc[-2]
			sma_short = cur_price_df[-5:].mean()
			sma_long = cur_price_df[-22:].mean()
			short_higher_than_long = (sma_short > sma_long)
			short_higher_than_long = short_higher_than_long.astype(int)
			weight_df.loc[change_date,:] = short_higher_than_long
			
		count = weight_df.sum(axis=1)
		weight_df = weight_df.div(count, axis=0)
		weight_df.fillna(0, inplace=True)
		
		return weight_df * self.leverage

#多頭排列
class Momentum2(Strategy):
	def reset(self):
		self.strategy_name = "Momentum2"
		self.leverage = 1

	def cal_weight(self):
		change_date_list = self._get_change_tradeDate_list(self.freq)
		price_df = self.universe_data_df_dict["adjclose"]
		price_df = self._slice_df(self.start_date, self.end_date, price_df, rolling_period=100)
		ticker_list = price_df.columns		
		weight_df = pd.DataFrame(index=change_date_list, columns=ticker_list)

		for index, change_date in enumerate(change_date_list, 1):
			percentage = 100*(index / len(change_date_list))
			sys.stdout.write('\r'+self.strategy_name+" 權重計算完成度{percentage:.2f}%".format(percentage=percentage))
			cur_price_df = price_df[price_df.index <= change_date][:-1]
			sma_week = cur_price_df[-5:].mean()
			sma_month = cur_price_df[-22:].mean()
			sma_quarter = cur_price_df[-64:].mean()
			short_higher_than_long = (sma_week > sma_month) & (sma_month > sma_quarter)
			weight_df.loc[change_date,:] = short_higher_than_long.astype(int)
			
		count = weight_df.sum(axis=1)
		weight_df = weight_df.div(count, axis=0)
		weight_df.fillna(0, inplace=True)
		
		return weight_df * self.leverage

#黃金交叉
class Momentum3(Strategy):
	def reset(self):
		self.strategy_name = "Momentum3"
		self.leverage = 1

	def cal_weight(self):
		change_date_list = self._get_change_tradeDate_list(self.freq)
		price_df = self.universe_data_df_dict["adjclose"]
		price_df = self._slice_df(self.start_date, self.end_date, price_df, rolling_period=100)
		ticker_list = price_df.columns		
		weight_df = pd.DataFrame(index=change_date_list, columns=ticker_list)

		for index, change_date in enumerate(change_date_list, 1):
			percentage = 100*(index / len(change_date_list))
			sys.stdout.write('\r'+self.strategy_name+" 權重計算完成度{percentage:.2f}%".format(percentage=percentage))
			cur_price_df = price_df[price_df.index <= change_date][:-1]
			
			sma_week_last = cur_price_df[-6:-1].mean()
			sma_month_last = cur_price_df[-23:-22].mean()
			sma_week_cur = cur_price_df[-5:].mean()
			sma_month_cur = cur_price_df[-22:].mean()

			golden_cross = (sma_week_cur > sma_month_cur) & (sma_week_last < sma_month_last)
			print(golden_cross)
			weight_df.loc[change_date,:] = golden_cross.astype(int)
			
		count = weight_df.sum(axis=1)
		weight_df = weight_df.div(count, axis=0)
		weight_df.fillna(0, inplace=True)
		
		return weight_df * self.leverage