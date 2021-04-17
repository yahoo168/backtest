from self_finance_database_tool import *
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from math import sqrt
from pyrb import EqualRiskContribution
import sys

from .strategy_interface import *
from .Momentum import *
from .Riskparity import *
from .Others import *

class Mix_Momentum_RiskParity(Strategy):
	def reset(self):
		self.strategy_name = "Mix_Momentum_RiskParity"
		self.past_parameter = 90
		
	def cal_weight(self):
		Momentum_df = self.get_weight_df_from_other(Momentum)
		RiskParity_equal_df = self.get_weight_df_from_other(RiskParity_equal)
		change_date_list = self._get_change_tradeDate_list(self.freq)
		
		#判斷多空環境以計算兩策略的占比
		ADL_list = []
		
		for index, change_date in enumerate(change_date_list, 1):
			percentage = 100*(index / len(change_date_list))
			sys.stdout.write('\r'+"混合權重比例計算完成度{percentage:.2f}%".format(percentage=percentage))

			price_df = self.universe_data_df_dict["adjclose"].copy()
			past_tradeDate = self.db.get_next_tradeDate(change_date-timedelta(days=self.past_parameter))
			last_one_tradeDate = self.db.get_last_tradeDate(change_date-timedelta(days=1))
			past_price = price_df.loc[past_tradeDate,:]
			last_price = price_df.loc[last_one_tradeDate,:]
			up_or_down = last_price > past_price
			#過去區間上漲檔數佔總檔數的比例
			count = sum(up_or_down) / len(up_or_down)
			ADL_list.append(count)
		print()

		ADL_series = pd.Series(ADL_list, index=change_date_list)
		ADL_series.to_excel("TW_ADL_90_series.xlsx")
		ADL_series 
		weight_df = Momentum_df.mul(ADL_series, axis=0) + RiskParity_equal_df.mul(1-ADL_series, axis=0)
		print(weight_df)
		return weight_df

class Mix_Momentum_RiskParity_Simple(Strategy):
	def reset(self):
		self.strategy_name = "Mix_Momentum_RiskParity_Simple"
		
	def cal_weight(self):
		Momentum_df = self.get_weight_df_from_other(Momentum)
		RiskParity_equal_df = self.get_weight_df_from_other(RiskParity_equal)
		change_date_list = self._get_change_tradeDate_list(self.freq)
		weight_df = (Momentum_df+RiskParity_equal_df) / 2
		return weight_df

#結合多頭排列與簡單平均
class Mix_Momentum2_Equal_Simple(Strategy):
	def reset(self):
		self.strategy_name = "Mix_Momentum_RiskParity_Simple"
		
	def cal_weight(self):
		Momentum_df = self.get_weight_df_from_other(Momentum2)
		Equal_weight_df = self.get_weight_df_from_other(Equal_weight)
		change_date_list = self._get_change_tradeDate_list(self.freq)
		weight_df = (Momentum_df+Equal_weight_df) / 2
		return weight_df

class Mix_Momentum_Equal(Strategy):
	def reset(self):
		self.strategy_name = "Mix_Momentum_Equal"
		self.past_parameter = 90
		
	def cal_weight(self):
		Momentum_df = self.get_weight_df_from_other(Momentum)
		Equal_weight_df = self.get_weight_df_from_other(Equal_weight)
		change_date_list = self._get_change_tradeDate_list(self.freq)
		
		#判斷多空環境以計算兩策略的占比
		ADL_list = []
		print()
		for index, change_date in enumerate(change_date_list, 1):
			percentage = 100*(index / len(change_date_list))
			sys.stdout.write('\r'+"混合權重比例計算完成度{percentage:.2f}%".format(percentage=percentage))

			price_df = self.universe_data_df_dict["adjclose"].copy()
			past_tradeDate = self.db.get_next_tradeDate(change_date-timedelta(days=self.past_parameter))
			last_one_tradeDate = self.db.get_last_tradeDate(change_date-timedelta(days=1))
			past_price = price_df.loc[past_tradeDate,:]
			last_price = price_df.loc[last_one_tradeDate,:]
			up_or_down = last_price > past_price
			#過去區間上漲檔數佔總檔數的比例
			count = sum(up_or_down) / len(up_or_down)
			ADL_list.append(count)
		print()

		ADL_series = pd.Series(ADL_list, index=change_date_list)
		# ADL_series.to_excel("TW_ADL_30_series.xlsx")
		weight_df = Momentum_df.mul(ADL_series, axis=0) + Equal_weight_df.mul(1-ADL_series, axis=0)
		weight_df.fillna(0, inplace=True)
		print(weight_df)
		return weight_df