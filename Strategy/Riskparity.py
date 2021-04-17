from self_finance_database_tool import *
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from math import sqrt
from pyrb import EqualRiskContribution
from .strategy_interface import *
import sys

class RiskParity_simple(Strategy):
	def reset(self):
		self.strategy_name = "RiskParity_simple"
	
	def cal_weight(self):
		total_vol = 0.2
		rolling_period = 252
		change_date_list = self._get_change_tradeDate_list(self.freq)
		percent_df = self.universe_data_df_dict["adjclose"].pct_change()
		volatility_df = percent_df.rolling(rolling_period).std() * sqrt(rolling_period)
		sliced_volatility_df = self._slice_df(self.start_date, self.end_date, volatility_df, rolling_period=365)
		change_date_volatility_df = sliced_volatility_df.loc[change_date_list,:]
		hold_ticker_list = sliced_volatility_df.columns
		weight_df = pd.DataFrame()
		#給定各資產權重
		target_vol = total_vol / len(hold_ticker_list)    
		for ticker in hold_ticker_list:
			weight_df[ticker] = target_vol / change_date_volatility_df[ticker]

		return weight_df

class RiskParity_equal(Strategy):
	def reset(self):
		self.strategy_name = "RiskParity_equal"
	
	def cal_weight(self):
		rolling_period = 365
		weight_df = pd.DataFrame()
		percent_df = self.universe_data_df_dict["adjclose"].pct_change()
		percent_df = self._slice_df(self.start_date, self.end_date, percent_df, rolling_period=rolling_period)
		ticker_list = percent_df.columns
		
		change_date_list = self._get_change_tradeDate_list(self.freq)
		
		weight_list = []		
		print()
		for index, date in enumerate(change_date_list, 1):			
			percentage = 100*(index / len(change_date_list))
			sys.stdout.write('\r'+self.strategy_name+" 權重計算完成度{percentage:.2f}%".format(percentage=percentage))
			s_date = date-timedelta(days=rolling_period)
			interval_df = percent_df[percent_df.index >= s_date]
			interval_df = interval_df[interval_df.index < date]
			cov_df = interval_df.cov()
			ERC = EqualRiskContribution(cov_df)
			ERC.solve()
			weight_list.append(ERC.x)
		print()
		weight_df = pd.DataFrame(weight_list, columns=ticker_list, index=change_date_list)

		return weight_df