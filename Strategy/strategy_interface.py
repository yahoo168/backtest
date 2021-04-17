from self_finance_database_tool import *
import pandas as pd
import numpy as np
from math import sqrt
from datetime import datetime, timedelta

class Strategy(object):
	def __init__(self, start_date, end_date, universe_data_df_dict=None, options=None):
		self.strategy_name = None
		self.start_date = start_date
		self.end_date = end_date
		self.universe_data_df_dict = universe_data_df_dict
		self.options = options
		self.freq = self.options["freq"]
		self.hyperParameters_dict = self.options["hyperParameters_dict"]
		self.db = Database(self.options["database_path"])

	def _get_change_tradeDate_list(self, rebalance_freq):
		tradeDate_list = self.db.get_tradeDate_list(country=self.options["country"])
		start_date_index = tradeDate_list.index(self.start_date)
		end_date_index = tradeDate_list.index(self.end_date)
		Backtest_period_tradeDate_list = tradeDate_list[start_date_index:end_date_index+1]
		change_date_list = []

		#不調倉，買入即持有至回測結束
		if rebalance_freq=="none":
			pass

		#週調倉，每週一若為交易日則調倉，若遇國定休假則順延下一週
		elif rebalance_freq=="week":
			for date in Backtest_period_tradeDate_list:
				if date.weekday()==0:
					change_date_list.append(date)
				
		#月調倉，若前後日的月份不同，即為該月第一個交易日
		elif rebalance_freq=="month":
			for i in range(len(Backtest_period_tradeDate_list)):
				if Backtest_period_tradeDate_list[i].month != Backtest_period_tradeDate_list[i-1].month:
					change_date_list.append(Backtest_period_tradeDate_list[i])

		# 季調，待補
		elif rebalance_freq=="quarter":
			pass

		if self.start_date not in change_date_list:
			change_date_list.append(self.start_date)

		if self.end_date not in change_date_list:
			change_date_list.append(self.end_date)

		change_date_list.sort()
		return change_date_list
	
	def get_weight_df_from_other(self, strategyClass):
		strategy = strategyClass(self.start_date, self.end_date, self.universe_data_df_dict, self.options)
		strategy.reset()
		return strategy.cal_weight()

	def _get_backtest_period_tradeDate_list(self):
		tradeDate_list = self.db.get_tradeDate_list(country=self.options["country"])
		start_date_index = tradeDate_list.index(self.start_date)
		end_date_index = tradeDate_list.index(self.end_date)
		Backtest_period_tradeDate_list = tradeDate_list[start_date_index:end_date_index+1]
		return Backtest_period_tradeDate_list

	#與Backtest對接的接口，統一返回調倉日與該日的權重(df)
	def cal_weight(self):
		change_date_list=self._get_change_tradeDate_list(self.freq)
		weight_df = pd.DataFrame(index=change_date_list)
		return weight_df

	#待改
	#刪除不想顯示在回測紀錄的參數
	def get_infomation_parameters_dict(self):
		delete_key_list = ["db", "universe_data_df_dict", "database_path", "start_date", "end_date"]
		parameters_dict = self.__dict__
		
		for key in delete_key_list:
			parameters_dict.pop(key, None)
		
		return parameters_dict