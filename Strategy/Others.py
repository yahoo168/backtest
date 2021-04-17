from self_finance_database_tool import *
from .strategy_interface import *
import pandas as pd

class Equal_weight(Strategy):
	"""簡單平均，所有標的平均分配"""
	def reset(self):
		self.strategy_name = "Equal_weight"
	
	def cal_weight(self):
		change_date_list = self._get_change_tradeDate_list(self.freq)
		hold_ticker_list = self.universe_data_df_dict["adjclose"].columns
		weight_df = pd.DataFrame(index=change_date_list, columns=hold_ticker_list)
		equal_weight_num = 1/len(hold_ticker_list)
		for ticker in hold_ticker_list:
			weight_df[ticker].values[:] = equal_weight_num

		return weight_df