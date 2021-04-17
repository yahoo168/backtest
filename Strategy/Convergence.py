from .strategy_interface import *
from self_finance_database_tool import *
import pandas
import matplotlib.pyplot as  plt

class Con_by_level(Strategy):
	def reset(self):
		self.strategy_name = "Con_by_level_ANGL"
		self.past_parameter = 90
		
	def get_yield_index(self):
		B_Yield_df = self.db.get_fred_data("B_Yield")
		BB_Yield_df = self.db.get_fred_data("BB_Yield")
		BBB_Yield_df = self.db.get_fred_data("BBB_Yield")

		frame = {'B_yield': B_Yield_df, 'BB_yield': BB_Yield_df,'BBB_yield': BBB_Yield_df} 
		bond_Yield_df = pd.DataFrame(frame)

		bond_Yield_df = bond_Yield_df.dropna()
		bond_Yield_df['BB_minus_BBB'] = bond_Yield_df.apply(lambda x: x.BB_yield-x.BBB_yield, axis=1)
		bond_Yield_df['B_minus_BB'] = bond_Yield_df.apply(lambda x: x.B_yield-x.BB_yield, axis=1)
		bond_Yield_df['Raw_Index'] = bond_Yield_df.apply(lambda x: x.B_minus_BB-x.BB_minus_BBB, axis=1)

		LONG_CONVERGENCE_DAYS = 5
		SHORT_CONVERGENCE_DAYS = 60

		short_conver_index = bond_Yield_df["Raw_Index"].rolling(SHORT_CONVERGENCE_DAYS).mean()
		long_conver_index = bond_Yield_df["Raw_Index"].rolling(LONG_CONVERGENCE_DAYS).mean()
		
		index_series = short_conver_index - long_conver_index
		bond_Yield_df["Index"] = index_series
		shifted_index_series = index_series.shift(1)
		shifted_index_series.dropna(inplace=True)
		
		return shifted_index_series

	def cal_weight(self):
		
		change_date_list = self._get_change_tradeDate_list(self.freq)
		index_series = (self.get_yield_index())[change_date_list]
		weight_df = pd.DataFrame(index=change_date_list, columns=["HYG", "TLT"])
		HY_weight_series = index_series
		
		level_1_mask = (HY_weight_series > 0.5)
		level_2_mask = ((HY_weight_series > 0.4) & (HY_weight_series <= 0.5))
		level_3_mask = ((HY_weight_series > 0.3) & (HY_weight_series <= 0.4))
		level_4_mask = ((HY_weight_series > 0.2) & (HY_weight_series <= 0.3))
		level_5_mask = ((HY_weight_series > 0) & (HY_weight_series <= 0.2))
		level_6_mask = (HY_weight_series < 0)
		
		HY_weight_series[level_1_mask] = 1
		HY_weight_series[level_2_mask] = 0.7
		HY_weight_series[level_3_mask] = 0.5
		HY_weight_series[level_4_mask] = 0.4
		HY_weight_series[level_5_mask] = 0.3
		HY_weight_series[level_6_mask] = 0.2

		TLT_weight_series = 1 - HY_weight_series
		
		weight_df["HYG"] = HY_weight_series
		weight_df["TLT"] = TLT_weight_series
		return weight_df