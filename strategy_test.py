from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sys
from self_finance_database_tool import *
from Strategy import *
from utils import *
import json
import matplotlib.pyplot as plt

options = {
	"commission_rate":0.004,
    "initial_value":1,
    "country":"US",
    "freq":"month",
    "short_permit":False,
    "database_path":"/Users/yahoo168/Documents/programming/Quant/Database",
    "universe_name":'',}

TW_ticker_list = ['2330_TW', '2454_TW', '2317_TW', '2308_TW', '2412_TW', '1301_TW', '2303_TW', '1303_TW', '2891_TW', '3008_TW', '2882_TW', '2881_TW', '2886_TW', '1216_TW', '2884_TW', '2002_TW', '1326_TW', '3711_TW', '2885_TW', '1101_TW', '2892_TW', '2207_TW', '2382_TW', '5880_TW', '5871_TW', '2379_TW', '2357_TW', '2880_TW', '3045_TW', '2912_TW', '2887_TW', '5876_TW', '4938_TW', '2395_TW', '2883_TW', '2890_TW', '2801_TW', '6415_TW', '6505_TW', '1402_TW', '2301_TW', '4904_TW', '1102_TW', '9910_TW', '2105_TW', '6669_TW', '2408_TW']
sector_10_ticker_list = ["XLE", "GDX", "DIA", "XLY", "XLP", "XLV", "XLF","XLK", "XLU", "VNQ", "TLT"]
SPX_sector_ticker_list = ["XLE", "XLP", "XLI", "XLY", "XLV", "XLF","XLK", "XLU", "XLB", "TLT"]
ARC_ticker_list = ["EMB","HYG","SPY", "TLT"]
single_ticker = ["SPY", "AAPL", "OXY", "TLT"]
# single_ticker = ["AAPL","SPY", "TLT"]
# single_ticker = ["AAPL", "TLT"]

universe_ticker_list = single_ticker
strategy_kind = ARC_advance

db = Database(options["database_path"])
start_date = "2008-01-01"
end_date = "2019-01-01"

adj_start_date = db.get_next_tradeDate(datetime.strptime(start_date, "%Y-%m-%d"), country=options["country"])
adj_end_date = db.get_next_tradeDate(datetime.strptime(end_date, "%Y-%m-%d"), country=options["country"])

universe_data_df_dict = dict()
universe_data_df_dict["adjclose"]= db.get_universe_df(universe_ticker_list, data_type="adjclose", end_date=adj_end_date, data_format="all")


strategy = strategy_kind(adj_start_date, adj_end_date, universe_data_df_dict, options=options)
strategy = strategy_kind(adj_start_date, adj_end_date, universe_data_df_dict, options=options)
strategy.reset()

weight_df = strategy.cal_weight()
print()
print(weight_df)

# weight_df.plot(legend=True)
# plt.show()