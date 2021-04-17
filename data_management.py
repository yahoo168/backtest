from self_finance_database_tool import *
import numpy as np

with open("TW_Ticker.txt") as f:
    TW_ticker_list = f.readlines()[0].split(',')
TW_ticker_list = [i.strip()+".TW" for i in TW_ticker_list]

# for ticker in TW_ticker_list:
# 	print(ticker)

db = Database("/Users/yahoo168/Documents/programming/Quant/Database")

SPX_sector_ticker_list = ["XLE", "XLP", "XLI", "XLY", "XLV", "XLF","XLK", "XLU", "XLB", "TLT"]

db.save_stockPrice_to_pkl(TW_ticker_list, country='TW')
# db.save_US_tradeDate_to_pkl()

# #更新S&P500成分股的價格資料以及交易日期變動
# def SP500_update():
#     ticker_list = db.get_ticker_list("SP500")
#     db.save_stockPrice_to_pkl(ticker_list)
#     db.save_US_tradeDate_to_pkl()

# SP500_update()

# for i in TW_ticker_list:
	# a = 
# 	print(a)
# get_data("5905_TW")