from yahoo_fin.stock_info import *
from fredapi import Fred
import time
from threading import Thread, Lock
import pandas as pd
import pickle
import os
import sys
from .utils import *

class Database(object):
    def __init__(self, folder_path):
        self.database_folder_path = folder_path
        self.stock_folder_name = "Stock"
        self.tradedate_folder_name = "TradeDate"
        self.ticker_folder_name = "Ticker"
        self.test_folder_name = "Test"
        self.macro_folder_name = "Macro"
        self.macro_token_trans_folder_name = "Token_trans"
        self.cache_dict = {}

    ## Get data part starts
    #取得標的之價格資料，返回df
    def get_stock_data(self, ticker, auto_download=False, show_warning=True):
        folder_name = self.stock_folder_name
        ticker_for_downloadFile = ticker.replace('.', '_')
        file_position = os.path.join(self.database_folder_path, folder_name, ticker_for_downloadFile+".pkl")
        
        if os.path.isfile(file_position):
            stockPrice = pd.read_pickle(file_position)
            return stockPrice

        else:
            if show_warning:
                print(ticker, ": Data has not been downloaded.")
            
            if auto_download == True:
                if show_warning:
                    print(ticker, ": Try to fill the data...")
                self.save_stockPrice_to_pkl([ticker])
                stockPrice = pd.read_pickle(file_position)
                return stockPrice
            return None
            
    #取得指數的構成ticker
    #name的選項：SP500, NASAQ, TW_0050
    def get_ticker_list(self, name):
        folder_name = self.ticker_folder_name
        file_position = os.path.join(self.database_folder_path, folder_name, name+"_ticker_list.pkl")
        
        if os.path.isfile(file_position):
            with open(file_position, 'rb') as f:
                ticker_list = pickle.load(f)
                f.close()
            return ticker_list
        
        else:
            print(name, "：資料尚未下載")
            return []
    
    def get_tradeDate_list(self, country="US"):
        folder_name=self.tradedate_folder_name
        file_position = os.path.join(self.database_folder_path, folder_name, country+"_trade_date.pkl")
        tradeDate_list = pd.read_pickle(file_position)
        tradeDate_list = tradeDate_list.to_list()
        self.cache_dict["tradeDate_list"] = tradeDate_list
        return tradeDate_list

    def get_next_tradeDate(self, date, country="US"):
        if "tradeDate_list" in self.cache_dict.keys():
            tradeDate_list = self.cache_dict["tradeDate_list"]
        else:
            tradeDate_list = self.get_tradeDate_list(country=country)

        if date not in tradeDate_list:
            virtual_date_list = tradeDate_list.copy()
            virtual_date_list.append(date)
            virtual_date_list.sort()
            latest_tradeDate_index = virtual_date_list.index(date)
            return tradeDate_list[latest_tradeDate_index]
        else:
            return date

    def get_last_tradeDate(self, date, country="US"):
        if "tradeDate_list" in self.cache_dict.keys():
            tradeDate_list = self.cache_dict["tradeDate_list"]
        else:
            tradeDate_list = self.get_tradeDate_list(country=country)

        if date not in tradeDate_list:
            virtual_date_list =tradeDate_list.copy()
            virtual_date_list.append(date)
            virtual_date_list.sort()        
            latest_tradeDate_index = virtual_date_list.index(date)
            return tradeDate_list[latest_tradeDate_index-1]
        else:
            return date

    def get_fred_data(self, name):
        folder_path = os.path.join(self.database_folder_path, self.macro_folder_name)
        file_position = os.path.join(folder_path, name+".pkl")
        data_df = pd.read_pickle(file_position)  
        return data_df

    # 建構Universe的價格資料，合併成完整的df
    # 從回測模組(save_data_to_pkl)複製而來的函數
    def get_universe_df(self, universe_ticker_list, data_type="adjclose", start_date="1900-01-01", end_date="2100-12-31", data_format="all"):
        universe_df_list = []
        print("["+data_type + "] Data_df of universe is merging...")
        for index, ticker in enumerate(universe_ticker_list, 1):
            ticker_df = self.get_stock_data(ticker)
            ticker_df_specific_data = ticker_df.loc[:,[data_type]]
            universe_df_list.append(ticker_df_specific_data.rename(columns={data_type:ticker}))       
            percentage = 100*index/len(universe_ticker_list)
            sys.stdout.write('\r'+"資料合併完成度{percentage:.2f}%".format(percentage=percentage))
        print()

        universe_df = pd.concat(universe_df_list, axis=1)
        sliced_universe_df = self._slice_df(universe_df, start_date, end_date, data_format)
        return sliced_universe_df

    def _slice_df(self, df, start_date, end_date, data_format):
        df = df[df.index >= start_date]
        df = df[df.index <= end_date]
        #僅擷取日期後作ffill回傳，開頭可能有大量空值
        if data_format == "all":
            df.ffill(inplace=True, axis ='rows')
            
        #只留下start_date當日有資料的ticker，當日不存在的資料即全數刪除
        elif data_format == "only_exist_ticker":
            df.ffill(inplace=True, axis ='rows')
            df.dropna(inplace=True, axis='columns')
            df.dropna(inplace=True, axis='rows')

        #延後start_date至所有ticker的資料皆存在的那天
        elif data_format == "all_ticker_latest":
            df.ffill(inplace=True, axis ='rows')
            df.dropna(inplace=True, axis='rows')
            df.dropna(inplace=True, axis='columns')

        return df
    
    ## Get data part ends.
    ## Save data part starts.
    
    # yahoo_finance的國家代碼間隔號使用‘.’，但存檔時一律用‘_’
    # yahoo_finance的股類分隔號使用'-'，如：rds-b

    def _save_stockPrice(self, ticker, folder_name, country):
        if country == "US":
            ticker_for_search = ticker.replace('.', '-')

        elif country =="TW":
            ticker_for_search = ticker.replace('_', '.')
        
        ticker_for_saveFile = ticker.replace('.', '_')
        file_position = os.path.join(self.database_folder_path, folder_name, ticker_for_saveFile+".pkl")
        price_data = _download_data_from_yahoo(ticker_for_search)
        price_data.to_pickle(file_position)

    def save_stockPrice_to_pkl(self, ticker_list, country="US"):
        folder_name = self.stock_folder_name
        threads = []  # 儲存線程以待關閉
        num_completed = 0
        try:
            start_time = time.time()
            for index, ticker in enumerate(ticker_list, 1):
                t = Thread(target=self._save_stockPrice, args=(ticker, folder_name, country))
                t.start()  # 開啟線程
                threads.append(t)
                num_completed +=1
                percentage = 100*round(index/len(ticker_list), 2)            
                print("{ticker:<6} 股價資料下載中，完成度{percentage}%".format(ticker=ticker, percentage=percentage))                
            # 關閉線程
            for t in threads:
                t.join()
        
        except Exception as e:
            print(e)

        finally:    
            end_time = time.time()
            print("總共下載了{}筆".format(num_completed))
            print('共耗費{:.3f}秒'.format(end_time - start_time))

    def save_US_tradeDate_to_pkl(self):
        SPX = _download_data_from_yahoo("^GSPC")
        trade_date = SPX.index.to_series()
        folder_name=self.tradedate_folder_name
        file_position = os.path.join(self.database_folder_path, folder_name, "US_trade_date.pkl")
        trade_date.to_pickle(file_position)
        print("US TradeDate has been saved. (Referenced by SPX 500)")
        return trade_date

    def save_TW_tradeDate_to_pkl(self):
        TW_0050 = _download_data_from_yahoo("0050.TW")
        trade_date = TW_0050.index.to_series()
        folder_name=self.tradedate_folder_name
        file_position = os.path.join(self.database_folder_path, folder_name, "TW_trade_date.pkl")
        trade_date.to_pickle(file_position)
        print("TW TradeDate has been saved. (Referenced by TW_0050)")
        return trade_date

    #待改，尋找自動抓取的來源
    def save_TW50_ticker_list(self):
        folder_name=self.ticker_folder_name
        ticker_list = TW_ticker_list = ['2330_TW', '2454_TW', '2317_TW', '2308_TW', '2412_TW', '1301_TW', '2303_TW', '1303_TW', '2891_TW', '3008_TW', '2882_TW', '2881_TW', '2886_TW', '1216_TW', '2884_TW', '2002_TW', '1326_TW', '3711_TW', '2885_TW', '1101_TW', '2892_TW', '2207_TW', '2382_TW', '5880_TW', '5871_TW', '2379_TW', '2357_TW', '2880_TW', '3045_TW', '2912_TW', '2887_TW', '5876_TW', '4938_TW', '2395_TW', '2883_TW', '2890_TW', '2801_TW', '6415_TW', '6505_TW', '1402_TW', '2301_TW', '4904_TW', '1102_TW', '9910_TW', '2105_TW', '6669_TW', '2408_TW']
        file_position = os.path.join(self.database_folder_path, folder_name, "TW50_ticker_list.pkl")
        
        with open(file_position, 'wb') as f:
            pickle.dump(ticker_list, f)
            f.close()
        
        print("TW50 tickers has been saved.")
        return ticker_list

    def save_sp500_ticker_list(self):
        folder_name=self.ticker_folder_name
        ticker_list = _download_sp500_ticker_list_from_yahoo()
        file_position = os.path.join(self.database_folder_path, folder_name, "SP500_ticker_list.pkl")
        
        with open(file_position, 'wb') as f:
            pickle.dump(ticker_list, f)
            f.close()
        
        print("SP500 tickers has been saved.")
        return ticker_list

    def save_nasdaq_ticker_list(self):
        folder_name=self.ticker_folder_name
        ticker_list = _download_nasdaq_ticker_list_from_yahoo()
        file_position = os.path.join(self.database_folder_path, folder_name, "NASDAQ_ticker_list.pkl")
        
        with open(file_position, 'wb') as f:
            pickle.dump(ticker_list, f)
            f.close()

        print("NASDSQ tickers has been saved.")
        return ticker_list

    def save_fred_data(self, name):
        folder_path = os.path.join(self.database_folder_path, self.macro_folder_name)
        token_trans_folder_name = os.path.join(folder_path, self.macro_token_trans_folder_name)
        
        api_key = token_trans("API_Key", source="fred", folder_path=token_trans_folder_name)
        data_key = token_trans(name, source="fred", folder_path=token_trans_folder_name)
        
        fred = Fred(api_key=api_key)
        
        try:
            data_df = fred.get_series(data_key)

            print(name, " has been saved.")
        
        except Exception as e:
            print(e)
            print(name, " doesn't exist in token trans table.")

        file_position = os.path.join(folder_path, name+".pkl")
        data_df.to_pickle(file_position)
        return data_df

# 資料爬蟲包裝區
# 抓取資料的底層使用yahoo_fin的模組，以下將其包裝起來，以方便日後修改
def _download_data_from_yahoo(ticker):
    return get_data(ticker)

def _download_sp500_ticker_list_from_yahoo():
    return tickers_sp500()

def _download_nasdaq_ticker_list_from_yahoo():
    return tickers_nasdaq()
    