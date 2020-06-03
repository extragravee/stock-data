from pandas_datareader import data 
from pandas_datareader._utils import RemoteDataError
import matplotlib.pyplot as plt 
import pandas as pd 
import numpy as np 
from datetime import datetime
import mysql.connector

START_DATE = '2020-01-01'
END_DATE = '2020-01-31'

#stock to track
watchlist= ['VDHG.AX', 'Z1P.AX','APT.AX', 'CSL.AX']

def clean_data(stockdata, col):
	weekdays = pd.date_range(start = START_DATE, end = END_DATE)
	clean_data = stockdata[col].reindex(weekdays)
	return clean_data.fillna(method = 'ffill')

#query the yahoo finance api and get ticker data for (US CODES) of asx stocks
def get_data(asxcode):
	try:
		stockdata = data.DataReader(asxcode, 'yahoo',START_DATE,END_DATE)
		# print(stockdata)

	except RemoteDataError:
		print("No data found for {}", asxcode)

	stockdata['code'] = asxcode[:-3]
	return stockdata

#store all dataframes
dfs = []

#x contains stock data
x = pd.DataFrame()

#get dataframe of data for each stock
for code in watchlist:
	x = get_data(code)
	dfs.append(x)
	# print(x.dtypes, type(x.index))


'''
SQL CONNECTOR
'''
#open connection
db = mysql.connector.connect(user='root', password = '3672', host='127.0.0.1', database='stocks')
cursor = db.cursor()

pk = 0
#Insert queries
for df in dfs:
	sql = "CREATE TABLE IF NOT EXISTS prices ({})".format(' ,'.join(df.columns))
	cursor.execute(sql)