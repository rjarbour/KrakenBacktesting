import plotly.plotly as py
import plotly as plotly
import plotly.graph_objs as go
import pandas as pd
from datetime import datetime
import pandas_datareader.data as web
from plotly.graph_objs import Layout
import urllib.request, json
import analysislib as al
import time
import os.path

kraken_trade_struct = ['price', 'volume', 'date', 'type', 'market_limit', 'misc']
#candle_struct = ['open', 'close', 'high', 'low', 'date']
trades = {}

last = 0
try:
	while True:
		print('https://api.kraken.com/0/public/Trades?pair=XBTUSD&since='+str(last))
		with urllib.request.urlopen('https://api.kraken.com/0/public/Trades?pair=XBTUSD&since='+str(last)) as url:
		#with urllib.request.urlopen('https://poloniex.com/public?command=returnChartData&currencyPair=BTC_ETH&start='+str(start_epoch)+'&end='+str(end_epoch)+'&period='+str(low_interval)) as url:
			data = json.loads(url.read().decode())
			if(len(data['error']) != 0):
				if 'Unavailable' in str(data['error']):
					print('service drop...')
					time.sleep(2)
					continue
				print('error!', data['error'])
				trade_df = pd.DataFrame.from_dict(data=trades)
				trade_df.to_pickle('kraken_XBTUSD.pickle')
				print('pickle saved!')
				exit()

			print('processing time from', datetime.fromtimestamp(float(data['result']['XXBTZUSD'][0][2])))
			for obj in data['result']['XXBTZUSD']:
				cur_trade = dict(zip(kraken_trade_struct, obj))
				cur_time = datetime.fromtimestamp(float(cur_trade['date']))
				trades[cur_time] = cur_trade

			if data['result']['last']:
				if not data['result']['last'] == last:
					last = data['result']['last']
				else:
					print('last occured...',last)
					trade_df = pd.DataFrame.from_dict(data=trades)
					trade_df.to_pickle('kraken_XBTUSD.pickle')
					print('pickle saved!')
					exit()
			else:
				print('last occured...',last)
				trade_df = pd.DataFrame.from_dict(data=trades)
				trade_df.to_pickle('kraken_XBTUSD.pickle')
				print('data pickled!')
				exit()
		time.sleep(2)
except Exception as e:
	print('oops',e)

finally:
	trade_df = pd.DataFrame.from_dict(data=trades)
	trade_df.to_pickle('kraken_XBTUSD.pickle')

