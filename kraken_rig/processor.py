import pickle
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


class processor():
	
	def __init__(self, market, start_date, end_date, runner=None, frequency=60.0):
		self.df = pd.read_pickle('kraken_'+market+'.pickle')
		self.start_date = start_date
		self.end_date = end_date
		self.frequency = frequency
		self.market = market
		self.runner = runner
		self.kraken_trade_struct = ['price', 'volume', 'date', 'type', 'market_limit', 'misc']
		self.current_candle = {'start': None, 'end': None, 'open': None, 'close': None, 'high': None, 'low': None}
		
	def initialize(self):
		self.df = self.df.T
		print(self.df.shape)
		#print('it worked?')
		start = float(datetime.strptime(self.start_date, "%Y-%m-%d %H:%M:%S.%f").timestamp())
		end = float(datetime.strptime(self.end_date, "%Y-%m-%d %H:%M:%S.%f").timestamp())
		self.df = self.df.loc[self.start_date:self.end_date]
		print(self.df.shape)
		self.current_candle['start'] = start
		self.current_candle['end'] = start + self.frequency

	def run(self):
		self.initialize()
		for index, row in self.df.iterrows():
			self.candle_substep(row)
			if str(row['market_limit']) == 'l' or str(row['market_limit']) == 'm':
				row['price'] = float('%.8g' % row['price'])
				row['volume'] = float('%.8g' % row['volume'])
				self.runner.process_delegator(row, 'closed_trade')

	def candle_substep(self, row):
		if row['price'] is None:
			return
		row['price'] = float(row['price'])
		row['volume'] = float(row['volume'])
		cc = self.current_candle
		#print(row['date'])
		if cc['end'] < row['date']:
			if cc['low'] > row['price']:
				cc['low'] = row['price']
			if cc['high'] < row['price']:
				cc['high'] = row['price']
			cc['close'] = float(row['price'])
			cc['high'] = float('%.8g' % cc['high'])
			cc['low'] = float('%.8g' % cc['low'])
			cc['close'] = float('%.8g' % cc['close'])
			cc['open'] = float('%.8g' % cc['open'])
			self.runner.process_delegator(cc, 'candle')
			self.current_candle = {'start': cc['end'], 'end': cc['end'] + self.frequency, 'open': cc['close'], 'close': None, 'high': cc['close'], 'low': cc['close']}
			return
		if not cc['open']:
			cc['open'], cc['high'], cc['low'] = row['price'], row['price'], row['price']

		if cc['low'] > row['price']:
			cc['low'] = row['price']
		if cc['high'] < row['price']:
			cc['high'] = row['price']
		cc['close'] = row['price']