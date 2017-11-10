from collections import OrderedDict

import pandas as pd
from datetime import datetime


class Processor:
    def __init__(self, market, start_date, end_date, runner=None, frequency=60.0):
        self.df = pd.read_pickle('kraken_' + market + '.pickle')
        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        self.market = market
        self.runner = runner
        self.kraken_trade_struct = ['price', 'volume', 'date', 'type', 'market_limit', 'misc']
        self.current_candle = {'start': None, 'end': None, 'open': None, 'close': None, 'high': None, 'low': None}

    def initialize(self):
        self.df = self.df.T
        start = float(datetime.strptime(self.start_date, "%Y-%m-%d %H:%M:%S.%f").timestamp())
        # end = float(datetime.strptime(self.end_date, "%Y-%m-%d %H:%M:%S.%f").timestamp())
        self.df = self.df.loc[self.start_date:self.end_date]
        print(self.df.head(1).index)
        print(self.df.tail(1).index)
        print(self.df.shape)
        # for some reason my timestamps were off of what I was really working with, it was exactly 1 hour off....
        # need to fix this somehow...
        self.current_candle['start'] = start + 3600
        self.current_candle['end'] = start + 3600 + self.frequency

    def save_session_to_dataframe(self, path):
        dframe = pd.DataFrame(self.runner.additional_records)
        dframe.to_pickle(path)

    def run(self):
        self.initialize()
        for index, row in self.df.iterrows():
            self.candle_substep(row)
            if str(row['market_limit']) == 'l' or str(row['market_limit']) == 'm':
                row['price'] = float('%.8g' % row['price'])
                row['volume'] = float('%.8g' % row['volume'])
                self.runner.process_delegator(row, 'closed_trade')


                # convert our data to be ordered lists set by our dates.
                # for key in self.runner.additional_records:
                # cur_entry = self.runner.additional_records[entry]
                # print(entry+": "+type(cur_entry).__name__)

    def candle_substep(self, row):
        if row['price'] is None:
            return
        row['price'] = float(row['price'])
        row['volume'] = float(row['volume'])
        date = row['date']
        if self.current_candle['end'] < date:
            while self.current_candle['end'] < date:
                if not self.current_candle['open']:
                    self.current_candle['open'] = row['price']
                    self.current_candle['low'] = row['price']
                    self.current_candle['high'] = row['price']
                if self.current_candle['low'] > row['price']:
                    self.current_candle['low'] = row['price']
                if self.current_candle['high'] < row['price']:
                    self.current_candle['high'] = row['price']
                self.current_candle['close'] = float(row['price'])
                # I know this shows some ambiguous None type issues here but it works fine;
                # the candle's values should be floats by the time they get here
                self.current_candle['high'] = float('%.8g' % self.current_candle['high'])
                self.current_candle['low'] = float('%.8g' % self.current_candle['low'])
                self.current_candle['close'] = float('%.8g' % self.current_candle['close'])
                self.current_candle['open'] = float('%.8g' % self.current_candle['open'])
                self.runner.process_delegator(self.current_candle, 'candle')
                # print('\nCANDLE:', self.current_candle, '\n')
                self.current_candle['start'] = self.current_candle['end']
                self.current_candle['end'] = self.current_candle['end'] + self.frequency
                self.current_candle['close'] = None
                self.current_candle['open'] = None
            return
        if not self.current_candle['open']:
            self.current_candle['open'], self.current_candle['high'], self.current_candle['low'] = row['price'], row[
                'price'], row['price']

        if self.current_candle['low'] > row['price']:
            self.current_candle['low'] = row['price']
        if self.current_candle['high'] < row['price']:
            self.current_candle['high'] = row['price']
        self.current_candle['close'] = row['price']
