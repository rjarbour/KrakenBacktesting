from collections import OrderedDict

import pandas as pd
from datetime import datetime

class Processor:
    def __init__(self, file, market, start_date, end_date, runner=None, frequency=60.0):
        """
       the Processoris intended to be the interpreter of the historical order book data.

        :param file: The name of the pickle that has all of the orderbook data.
        :type file: str
        :param market: The particular name of the trading pair were working with.
        :type market: str
        :param start_date: The starting date for the processor to read data from in seconds since Epoch
        :type start_date: str
        :param end_date: The ending date for the processor to stop reading data to in seconds since Epoch.
        :type end_date: str
        :param runner: The runner object, which is intended as a wrapper for the user, which will pass data
        to the user when needed and make sense of the data passed from the processor.
        :type runner: kraken_rig.runner.Runner
        :param frequency: The amount of time that passes between each 'step' for the user to make decisions in seconds.
        :type frequency: float
        """
        self.df = pd.read_pickle(file)
        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        self.market = market
        self.runner = runner
        self.kraken_trade_struct = ['price', 'volume', 'date', 'type', 'market_limit', 'misc']
        self.current_candle = {'start': None, 'end': None, 'open': None, 'close': None, 'high': None, 'low': None}

    def initialize(self):
        """
        allows the user to decide when they wish to initialize the processor, just in case they want to change something
        in between creating the processor and starting it.
        """
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
        panda_frames = [x.get_frame_output() for x in self.runner.runtime_analysis.values()]
        panda_frames.append(self.runner.candles)
        self.runner.transactions.index += self.frequency
        panda_frames.append(self.runner.transactions)
        dframe = pd.concat(panda_frames, axis=1)

        dframe['date'] = [datetime.fromtimestamp(x) for x in dframe[dframe.start.notnull()].start] + [x for x in dframe[
            dframe.start.isnull()].start]
        dframe.to_pickle(path)

    def save_session_to_csv(self, path):
        panda_frames = [x.get_frame_output() for x in self.runner.runtime_analysis.values()]
        panda_frames.append(self.runner.candles)
        self.runner.transactions.index += self.frequency
        panda_frames.append(self.runner.transactions)
        dframe = pd.concat(panda_frames, axis=1)

        dframe['date'] = [datetime.fromtimestamp(x) for x in dframe[dframe.start.notnull()].start] + [x for x in dframe[
            dframe.start.isnull()].start]
        dframe.to_csv(path)

    def run(self):
        """
        The 'main loop' of the program, steps through each order book entry and sends the information off to the runner
        """
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
        """
        Intended to group the order book transactions into candles for analysis

        :param row: the current order book exchange occurring
        :type row:
        :return:
        """
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
