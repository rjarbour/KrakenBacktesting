import pandas as pd
from kraken_rig.analysis import analysis

"""
class trash(analysis):
    def __init__(self, period=None, frame_input=None, name=None, shift=0):

        A simple moving average trend object, which is intended to provide a simple moving average over every
        step of the trading test, and be recorded for later analysis.

        (high + low + close) / 3

        :param period: The window of time to roll the average across, where period is in units of the candle's period.
        IE period=3, with candle period=60.0, the sma would consider 3 minutes of data as an average.
        :type period: float
        :param frame_input: The specific series to look over and take the simple moving average of.
        :type frame_input: pandas.DataFrame
        :param name: The name of the SMA to refer to the specific trend as for later analysis.
        :type name: str
        
        if period is None or period is not int:
            assert ("pass period key value pair to sma")
        if frame_input is None or frame_input is not pd.DataFrame:
            assert ("pass pandas series key value pair to sma")
        if name is None:
            self.name = "sma{0}".format(period)
        else:
            self.name = name
        self.shift = shift
        self.period = period
        self.frame_source = frame_input
        self.targets = {'close': None, 'low': None, 'high': None}
        for key in list(self.targets.keys()):
            if key in self.frame_source.columns.values.tolist():
                self.targets[key] = key
            else:
                assert ("Missing key {0} as input for SMA trend".format(key))
        self.frame_output = pd.DataFrame(columns=[name])

    def step(self, cur_time):
        self.frame_output.at[cur_time, self.name] = (self.frame_source[self.targets['close']].rolling(
            window=self.period).mean().iloc[-1] + self.frame_source[self.targets['high']].rolling(
            window=self.period).mean().iloc[-1] + self.frame_source[self.targets['low']].rolling(
            window=self.period).mean().iloc[-1]) / 3.0

    def __getitem__(self, index):
        return self.frame_output[self.name].shift(self.shift).iloc[index]

    def get_frame_output(self):
        return self.frame_output
"""


class sma(analysis):
    def __init__(self, period=None, frame_input=None, target_column=None, name=None, shift=0):
        """
        A simple moving average trend object, which is intended to provide a simple moving average over
        every step of the trading test, and be recorded for later analysis.

        :param period: The window of time to roll the average across, where period is in units of the candle's period.
        IE period=3, with candle period=60.0, the sma would consider 3 minutes of data as an average.
        :type period: float
        :param frame_input: The specific series to look over and take the simple moving average of.
        :type frame_input: pandas.DataFrame
        :param name: The name of the SMA to refer to the specific trend as for later analysis.
        :type name: str
        """
        if period is None or period is not int:
            assert ("pass period key value pair to sma")
        if frame_input is None or frame_input is not pd.DataFrame:
            assert ("pass pandas series key value pair to sma")
        if target_column is None or target_column not in list(frame_input.columns.values):
            assert ("you have to pass the specific column of of your dataframe's input for {0}".format(name))

        if name is None:
            self.name = "sma{0}".format(period)
        else:
            self.name = name
        self.shift = shift
        self.period = period
        self.target_column = target_column
        self.frame_source = frame_input
        self.frame_output = pd.DataFrame(columns=[name])

    def step(self, cur_time):
        self.frame_output.at[cur_time, self.name] = self.frame_source[self.target_column].tail(self.period).rolling(
            window=self.period).mean().iloc[-1]

    def __getitem__(self, index):
        return self.frame_output[self.name].shift(self.shift).iloc[index]

    def get_frame_output(self):
        return self.frame_output


class ema(analysis):
    def __init__(self, period=None, frame_input=None, target_column=None, name=None, shift=0):
        """
        Am exponential moving average trend object, which is intended to provide an exponential moving average over
        every step of the trading test, and be recorded for later analysis.

        :param period: The window of time to roll the average across, where period is in units of the candle's period.
        IE period=3, with candle period=60.0, the sma would consider 3 minutes of data as an average.
        :type period: float
        :param frame_input: The specific series to look over and take the simple moving average of.
        :type frame_input: pandas.DataFrame
        :param name: The name of the SMA to refer to the specific trend as for later analysis.
        :type name: str
        """
        if period is None or period is not int:
            assert ("pass period key value pair to sma")
        if frame_input is None or frame_input is not pd.DataFrame:
            assert ("pass pandas series key value pair to sma")
        if target_column is None or target_column not in list(frame_input.columns.values):
            assert ("you have to pass the specific column of of your dataframe's input for {0}".format(name))

        if name is None:
            self.name = "sma{0}".format(period)
        else:
            self.name = name
        self.shift = shift
        self.period = period
        self.target_column = target_column
        self.frame_source = frame_input
        self.frame_output = pd.DataFrame(columns=[name])

    def step(self, cur_time):
        self.frame_output.at[cur_time, self.name] = \
            self.frame_source[self.target_column].tail(self.period).ewm(span=self.period,
                                                                        min_periods=self.period - 1).mean().iloc[-1]

    def __getitem__(self, index):
        return self.frame_output[self.name].shift(self.shift).iloc[index]

    def get_frame_output(self):
        return self.frame_output
