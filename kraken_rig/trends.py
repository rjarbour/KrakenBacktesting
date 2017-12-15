import pandas as pd
from kraken_rig.analysis import analysis


class sma(analysis):
    def __init__(self, period=None, frame_input=None, col_input=None, name=None):
        """
        A simple moving average trend object, which is intended to provide a simple moving average over every
        step of the trading test, and be recorded for later analysis.

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
        if col_input is None or col_input not in list(frame_input.columns.values):
            assert ("you have to pass the specific column of of your dataframe's input for {0}".format(name))
        if name is None:
            self.name = "sma{0}".format(period)
        else:
            self.name = name
        self.period = period
        self.frame_source = frame_input
        self.target_column = col_input
        self.frame_output = pd.DataFrame(columns=[name])

    def step(self, cur_time):
        self.frame_output.at[cur_time, self.name] = \
        self.frame_source[self.target_column].rolling(window=self.period).mean().iloc[-1]

    def __getitem__(self, index):
        return self.frame_source[self.target_column].iloc[index]

    def get_frame_output(self):
        return self.frame_output


"""
class ema(analysis):
    def __init__(self, period=None, series_input=None, name=None):

        if period is None:
            assert("pass period key value pair to sma")
        if series_input is None or series_input is not pd.core.series.Series:
            assert("pass pandas series key value pair to sma")
        if name is None:
            self.name = "sma{0}".format(period)
        else:
            self.name = name
        self.period = period
        self.series_source = series_input
        self.series_output = pd.core.series.Series

    def step(self):
        pass
"""
