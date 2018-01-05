import pandas as pd
from datetime import datetime
import kraken_rig as kr
from kraken_rig import trends


class ThreeLines(kr.runner.Runner):
    def __init__(self, currency_offered, currency_sought, wallet):
        super().__init__(currency_offered, currency_sought, wallet)
        self.context['initial'] = 0
        self.currency_offered = currency_offered
        self.currency_sought = currency_sought
        self.context['invested'] = False
        self.context['buy_orders'] = []
        self.context['sell_orders'] = []
        self.stop = None

    def process_historical_trade_event(self, row):
        # this garbage trade strat throws away more refined data...
        pass

    def process_candle(self):
        # initialize our trends; the longest one to finish is the 144 period ema
        i = self.context['initial']
        if i < 144:
            self.context['initial'] += 1
            return

        # fetch our important information for trade logic...
        # current buy orders..
        cur_buy_orders = self.context['buy_orders']
        # current sell orders..
        cur_sell_orders = self.context['sell_orders']
        # the most recent and previous ema's
        ema3 = self.runtime_analysis['ema3'][-1]
        prev_ema3 = self.runtime_analysis['ema3'][-2]
        ema13 = self.runtime_analysis['ema13'][-1]
        prev_ema13 = self.runtime_analysis['ema13'][-2]
        ema144 = self.runtime_analysis['ema144'][-1]
        # our wallet balance...
        balance = self.wallet.balance
        # the most recent candle
        cc = self.candles.iloc[-1]
        # the close value of the recent candle for ease...
        close = cc['close']
        # the previous close of our candles..
        prev_close = self.candles.iloc[-2]['close']

        # trade logic
        # check if we are invested already, or if we have already placed an order
        if not self.context['invested'] and len(cur_buy_orders) == 0:
            # make sure all of our ema's are in order from fastest to slowest upward
            if (ema3 > ema13) and (prev_ema3 < prev_ema13) and (ema3 > ema144) and (ema13 > ema144):
                # get the current balance of dollars in our wallet.
                cur_balance = balance['usd']
                # buy as much as we can with our current balance; we're all in!
                buy_volume = (cur_balance) / close
                self.place_buy_order(close, buy_volume)

        # otherwise if we are invested, try to see if we should place a sell order.
        elif self.context['invested']:
            # did our fastest ema fall below the other emas?
            if ((ema3 < ema13) and (prev_ema3 > prev_ema13)) or ((ema3 < ema144) and (ema13 < ema144)):
                # get all of our bitcoin and sell it at the current price!
                cur_balance = balance['btc']
                self.place_sell_order(close, cur_balance)



    def on_sell_order_partially_filled(self, order, volume):
        # do nothing if we havent filled the entire sell order.
        pass

    def on_stop_loss_trigger(self, stop_loss):
        # make sure we dont have any buy or sell orders left from previous logic, just in case.
        self.clear_buy_orders()
        self.clear_sell_orders()
        # we lost our investment, so we're no longer invested
        self.context['invested'] = False

    def on_sell_order_filled(self, order):
        # if we sold our investment, we probably need to get rid of the stop loss associated with that investment
        if self.context['stop'] is not None:
            self.remove_stop_loss(self.context['stop'])
            self.context['stop'] = None
        self.context['invested'] = False

    def on_buy_order_partially_filled(self, old_order, new_order):
        # we have some money, so now were invested and waiting to buy all of what we want.
        self.context['invested'] = True

    def on_buy_order_filled(self, old_order):
        # we filled our buy order, now place a stop-loss on low of the previous candle
        cc = self.candles.iloc[-2]
        stop = cc['low']
        # get the amount of bitcoin we have in our wallet right now...
        volume_in_sought_currency = self.wallet.balance[self.currency_sought]
        # and place a stop loss at the low we established.
        self.context['stop'] = self.place_stop_loss(stop, volume_in_sought_currency)
        # since we have our btc, we're invested.
        self.context['invested'] = True


# start and end date to our test....
start_date = '2017-06-01 01:00:00.00'
end_date = '2017-06-14 01:00:00.00'
# initial capital for our test.
wallet = kr.runner.Wallet({'usd': 500.0})
# the trading strategy were using...
runner = ThreeLines('usd', 'btc', wallet)
# the initialization of the trends we will be using during the testing.
ema3 = trends.ema(period=3, frame_input=runner.candles, name='ema3', target_column='close')
runner.set_trend(trend=ema3, name='ema3')
ema13 = trends.ema(period=13, frame_input=runner.candles, name='ema13', target_column='close')
runner.set_trend(trend=ema13, name='ema13')
ema144 = trends.ema(period=144, frame_input=runner.candles, name='ema144', target_column='close')
runner.set_trend(trend=ema144, name='ema144')
# set our program in motion from the start date to the end date using our data, it also mentions the trading pair,
# the trading strategy to use, and the period of the candles to make in seconds; this is in 30 minute candles.
processor = kr.processor.Processor('kraken_XBTUSD.pickle', 'XBTUSD', start_date, end_date, runner, frequency=1800.0)
# run our test...
processor.run()
# and save the results to be plotted.
processor.save_session_to_csv('save.csv')
