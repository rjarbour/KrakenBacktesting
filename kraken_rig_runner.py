import pandas as pd
from datetime import datetime
import kraken_rig as kr
from kraken_rig import trends


class EmaRunner(kr.runner.Runner):
    def __init__(self, currency_offered, currency_sought, wallet):
        super().__init__(currency_offered, currency_sought, wallet)
        self.context['initial'] = 0
        self.currency_offered = currency_offered
        self.currency_sought = currency_sought
        self.context['invested'] = False
        self.context['target_percent'] = 1.005
        self.context['break_even'] = 1.006 / self.context['target_percent']
        self.context['stop_percent'] = 0.992
        self.context['buy_orders'] = []
        self.context['sell_orders'] = []
        self.context['down_turn'] = False

    def process_historical_trade_event(self, row):
        # this garbage trade strat throws away more refined data...
        pass

    def process_candle(self):
        # init
        i = self.context['initial']
        if i < 50:
            self.context['initial'] += 1
            return

        # analysis
        cur_buy_orders = self.context['buy_orders']
        cur_sell_orders = self.context['sell_orders']
        sma5 = self.runtime_analysis['sma5'][-1]
        sma8 = self.runtime_analysis['sma8'][-1]
        balance = self.wallet.balance
        cc = self.candles.iloc[-1]

        # trade logic
        if not self.context['invested']:
            if sma5 > sma8 and len(cur_buy_orders) == 0:
                self.place_buy_order(cc['low'], 0.15)
                self.record(cc['start'], buy_order_place_price=cc['low'], buy_order_place_volume=0.15)
            elif sma8 > sma5 and len(cur_buy_orders) > 0:
                self.remove_buy_order(cur_buy_orders[0])
                self.record(cc['start'], buy_order_remove_price=cc['low'], buy_order_remove_volume=0.15)
        else:
            if sma8 > sma5 and not len(cur_sell_orders) == 0 and not self.context['down_turn']:
                print('short')
                old_sell_order = cur_sell_orders[0]
                old_volume = self.wallet.balance[self.currency_sought + '_sell_order']
                new_price = old_sell_order['price'] * self.context['break_even']
                new_stop = new_price * self.context['stop_percent']
                self.remove_stop_limit(old_sell_order)
                self.remove_sell_order(old_sell_order)
                order = self.place_sell_order(new_price, old_volume)
                self.place_stop_limit(new_stop, new_stop - 0.00001, order)
                self.context['down_turn'] = True
                self.record(cc['start'], sell_order_place_price=new_price, sell_order_place_volume=old_volume)


                # self.record(cc['start'], sma5=sma5, ema43=sma8)

    # print(self.wallet.balance)

    def on_sell_order_partially_filled(self, order, volume):
        cc = self.candles[-1]
        print('total', self.get_total_value(order['price']), 'p sell', self.wallet.balance)
        self.record(cc['start'], sell_order_filled_price=order['price'], sell_order_place_volume=volume)
        pass

    def on_stop_limit_trigger(self, stop_limit, order):
        cc = self.candles[-1]
        print(stop_limit)
        self.record(cc['start'], stop_limit_filled=stop_limit['limit'], sell_order_place_volume=order['volume'])
        print(order)
        print(self.wallet.balance)

    def on_sell_order_filled(self, order):
        cc = self.candles[-1]
        self.context['invested'] = False
        self.context['down_turn'] = False
        print('total', self.get_total_value(order['price']), 'f sell', self.wallet.balance)
        self.record(cc['start'], sell_order_filled_price=order['price'], sell_order_place_volume=order['volume'])
        pass

    def on_buy_order_partially_filled(self, order, new_order):
        cc = self.candles[-1]
        print('total', self.get_total_value(order['price']), 'p buy:', self.wallet.balance)
        self.record(cc['start'], buy_order_filled_price=order['price'], sell_order_place_volume=new_order['volume'])
        price = order['price'] * self.context['target_percent']
        stop = order['price'] * self.context['stop_percent']
        volume_in_sought_currency = self.wallet.balance[order['currency_sought']]
        self.remove_buy_order(new_order)
        new_sell_order = self.place_sell_order(price, volume_in_sought_currency)
        self.place_stop_limit(stop, stop - 0.00001, new_sell_order)
        self.context['invested'] = True

    def on_buy_order_filled(self, order):
        cc = self.candles[-1]
        print('total', self.get_total_value(order['price']), 'f buy:', self.wallet.balance)
        price = order['price'] * self.context['target_percent']
        stop = order['price'] * self.context['stop_percent']
        volume_in_sought_currency = self.wallet.balance[order['currency_sought']]
        new_sell_order = self.place_sell_order(price, volume_in_sought_currency)
        self.place_stop_limit(stop, stop - 0.00001, new_sell_order)
        self.context['invested'] = True


start_date = '2017-06-01 01:00:00.00'
end_date = '2017-06-03 01:00:00.00'
wallet = kr.runner.Wallet({'usd': 50000.0})
runner = EmaRunner('usd', 'btc', wallet)
sma5 = trends.sma(period=5, frame_input=runner.candles, col_input='close', name='sma5')
runner.set_trend(trend=sma5, name='sma5')
sma8 = trends.sma(period=8, frame_input=runner.candles, col_input='close', name='sma8')
runner.set_trend(trend=sma8, name='sma8')
processor = kr.processor.Processor('XBTUSD', start_date, end_date, runner, frequency=300.0)
processor.run()
processor.save_session_to_dataframe('save.p')
