import pandas as pd
from datetime import datetime
import kraken_rig as kr
import talib


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
        close_list = [self.candles[x]['close'] for x in range(len(self.candles))]
        balance = self.wallet.balance
        cc = self.candles[-1]
        ema3 = talib.MA(pd.Series(close_list).as_matrix(), 20, 1)[-1]
        ema8 = talib.MA(pd.Series(close_list).as_matrix(), 42, 1)[-1]

        # trade logic
        if not self.context['invested']:
            if ema3 > ema8 and len(cur_buy_orders) == 0:
                self.place_buy_order(cc['low'], 0.15)
                self.record(cc['start'], buy_order_place_price=cc['low'], buy_order_place_volume=0.15)
            elif ema8 > ema3 and len(cur_buy_orders) > 0:
                self.remove_buy_order(cur_buy_orders[0])
                self.record(cc['start'], buy_order_remove_price=cc['low'], buy_order_remove_volume=0.15)
        else:
            if ema8 > ema3 and not len(cur_sell_orders) == 0 and not self.context['down_turn']:
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

        self.record(cc['start'], ema20=ema3, ema43=ema8)

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


# lagurre
#
# Some other example that isnt updated...

# class lag_runner(kr.runner.runner):
#     def __init__(self, currency_offered, currency_sought, wallet):
#         super().__init__(currency_offered, currency_sought, wallet)
#         self.context['initial'] = 0
#         self.currency_offered = currency_offered
#         self.currency_sought = currency_sought
#         self.context['invested'] = False
#         self.context['target_percent'] = 1.008
#         self.context['break_even'] = 1.005 / self.context['target_percent']
#         self.context['stop_percent'] = 0.990 / self.context['target_percent']
#         self.context['buy_orders'] = []
#         self.context['sell_orders'] = []
#         self.context['down_turn'] = False
#         self.context['trade_prices'] = []
#
#     def process_historial_trade_event(self, row):
#         pass
#
#     def process_candle(self):
#         # wait for a few cycles to pass to get our indicators working...
#         i = self.context['initial']
#         if i < 50:
#             self.context['initial'] += 1
#             return
#
#         if not 'prev_lag' in self.context:
#             close_list = [candles[x]['close'] for x in range(len(candles))]
#             lag = self.laguerre_series(close_list, gamma=0.7)
#         else:
#             lag = self.laguerre_index(candles[-1]['close'], self.context['prev_lag'], gamma=0.7)
#         # print(talib.MA(pd.Series(close_list).as_matrix(), 14, 1))
#         ema14_prev, ema14 = talib.MA(pd.Series(close_list).as_matrix(), 14, 1)[-2:]
#         ema34_prev, ema34 = talib.MA(pd.Series(close_list).as_matrix(), 34, 1)[-2:]
#         macd = talib.MACD(pd.Series(close_list).as_matrix(), 5, 34, 9)[-1][-1]
#         if not self.context['invested']:
#             # print('\n\n\n\n\n',macd, lag['lag'][-1], ema14_prev, ema34_prev, ema14, ema34)
#             if macd > 0 and lag['lag'][-1] > 0.15 and ema14_prev < ema34_prev and ema14 > ema34:
#                 self.place_buy_order(self.currency_offered, self.currency_sought, candles[-1]['close'], 0.05)
#
#             if macd < 0 and lag['lag'][-1] < 0.75 and ema14_prev < ema34_prev and ema14 > ema34:
#                 if not len(self.context['buy_orders']) > 0:
#                     for orders in self.context['buy_orders']:
#                         self.remove_buy_order(order)
#
#         else:
#             if macd > 0 and lag['lag'][-1] > 0.15 and ema14_prev < ema34_prev and ema14 > ema34:
#                 return
#
#             if macd < 0 and lag['lag'][-1] < 0.75 and ema14_prev < ema34_prev and ema14 > ema34 and not self.context[
#                 'down_turn']:
#                 old_sell_order = self.context['sell_orders'][0]
#                 old_volume = self.wallet.balance[self.currency_sought + '_sell_order']
#                 new_price = old_sell_order['price'] * self.context['break_even']
#                 new_stop = new_price * self.context['stop_percent']
#                 self.remove_stop_limit(old_sell_order)
#                 self.remove_sell_order(old_sell_order)
#                 order = self.place_sell_order(self.currency_offered, self.currency_sought, new_price, old_volume)
#                 self.place_stop_limit(new_stop, new_stop - 0.00001, order)
#                 self.context['down_turn'] = True
#
#     def on_sell_order_partially_filled(self, order, volume):
#         print('p sell', self.wallet.balance)
#
#     def on_stop_limit_trigger(self, stop_Limit, order):
#         print(stop_Limit)
#         print(order)
#         print(self.wallet.balance)
#
#     def on_sell_order_filled(self, order):
#         self.context['invested'] = False
#         self.context['down_turn'] = False
#         print('f sell', self.wallet.balance)
#
#     def on_buy_order_partially_filled(self, order, new_order, **kwargs):
#         print('p buy:', self.wallet.balance)
#         price = order['price'] * self.context['target_percent']
#         stop = order['price'] * self.context['stop_percent']
#         volume_in_sought_currency = self.wallet.balance[order['currency_sought']]
#         self.remove_buy_order(new_order)
#         new_sell_order = self.place_sell_order(self.currency_offered, self.currency_sought, price,
#                                                volume_in_sought_currency)
#         self.place_stop_limit(stop, stop - 0.00001, new_sell_order)
#         self.context['invested'] = True
#
#     def on_buy_order_filled(self, order):
#         print('f buy:', self.wallet.balance)
#         price = order['price'] * self.context['target_percent']
#         stop = order['price'] * self.context['stop_percent']
#         volume_in_sought_currency = self.wallet.balance[order['currency_sought']]
#         new_sell_order = self.place_sell_order(self.currency_offered, self.currency_sought, price,
#                                                volume_in_sought_currency)
#         self.place_stop_limit(stop, stop - 0.00001, new_sell_order)
#         self.context['invested'] = True
#
#     def laguerre_series(self, close, gamma=0.6):
#         L0 = [0.0]
#         L1 = [0.0]
#         L2 = [0.0]
#         L3 = [0.0]
#         CU = [0.0]
#         CD = [0.0]
#         rsi = [0.0]
#         lag = [0.0]
#         for idx in range(len(close) - 1):
#             L0.append(((1 - gamma) * close[idx]) + (gamma * L0[idx]))
#             L1.append((-1 * gamma * L0[idx + 1]) + L0[idx] + (gamma * L1[idx]))
#             L2.append((-1 * gamma * L1[idx + 1]) + L1[idx] + (gamma * L2[idx]))
#             L3.append((-1 * gamma * L2[idx + 1]) + L2[idx] + (gamma * L3[idx]))
#
#             CU_t = None
#             CD_t = None
#             if L0[idx + 1] >= L1[idx + 1]:
#                 CU_t = L0[idx + 1] - L1[idx + 1]
#                 CD_t = CD[idx]
#             else:
#                 CU_t = CU[idx]
#                 CD_t = L1[idx + 1] - L0[idx + 1]
#             if L1[idx + 1] >= L2[idx + 1]:
#                 CU_t += L1[idx + 1] - L2[idx + 1]
#             else:
#                 CD_t += L2[idx + 1] - L1[idx + 1]
#             if L2[idx + 1] >= L3[idx + 1]:
#                 CU_t += L2[idx + 1] - L3[idx + 1]
#             else:
#                 CD_t += L3[idx + 1] - L2[idx + 1]
#             CU.append(CU_t)
#             CD.append(CD_t)
#
#             if CU[idx + 1] + CD[idx + 1] != 0:
#                 rsi.append(CU[idx + 1] / (CU[idx + 1] + CD[idx + 1]))
#             else:
#                 rsi.append(0.0)
#
#             lag.append((rsi[idx] + rsi[idx + 1]) / 2)
#
#         lag_frame = {'L0': L0, 'L1': L1, 'L2': L2, 'L3': L3, 'CU': CU, 'CD': CD, 'rsi': rsi, 'lag': lag,
#                      'close': [close[len(close) - 1]]}
#         return lag_frame
#
#     def laguerre_index(self, close, prev_lag_frame, gamma=0.6):
#         p = prev_lag_frame
#         lag = {}
#         lag['L0'] = ((1 - gamma) * p['close']) + (gamma * p['L0'])
#         lag['L1'] = (-1 * gamma * lag['L0']) + p['L0'] + (gamma * p['L1'])
#         lag['L2'] = (-1 * gamma * lag['L1']) + p['L1'] + (gamma * p['L2'])
#         lag['L3'] = (-1 * gamma * lag['L2']) + p['L2'] + (gamma * p['L3'])
#
#         if lag['L0'] >= lag['L1']:
#             lag['CU'] = lag['L0'] - lag['L1']
#             lag['CD'] = p['CD']
#         else:
#             lag['CU'] = p['CU']
#             lag['CD'] = lag['L1'] - lag['L0']
#         if lag['L1'] >= lag['L2']:
#             lag['CU'] += lag['L1'] - lag['L2']
#         else:
#             lag['CD'] += lag['L2'] - lag['L1']
#         if lag['L2'] >= lag['L3']:
#             lag['CU'] += lag['L2'] - lag['L3']
#         else:
#             lag['CD'] += lag['L3'] - lag['L2']
#
#         if lag['CU'] + lag['CD'] != 0:
#             lag['rsi'] = lag['CU'] / (lag['CU'] + lag['CD'])
#         else:
#             lag['rsi'] = 0.0
#
#         lag['lag'] = (lag['rsi'] + p['rsi']) / 2
#         lag['close'] = close
#         return lag#


start_date = '2017-06-01 01:00:00.00'
end_date = '2017-08-01 01:00:00.00'
start_datetime = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S.%f")
start_epoch = start_datetime.timestamp()
end_datetime = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S.%f")
end_epoch = end_datetime.timestamp()

wallet = kr.runner.Wallet({'usd': 50000.0})
runner = EmaRunner('usd', 'btc', wallet)
# runner = lag_runner(wallet, 'usd', 'btc')
# market, start_date, end_date, runner=None, frequency=60.0)
processor = kr.processor.Processor('XBTUSD', start_date, end_date, runner, frequency=300.0)
processor.run()
processor.save_session_to_dataframe('save.p');

'''
candles = processor.runner.additional_records['candles']
buys_filled = processor.runner.additional_records['buy_order_filled_price']

for c in candles:
    c['date'] = datetime.fromtimestamp(c['date'])

trace = go.Candlestick(x=[x['date'] for x in candles],
                       open=[x['data']['open'] for x in candles],
                       high=[x['data']['high'] for x in candles],
                       low=[x['data']['low'] for x in candles],
                       close=[x['data']['close'] for x in candles])
# trace2 = go.


data_other = [trace]

layout = {
    'title': 'XBTUSD',
    'yaxis': {'title': 'USD/BTC'},
    'shapes': [{
        'x0': '2007-12-01', 'x1': '2007-12-01',
        'y0': 0, 'y1': 1, 'xref': 'x', 'yref': 'paper',
        'line': {'color': 'rgb(30,30,30)', 'width': 1}
    }],
    'annotations': [{
        'x': '2007-12-01', 'y': 0.05, 'xref': 'x', 'yref': 'paper',
        'showarrow': False, 'xanchor': 'left',
        'text': 'Official start of the recession'
    }]
}
fig = dict(data=data_other, layout=layout)
plotly.offline.plot({
    "data": data_other,
    "layout": Layout(title="hello world")
})
'''
