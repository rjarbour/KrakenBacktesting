import copy


# https://www.forexstrategiesresources.com/scalping-forex-strategies/

class Wallet:
    def __init__(self, balances, volume_cap=1.0):
        self.balance = balances
        self.volume_cap = volume_cap

        # self.market_count = market_count

    def transfer(self, sender, receiver, amount, ratio=1.0, fee=0.0):
        if sender in self.balance:
            # print(self.balance[sender], float(amount))
            if self.balance[sender] < float(amount):
                raise Exception('Not enough Money in ' + sender + ' to complete transfer!' + str(amount) + ' > ' + str(
                    self.balance[sender]))
            else:
                if receiver in self.balance:
                    self.balance[receiver] += float('{0:.10f}'.format((amount - (amount * fee)) * ratio)[:-1])
                else:
                    self.balance[receiver] = float('{0:.10f}'.format((amount - (amount * fee)) * ratio)[:-1])
                self.balance[sender] -= float('{0:.10f}'.format(amount)[:-1])
                self.balance[sender] = float('{0:.10f}'.format(self.balance[sender])[:-1])
                self.balance[receiver] = float('{0:.10f}'.format(self.balance[receiver])[:-1])
                # print('transaction',sender, receiver, self.balance, amount)
        else:
            raise Exception('no currency in the wallet of type ' + str(sender))


class Runner:
    def __init__(self, currency_offered, currency_sought, wallet):
        self.trade_count = 0
        self.buy_balance = 0.0
        self.trade_success = 0.0
        self.wallet = wallet
        # self.market = market
        self.trading = False
        self.prev_frames = None
        self.fee = 0.001
        self.buy_price = 0.0
        self.sellout_threshold = 0.008
        self.stop_threshold = 0.005
        # self.buy_orders = pd.DataFrame(index=self.pandas_index, columns=['open', 'close', 'high', 'low', 'date'])
        # self.sell_orders = pd.DataFrame(index=self.pandas_index, columns=['open', 'close', 'high', 'low', 'date'])
        # self.candles = pd.DataFrame(index=self.pandas_index, columns=['open', 'close', 'high', 'low', 'date'])
        self.candles = []
        self.buys_filled = []
        self.sells_filled = []
        self.additional_records = {}
        self.context = {}
        self.currency_sought = currency_sought
        self.currency_offered = currency_offered

    def get_total_value(self, current_value):
        value = 0.0
        if self.currency_sought in self.wallet.balance:
            value += self.wallet.balance[self.currency_sought] * current_value
        if self.currency_sought + '_sell_order' in self.wallet.balance:
            value += self.wallet.balance[self.currency_sought + '_sell_order'] * current_value
        if self.currency_offered in self.wallet.balance:
            value += self.wallet.balance[self.currency_offered]
        if self.currency_offered + '_buy_order' in self.wallet.balance:
            value += self.wallet.balance[self.currency_offered + '_buy_order']
        return value

    def record(self, time_frame, **kwargs):
        if kwargs is not None:
            for key in kwargs.keys():
                if time_frame in self.additional_records:
                    self.additional_records[time_frame].update(
                        {copy.deepcopy(key): copy.deepcopy(kwargs[key])})
                else:
                    self.additional_records[time_frame] = {copy.deepcopy(key): copy.deepcopy(kwargs[key])}

    def process_delegator(self, obj, flag):
        if flag == 'candle':
            print(obj)
            self.candles.append(obj)
            self.record(obj['start'], candles=obj)
            self.process_candle()
        if flag == 'closed_trade':
            if obj.type == 'b':
                if 'buy_orders' in self.context:
                    for entry in self.context['buy_orders']:
                        if obj['price'] < entry['price']:
                            obj['volume'] = float(obj['volume'])
                            if obj['volume'] > entry['volume']:
                                # print('internal b')
                                self.__fill_buy_order(entry)
                                self.buys_filled.append(entry)
                                self.on_buy_order_filled(entry)
                            else:
                                # print('internal p b')
                                new_order = self.__fill_partial_buy_order(entry, obj['volume'])
                                # self.buys_filled.append(partial_entry)
                                self.on_buy_order_partially_filled(entry, new_order)
            else:
                if 'sell_orders' in self.context:
                    for entry in self.context['sell_orders']:
                        # print('sell price',obj['price'],entry['price'])
                        if entry['price'] < obj['price']:
                            if float(obj['volume']) > entry['volume']:
                                # print('internal s: order vol:',str(obj['volume']),'price:',str(obj['price']))
                                self.__fill_sell_order(entry)
                                self.sells_filled.append(entry)
                                self.on_sell_order_filled(entry)
                            else:
                                # print('internal p s')
                                partial_entry = self.__fill_partial_sell_order(entry, obj['volume'])
                                self.sells_filled.append(partial_entry)
                                self.on_sell_order_partially_filled(entry, obj['volume'])

            if 'stop_limits' in self.context:
                for entry in self.context['stop_limits']:
                    if obj['price'] < entry['stop']:
                        order = self.__process_stop_limit(entry)
                        self.on_stop_limit_trigger(entry, order)

            self.process_historical_trade_event(obj)

    def place_buy_order(self, price, volume):
        price = float('{0:.10f}'.format(price)[:-1])
        volume = float('{0:.10f}'.format(volume)[:-1])
        if self.wallet.balance[self.currency_offered] >= float('{0:.10f}'.format(price * volume)[:-1]):
            if 'buy_orders' not in self.context:
                self.context['buy_orders'] = [{
                    'price': price,
                    'volume': volume,
                    'currency_offered': self.currency_offered,
                    'currency_sought': self.currency_sought
                }]
            else:
                self.context['buy_orders'].append({
                    'price': price,
                    'volume': volume,
                    'currency_offered': self.currency_offered,
                    'currency_sought': self.currency_sought
                })
            self.wallet.transfer(self.currency_offered, self.currency_offered + '_buy_order',
                                 float('{0:.10f}'.format(price * volume)[:-1]))
            return self.context['buy_orders'][-1]
        else:
            raise Exception('Cant try to buy more than your balance contains! balance: ' + str(
                self.wallet.balance[self.currency_offered]) + ' amount: ' + str((price * volume)))

    def place_sell_order(self, price, volume):
        price = float('{0:.10f}'.format(price)[:-1])
        volume = float('{0:.10f}'.format(volume)[:-1])
        if self.currency_offered in self.wallet.balance:
            if self.wallet.balance[self.currency_offered] >= volume:
                if 'sell_orders' not in self.context:
                    self.context['sell_orders'] = [{
                        'price': price,
                        'volume': volume,
                        'currency_offered': self.currency_offered,
                        'currency_sought': self.currency_sought
                    }]
                else:
                    self.context['sell_orders'].append({
                        'price': price,
                        'volume': volume,
                        'currency_offered': self.currency_offered,
                        'currency_sought': self.currency_sought
                    })
                    # print(currency_sought, volume)
                if self.currency_sought + '_sell_order' == 'usd_sell_order':
                    raise Exception('Dom currency sell!')
                self.wallet.transfer(self.currency_sought, self.currency_sought + '_sell_order', volume)
                return self.context['sell_orders'][-1]
            else:
                raise Exception('Cant try to sell more than your balance contains! balance: ' + str(
                    self.wallet.balance[self.currency_offered]) + ' amount: ' + str((price * volume)))

    def place_stop_limit(self, stop, limit, order):
        stop = float('{0:.10f}'.format(stop)[:-1])
        limit = float('{0:.10f}'.format(limit)[:-1])
        if 'stop_limits' not in self.context:
            self.context['stop_limits'] = [{
                'order': order,
                'stop': stop,
                'limit': limit
            }]
        else:
            self.context['stop_limits'].append({
                'order': order,
                'stop': stop,
                'limit': limit
            })
            return self.context['stop_limits'][-1]

    def remove_buy_order(self, order):
        if 'buy_orders' not in self.context:
            raise Exception('Cant remove buy order: no buy order has ever been made!')
        elif order not in self.context['buy_orders']:
            raise Exception('Cant remove buy order: buy order does not exist!')
        else:
            self.wallet.transfer(
                order['currency_offered'] + '_buy_order',
                order['currency_offered'],
                float('{0:.10f}'.format(order['price'] * order['volume'])[:-1])
            )
            self.context['buy_orders'].remove(order)

    def remove_stop_limit(self, order):
        if 'stop_limits' not in self.context:
            raise Exception('Cant remove stop limit: no stop limit has ever been made!')
        for entry in self.context['stop_limits']:
            if order == entry['order']:
                self.context['stop_limits'].remove(entry)
                return

                # having problems managing stop limits...
                # raise Exception('Cant remove stop limit: stop limit does not exist!')

    def remove_sell_order(self, order):

        if 'sell_orders' not in self.context:
            raise Exception('Cant remove sell order: no sell order has ever been made!')
        elif order not in self.context['sell_orders']:
            raise Exception('Cant remove sell order: sell order does not exist!')
        else:
            self.wallet.transfer(
                order['currency_sought'] + '_sell_order',
                order['currency_sought'],
                float('{0:.10f}'.format(order['volume'])[:-1])
            )
            self.context['sell_orders'].remove(order)

    def append_sell_order(self, order, new_order):
        if 'sell_orders' not in self.context:
            raise Exception('Cant append sell order: no sell order has ever been made!')
        elif order not in self.context['sell_orders']:
            raise Exception('Cant append sell order: sell order does not exist!')
        else:
            self.context['sell_orders'].remove(order)
            self.context['sell_orders'].append(new_order)

    def append_buy_order(self, order, new_order):
        if 'buy_orders' not in self.context:
            raise Exception('Cant append buy order: no buy order has ever been made!')
        elif order not in self.context['buy_orders']:
            raise Exception('Cant append buy order: buy order does not exist!')
        else:
            self.context['buy_orders'].remove(order)
            self.context['buy_orders'].append(new_order)

    def __fill_sell_order(self, order):
        # pdb.set_trace()
        # print(order['currency_sought']+'_sell_order', order['currency_sought'], order['volume'])
        self.wallet.transfer(
            order['currency_sought'] + '_sell_order',
            order['currency_offered'],
            order['volume'],
            ratio=order['price'],
            fee=self.fee
        )
        if 'sell_orders' not in self.context:
            raise Exception('Cant remove sell order: no sell order has ever been made!')
        elif order not in self.context['sell_orders']:
            raise Exception('Cant remove sell order: sell order does not exist!')
        else:
            self.context['sell_orders'].remove(order)
            self.__if_stop_remove_stop(order)

    def __fill_partial_sell_order(self, order, volume):
        volume = float('{0:.10f}'.format(volume)[:-1])
        self.wallet.transfer(
            order['currency_sought'] + '_sell_order',
            order['currency_offered'],
            volume,
            ratio=order['price'],
            fee=self.fee
        )
        new_order = order
        new_order['volume'] = float('{0:.10f}'.format(order['volume'] - volume)[:-1])
        self.append_sell_order(order, new_order)
        self.__if_stop_remove_stop(order)

    def __fill_buy_order(self, order):
        # print(order['currency_offered']+'_buy_order', order['currency_sought'], order['volume'], order['price'])
        self.wallet.transfer(
            order['currency_offered'] + '_buy_order',
            order['currency_sought'],
            float('{0:.10f}'.format(order['price'] * order['volume'])[:-1]),
            ratio=(float('{0:.10f}'.format(1 / order['price'])[:-1])),
            fee=self.fee
        )
        if 'buy_orders' not in self.context:
            raise Exception('Cant remove buy order: no buy order has ever been made!')
        elif order not in self.context['buy_orders']:
            raise Exception('Cant remove buy order: buy order does not exist!')
        else:
            self.context['buy_orders'].remove(order)

    def __fill_partial_buy_order(self, order, volume):
        volume = float('{0:.10f}'.format(volume)[:-1])
        self.wallet.transfer(
            order['currency_offered'] + '_buy_order',
            order['currency_sought'],
            float('{0:.10f}'.format(order['price'] * volume)[:-1]),
            ratio=(float('{0:.10f}'.format(1 / order['price'])[:-1])),
            fee=self.fee
        )
        new_order = order
        # print('DOUBLE INTERNAL: old:',str(order['volume']),'filled:',str(volume))
        new_order['volume'] = float('{0:.10f}'.format(order['volume'] - volume)[:-1])
        self.append_buy_order(order, new_order)
        return new_order

    def __if_stop_remove_stop(self, order):
        if 'stop_limits' in self.context:
            for entry in self.context['stop_limits']:
                if order == entry['order']:
                    self.context['stop_limits'].remove(entry)

    def __process_stop_limit(self, stop_limit):
        self.context['stop_limits'].remove(stop_limit)
        self.remove_sell_order(stop_limit['order'])
        order = self.place_sell_order(stop_limit['limit'], stop_limit['order']['volume'])
        return order

    def process_historical_trade_event(self, row):
        raise NotImplementedError()

    def process_candle(self, candle):
        raise NotImplementedError()

    def on_sell_order_partially_filled(self, order, volume):
        raise NotImplementedError()

    def on_sell_order_filled(self, order):
        raise NotImplementedError()

    def on_buy_order_partially_filled(self, order, volume):
        raise NotImplementedError()

    def on_buy_order_filled(self, order):
        raise NotImplementedError()

    def on_stop_limit_trigger(self, stop_limit, order):
        raise NotImplementedError()
