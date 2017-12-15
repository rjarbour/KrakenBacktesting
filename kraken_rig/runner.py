import copy
import pandas as pd
from types import FunctionType
from kraken_rig import trends
from kraken_rig import analysis


# https://www.forexstrategiesresources.com/scalping-forex-strategies/

class Wallet:
    def __init__(self, balances, volume_cap=1.0):
        """
        -----------VOLUME CAP REALLY ISN'T USEFUL AND PROBABLY SHOULD BE REMOVED!!-----------

        Initialized the amount of money the user starts with and sets a cap on how much money can be moved at one time.

        :param balances: A dictionary of currency names and the amount that the wallet starts with.
        :type balances: dict
        :param volume_cap: The amount of money that a wallet can move at one time.
        :type volume_cap: float
        """
        self.balance = balances
        self.volume_cap = volume_cap

        # self.market_count = market_count

    def transfer(self, sender, receiver, amount, ratio=1.0, fee=0.0):
        """
        Changes money from one currency to another given a particular ratio and transaction fee.

        :param sender: The currency we're transferring from.
        :type sender: str
        :param receiver: The currency we're transferring to.
        :type receiver: str
        :param amount: The amount of currency were spending in terms of the currency were taking from.
        :type amount: float
        :param ratio: The ratio of value from the sender to the receiver. IE: USD to JPY is 112.9; JPY to USD is 0.0088
        :type ratio: float
        :param fee: The percentage money to take from the transaction as a fee
        :type fee: float
        """
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
        """
        The initial setup for the runner; sets initial values for internal use and dynamically
        collects the internal analysis subclasses contained in trends...

        :param currency_offered: The currency the the wallet currently has
        :type currency_offered: str
        :param currency_sought: The currency the wallet is looking to gain
        :type currency_sought: str
        :param wallet: The class that keeps track of the amount of currencies held and the transactions
        :type: kraken_rig.runner.Wallet
        """
        self.wallet = wallet
        self.trading = False
        self.prev_frames = None
        self.fee = 0.001
        # self.buy_orders = pd.DataFrame(index=self.pandas_index, columns=['open', 'close', 'high', 'low', 'date'])
        # self.sell_orders = pd.DataFrame(index=self.pandas_index, columns=['open', 'close', 'high', 'low', 'date'])
        # self.candles = pd.DataFrame(index=self.pandas_index, columns=['open', 'close', 'high', 'low', 'date'])
        self.candles = pd.DataFrame(columns=['high', 'low', 'close', 'open', 'start', 'end'])
        self.runtime_analysis = {}
        self.context = {}
        self.currency_sought = currency_sought
        self.currency_offered = currency_offered

        # Get and adds all of the trends listed internally dynamically...

        trend_names = [x for x, y in trends.__dict__.items() if type(y) == type]
        trend_classes = [y for x, y in trends.__dict__.items() if type(y) == type]
        self.analysis = analysis.analysis(dict(zip(trend_names, trend_classes)))

    def set_trend(self, trend=None, name=None):
        """
        Sets a trend to be used during the trading window.

        """
        if trend is None or name is None:
            assert ('bad input for set_trend')
        self.runtime_analysis[name] = trend


    def get_total_value(self, current_value):
        """
        -----------BROKEN DONT USE THIS, NEEDS TO BE REWRITTEN!!!-----------

        Finds the total value of the wallet in USD.
        :param current_value: The current
        :return:
        """
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
        """
        -----------MAY BECOME OUTDATED DUE TO HOW TRENDS ARE BEING IMPLEMENTED!!!-----------
        -----------TIME_FRAME NEEDS TO BE A DATETIME RATHER THAN A FLOAT!!!-----------

        Tells the runner to record a specific value at a specific time-stamp for use after testing / debugging..

        :param time_frame: The date in the test when the value was recorded in milliseconds since Epoch
        :type time_frame: float
        :param kwargs: The data to be added as a keyword pair... IE {SMA: some_data....}
        """
        if kwargs is not None:
            for key in kwargs.keys():
                if time_frame in self.runtime_analysis:
                    self.runtime_analysis[time_frame].update(
                        {copy.deepcopy(key): copy.deepcopy(kwargs[key])})
                else:
                    self.runtime_analysis[time_frame] = {copy.deepcopy(key): copy.deepcopy(kwargs[key])}

    def process_delegator(self, obj, flag):
        """
        Takes the data being passed from the processor and records / passes the data to the test.

        :param obj: The particular event in question as a data object... typically a Dictionary.
        :param flag: Tells the runner exactly what the object is as a String.
        """
        if flag == 'candle':
            print(obj)
            self.candles.loc[obj['start']] = {'start': obj['start'], 'end': obj['end'], 'close': obj['close'],
                                              'open': obj['open'], 'low': obj['low'], 'high': obj['high']}
            [x.step(obj['end']) for x in self.runtime_analysis.values()]
            # [self.runtime_analysis[x].step(obj['end']) for x in self.runtime_analysis.keys()]
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
        """
        -----------EVENTUALLY NEEDS TO SUPPORT MULTIPLE CURRENCIES, CURRENTLY JUST HANDLES THE INITIAL ONE!!!-----------
        -----------CHANGE THE RETURN TO BE A UNIQUE IDENTIFIER IN THE ORDER BOOK, RATHER THAN THE ENTRY!!!-----------

        Places a buy order into the 'order book' of the market...

        :param price: The price that you want to buy the currency at
        :type price: float
        :param volume: The amount of the currency you want to buy
        :type volume: float
        :return: The buy order created and added to the order book, used to reference the order you created, so dont
        lose this.
        """
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
        """
        -----------EVENTUALLY NEEDS TO SUPPORT MULTIPLE CURRENCIES, CURRENTLY JUST HANDLES THE INITIAL ONE!!!-----------
        -----------CHANGE THE RETURN TO BE A UNIQUE IDENTIFIER IN THE ORDER BOOK, RATHER THAN THE ENTRY!!!-----------

        adds a sell order to the order book.

        :param price: the price of the currency you want to buy at.
        :type price: float
        :param volume: the volume of the currency you want to buy at the particular price
        :type volume: float
        :return: A copy of the order added to the order book, used to retrieve the order if you want to modify
        or remove the order.
        """
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
        """
        -----------EVENTUALLY NEEDS TO SUPPORT MULTIPLE CURRENCIES, CURRENTLY JUST HANDLES THE INITIAL ONE!!!-----------
        -----------CHANGE THE RETURN TO BE A UNIQUE IDENTIFIER IN THE ORDER BOOK, RATHER THAN THE ENTRY!!!-----------
        -----------CHANGE THE ORDER TO BE AN ACTUAL ORDER OBJECT INSTEAD OF A DICTIONARY!!!-----------

        Places a stop limit that will trigger at the specific limit price. When the limit is reached, the order
        provided will be added to the corresponding order book. wait what?

        :param stop: What is this?
        :param limit: The price of the currency that triggers the order to be added to the order book.
        :type limit: float
        :param order: The structure of an order as a dictionary
        {price: float, volume: float, currency_offered: str, currency_sought: str}
        :type order: dictionary
        :return: The stop limit as a dictionary, used to remove and modify the order for later use.
        """
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
        """
        -----------EVENTUALLY NEEDS TO SUPPORT MULTIPLE CURRENCIES, CURRENTLY JUST HANDLES THE INITIAL ONE!!!-----------
        -----------CHANGE THE RETURN TO BE A UNIQUE IDENTIFIER IN THE ORDER BOOK, RATHER THAN THE ENTRY!!!-----------
        -----------CHANGE THE RETURN TO INDICATE WHETHER THE ORDER WAS REMOVED!!!-----------

        Removed a specific order from the order book.

        :param order: The dictionary of an order, usually returned when you add an order to the order book.
        :type order: Dictionary
        """
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
        """
        -----------EVENTUALLY NEEDS TO SUPPORT MULTIPLE CURRENCIES, CURRENTLY JUST HANDLES THE INITIAL ONE!!!-----------
        -----------CHANGE THE RETURN TO BE A UNIQUE IDENTIFIER IN THE ORDER BOOK, RATHER THAN THE ENTRY!!!-----------
        -----------CHANGE THE RETURN TO INDICATE WHETHER THE ORDER WAS REMOVED!!!-----------

        Removes a specific stop limit.

        :param order: the specific stop limit to remove
        :type order: Dictionary
        """
        if 'stop_limits' not in self.context:
            raise Exception('Cant remove stop limit: no stop limit has ever been made!')
        for entry in self.context['stop_limits']:
            if order == entry['order']:
                self.context['stop_limits'].remove(entry)

    def remove_sell_order(self, order):
        """
        -----------EVENTUALLY NEEDS TO SUPPORT MULTIPLE CURRENCIES, CURRENTLY JUST HANDLES THE INITIAL ONE!!!-----------
        -----------CHANGE THE RETURN TO BE A UNIQUE IDENTIFIER IN THE ORDER BOOK, RATHER THAN THE ENTRY!!!-----------
        -----------CHANGE THE RETURN TO INDICATE WHETHER THE ORDER WAS REMOVED!!!-----------

        Removed a specific order from the order book.

        :param order: The dictionary of an order, usually returned when you add an order to the order book.
        :type order: Dictionary
        """
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
        """
        -----------EVENTUALLY NEEDS TO SUPPORT MULTIPLE CURRENCIES, CURRENTLY JUST HANDLES THE INITIAL ONE!!!-----------
        -----------CHANGE THE RETURN TO BE A UNIQUE IDENTIFIER IN THE ORDER BOOK, RATHER THAN THE ENTRY!!!-----------
        -----------CHANGE THE RETURN TO INDICATE WHETHER THE ORDER WAS SUCCESSFULLY MODIFIED!!!-----------

        Changes the specific sell order into a different sell order.

        :param order: The dictionary of an order, usually returned when you add an order to the order book.
        :type order: Dictionary
        :param new_order: The dictionary of an order, This one should be the changed order.
        :type new_order: Dictionary
        """
        if 'sell_orders' not in self.context:
            raise Exception('Cant append sell order: no sell order has ever been made!')
        elif order not in self.context['sell_orders']:
            raise Exception('Cant append sell order: sell order does not exist!')
        else:
            self.context['sell_orders'].remove(order)
            self.context['sell_orders'].append(new_order)

    def append_buy_order(self, order, new_order):
        """
        -----------EVENTUALLY NEEDS TO SUPPORT MULTIPLE CURRENCIES, CURRENTLY JUST HANDLES THE INITIAL ONE!!!-----------
        -----------CHANGE THE RETURN TO BE A UNIQUE IDENTIFIER IN THE ORDER BOOK, RATHER THAN THE ENTRY!!!-----------
        -----------CHANGE THE RETURN TO INDICATE WHETHER THE ORDER WAS SUCCESSFULLY MODIFIED!!!-----------

        Changes the specific sell order into a different sell order.

        :param order: The dictionary of an order, usually returned when you add an order to the order book.
        :type order: Dictionary
        :param new_order: The dictionary of an order, This one should be the changed order.
        :type new_order: Dictionary
        """
        if 'buy_orders' not in self.context:
            raise Exception('Cant append buy order: no buy order has ever been made!')
        elif order not in self.context['buy_orders']:
            raise Exception('Cant append buy order: buy order does not exist!')
        else:
            self.context['buy_orders'].remove(order)
            self.context['buy_orders'].append(new_order)

    def __fill_sell_order(self, order):
        """
        The function that fills sell orders when the processor sees that our order is the best offer given when a
        buy order is placed by others.

        :param order: The specific order that's being filled.
        :type order: Dictionary
        """
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
        """
        The function that fills sell orders when the processor sees that our order is the best offer given when a
        buy order is placed by others, except the person buying isn't buying the entire volume we've listed.

        :param order: The specific order that's being filled.
        :type order: Dictionary
        """
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
        """
        The function that fills buys orders when the processor sees that our order is the best offer given when a
        sell order is placed by others.

        :param order: The specific order that's being filled.
        :type order: Dictionary
        """
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
        """
        The function that fills buys orders when the processor sees that our order is the best offer given when a
        sell order is placed by others, except the person selling isn't selling the entire volume we've listed.

        :param order: The specific order that's being filled.
        :type order: Dictionary
        """
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
        """
        -----------IMPLEMENTATION IS TERRIBLE, MAKE STOP LIMITS GO INTO THE ORDER BOOK WHEN THEY TRIGGER!!!-----------

        checks if the order being filled is a stop order, if it is, remove the stop limit from the listing.

        :param order: The specific order that's being filled.
        :type order: Dictionary
        """
        if 'stop_limits' in self.context:
            for entry in self.context['stop_limits']:
                if order == entry['order']:
                    self.context['stop_limits'].remove(entry)

    def __process_stop_limit(self, stop_limit):
        """
        -----------FIGURE OUT WHY THIS IS EVEN HERE!!!-----------

        Some sort of redundant stop limit handling... Im not sure why this exists.

        :param stop_limit: The specific stop limit that's being filled.
        :type stop_limit: Dictionary
        """
        self.context['stop_limits'].remove(stop_limit)
        self.remove_sell_order(stop_limit['order'])
        order = self.place_sell_order(stop_limit['limit'], stop_limit['order']['volume'])
        return order

    def __process_historical_trade_event(self, row):
        """
        Intended as a handler for the user to fill logic each time the order book is altered

        :param row: the events that have passed in the given period.
        """
        raise NotImplementedError()

    def __process_candle(self, candle):
        """
        Intended as a handler for the user to fill logic each time a new candle has been created; which is related
        to the period specified.

        :param candle: the new candle created.
        """
        raise NotImplementedError()

    def on_sell_order_partially_filled(self, order, volume):
        """
        Intended as a handler for the user to fill logic each time a sell order is partially filled.

        :param order: the order that was partially filled.
        :type order: Dictionary
        :param volume: The amount of currency that was sold
        :type volume: float
        """
        raise NotImplementedError()

    def on_sell_order_filled(self, order):
        """
        Intended as a handler for the user to fill logic each time a sell order is filled.

        :param order: The sell order that was filled and removed from the order book.
        :type order: Dictionary
        """
        raise NotImplementedError()

    def on_buy_order_partially_filled(self, order, volume):
        """
        Intended as a handler for the user to fill logic each time a buy order is partially filled.

        :param order: the order that was partially filled.
        :type order: Dictionary
        :param volume: The amount of currency that was bought
        :type volume: float
        """
        raise NotImplementedError()

    def on_buy_order_filled(self, order):
        """
        Intended as a handler for the user to fill logic each time a buy order is filled.

        :param order: The buy order that was filled and removed from the order book.
        :type order: Dictionary
        """
        raise NotImplementedError()

    def on_stop_limit_trigger(self, stop_limit, order):
        """
        Intended as a handler for the user to fill logic each time a stop limit is triggered.

        :param stop_limit: The stop limit that was triggered.
        :type stop_limit: Dictionary
        :param order: the order that triggered the stop limit.
        :type order: Dictionary
        """
        raise NotImplementedError()
