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
            if self.balance[sender] < amount:
                if (amount - self.balance[sender]) >= 0.0000000001:
                    raise Exception(
                        'Not enough Money in ' + sender + ' to complete transfer!' + str(amount) + ' > ' + str(
                            self.balance[sender]))
                else:
                    amount = self.balance[sender]
            if receiver in self.balance:
                self.balance[receiver] += (amount - (amount * fee)) * ratio
            else:
                self.balance[receiver] = (amount - (amount * fee)) * ratio
            self.balance[sender] -= amount
            self.balance[sender] = float("%.10f" % self.balance[sender])
            self.balance[receiver] = float("%.10f" % self.balance[receiver])
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
        self.candles = pd.DataFrame(columns=['high', 'low', 'close', 'open', 'start', 'end'])
        self.runtime_analysis = {}
        self.context = {}
        self.currency_sought = currency_sought
        self.currency_offered = currency_offered
        self.transactions = pd.DataFrame(columns=['buy', 'sell', 'loss', 'balance'])
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
        if self.currency_offered in self.wallet.balance:
            value += self.wallet.balance[self.currency_offered]
        return value

    def process_delegator(self, obj, flag):
        """
        Takes the data being passed from the processor and records / passes the data to the test.

        :param obj: The particular event in question as a data object... typically a Dictionary.
        :param flag: Tells the runner exactly what the object is as a String.
        """
        if flag == 'candle':
            self.candles.loc[obj['start']] = {'start': obj['start'], 'end': obj['end'], 'close': obj['close'],
                                              'open': obj['open'], 'low': obj['low'], 'high': obj['high']}
            [x.step(obj['end']) for x in self.runtime_analysis.values()]
            if 'stop_losses' in self.context:
                for entry in self.context['stop_losses']:
                    if obj['close'] < entry['trigger']:
                        self.__process_stop_loss(entry)
                        self.on_stop_loss_trigger(entry)
            self.process_candle()
            self.transactions.at[self.candles.index.values[-1], 'balance'] = self.get_total_value(obj['close'])
        if flag == 'closed_trade':
            if obj.type == 'b':
                self._process_orderbook_buy(obj)
            else:
                self._process_orderbook_sell(obj)

            self.process_historical_trade_event(obj)

    def _process_orderbook_buy(self, order):
        if 'buy_orders' in self.context:
            for entry in self.context['buy_orders']:
                if order['price'] < entry['price']:
                    order['volume'] = float(order['volume'])
                    if order['volume'] > entry['volume']:
                        self.__fill_buy_order(entry)
                        self.transactions.at[self.candles.index.values[-1], 'buy'] = entry['volume']
                        self.on_buy_order_filled(entry)
                    else:
                        self.transactions.at[self.candles.index.values[-1], 'buy'] = order['volume']
                        new_order = self.__fill_partial_buy_order(entry, order['volume'])
                        self.on_buy_order_partially_filled(entry, new_order)

    def _process_orderbook_sell(self, order):
        if 'sell_orders' in self.context:
            for entry in self.context['sell_orders']:
                if entry['price'] < order['price']:
                    if float(order['volume']) > entry['volume']:
                        self.__fill_sell_order(entry)
                        self.transactions.at[self.candles.index.values[-1], 'sell'] = entry['volume']
                        self.on_sell_order_filled(entry)
                    else:
                        self.transactions.at[self.candles.index.values[-1], 'sell'] = order['volume']
                        new_order = self.__fill_partial_sell_order(entry, order['volume'])
                        self.on_sell_order_partially_filled(entry, new_order['volume'])

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
        if self.wallet.balance[self.currency_offered] >= float("%.10f" % (price * volume)):
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
        if self.currency_offered in self.wallet.balance:
            if self.wallet.balance[self.currency_sought] >= volume:
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
                return self.context['sell_orders'][-1]
            else:
                raise Exception('Cant try to sell more than your balance contains! balance: ' + str(
                    self.wallet.balance[self.currency_offered]) + ' amount: ' + str((price * volume)))

    def place_stop_loss(self, price_trigger, volume):
        """
        -----------EVENTUALLY NEEDS TO SUPPORT MULTIPLE CURRENCIES, CURRENTLY JUST HANDLES THE INITIAL ONE!!!-----------
        -----------CHANGE THE RETURN TO BE A UNIQUE IDENTIFIER IN THE ORDER BOOK, RATHER THAN THE ENTRY!!!-----------
        -----------CHANGE THE ORDER TO BE AN ACTUAL ORDER OBJECT INSTEAD OF A DICTIONARY!!!-----------

        Places a stop limit that will trigger at the specific price_trigger. When the limit is reached, The volume of
        currency will be sold at the next opportunity.

        :param price_trigger: The price that the stop loss will trigger at.
        :type price_trigger: float
        :param volume: the amount of currency to sell
        :type order: dictionary
        :return: The stop limit as a dictionary, used to remove and modify the order for later use.
        """
        if 'stop_losses' not in self.context:
            self.context['stop_losses'] = [{
                'trigger': price_trigger,
                'volume': volume
            }]
        else:
            self.context['stop_losses'].append({
                'trigger': price_trigger,
                'volume': volume
            })
        return self.context['stop_losses'][-1]

    def clear_buy_orders(self):
        """
        removes all buy orders from the order book.

        :return: None
        """
        for order in self.context['buy_orders']:
            self.remove_buy_order(order)

    def clear_sell_orders(self):
        """
        removes all buy orders from the order book.

        :return: None
        """
        for order in self.context['sell_orders']:
            self.remove_sell_order(order)


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
            self.context['buy_orders'].remove(order)

    def remove_stop_loss(self, order):
        """
        -----------EVENTUALLY NEEDS TO SUPPORT MULTIPLE CURRENCIES, CURRENTLY JUST HANDLES THE INITIAL ONE!!!-----------
        -----------CHANGE THE RETURN TO BE A UNIQUE IDENTIFIER IN THE ORDER BOOK, RATHER THAN THE ENTRY!!!-----------
        -----------CHANGE THE RETURN TO INDICATE WHETHER THE ORDER WAS REMOVED!!!-----------

        Removes a specific stop limit.

        :param order: the specific stop limit to remove
        :type order: Dictionary
        """
        if 'stop_losses' not in self.context:
            raise Exception('Cant remove stop limit: no stop limit has ever been made!')
        self.context['stop_losses'].remove(order)

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
            order['currency_sought'],
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

    def __fill_partial_sell_order(self, order, volume):
        """
        The function that fills sell orders when the processor sees that our order is the best offer given when a
        buy order is placed by others, except the person buying isn't buying the entire volume we've listed.

        :param order: The specific order that's being filled.
        :type order: Dictionary
        """
        self.wallet.transfer(
            order['currency_sought'],
            order['currency_offered'],
            volume,
            ratio=order['price'],
            fee=self.fee
        )
        new_order = order
        new_order['volume'] = (order['volume'] - volume)
        self.append_sell_order(order, new_order)
        return new_order

    def __fill_buy_order(self, order):
        """
        The function that fills buys orders when the processor sees that our order is the best offer given when a
        sell order is placed by others.

        :param order: The specific order that's being filled.
        :type order: Dictionary
        """
        self.wallet.transfer(
            order['currency_offered'],
            order['currency_sought'],
            order['price'] * order['volume'],
            ratio=(1 / order['price']),
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
        self.wallet.transfer(
            order['currency_offered'],
            order['currency_sought'],
            order['price'] * volume,
            ratio=(1 / order['price']),
            fee=self.fee
        )
        new_order = order
        new_order['volume'] = order['volume'] - volume
        self.append_buy_order(order, new_order)
        return new_order

    def __process_stop_loss(self, stop_loss):
        """
        sells off the currency 0.4% less than the close value that triggered the stop loss; speculative price

        :param stop_loss: The specific stop limit that's being filled.
        :type stop_loss: Dictionary
        """
        self.context['stop_losses'].remove(stop_loss)
        vol = None
        if self.wallet.balance[self.currency_sought] > stop_loss['volume']:
            vol = stop_loss['volume']
        else:
            vol = self.wallet.balance[self.currency_sought]
        self.wallet.transfer(
            self.currency_sought,
            self.currency_offered,
            vol,
            ratio=(stop_loss['trigger'] - (stop_loss['trigger'] * 0.04)),
            fee=self.fee
        )
        self.transactions.at[self.candles.index.values[-1], 'loss'] = stop_loss['volume']

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

    def on_stop_loss_trigger(self, order):
        """
        Intended as a handler for the user to fill logic each time a stop limit is triggered.

        :param stop_loss: The stop limit that was triggered.
        :type stop_loss: Dictionary
        :param order: the order that triggered the stop limit.
        :type order: Dictionary
        """
        raise NotImplementedError()
