# KrakenBacktesting
Backtesting for cyrpto trading strategies on Kraken

# What does this do? Why was this made?
The intentions of this project was to create an environment that simulates the order books on cryptocurrency markets; I chose Kraken as the market of choice since it was easy enough to pull all of the historical data from a particular trading pair, some markets don’t offer this. What makes this testing library different from other back testing financial libraries is that it was intended to be used for cryptocurrency markets; so instead of relying on particular candle intervals, the precision can go all the way down to a per order book update logic; so you can make the update interval nearly anything you like. It also has different functionality when it comes to placing orders or stop limits; no assumptions are made on the buying or selling of crypto currency, meaning someone must fill your order for it to go through.

Currently the only functionality of this library is to show the wallet's balances and the total balance of the wallet as it runs through the interval of data given to it; so it is currently only good to see if a strategy could be successful, any graphing or analytical pieces simply haven’t been developed yet but are being planned.

# Setup
To run this script you will need 
  * python 3.4
  * Pandas


# How do I use this?
Kraken_rig_runner is a commented example to see the flow of the program:
Typically you want to set your trade logic apart from your indicator or trend declarations.
Using the example plotting file you will get this!:
![alt text](https://image.prntscr.com/image/Uneoch7gQ9CnpdU-nhyylg.png)

# to-do
  * generate the required javascript and css to produce a graph
  * work on the techan branch to include other features I want
  * include volume, expiring orderbook requests, and many basic finacial indicators and trends
  * include many trading pairs at once
