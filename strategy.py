import pandas
import numpy
import binance_interaction
import time


# Function to convert candle data from Binance into a dataframe
def get_and_transform_binance_data(symbol, timeframe, number_of_candles):
    # Retrieve the raw data from Binance
    raw_data = binance_interaction.get_candlestick_data(symbol=symbol, timeframe=timeframe, qty=number_of_candles)
    # Transform raw_data into a Pandas DataFrame
    df_data = pandas.DataFrame(raw_data)
    # Convert the time to human readable format. Note that Binance uses micro-seconds.
    df_data['time'] = pandas.to_datetime(df_data['time'], unit='ms')
    # Convert the close time to human readable format. Note that Binance uses micro-seconds.
    df_data['close_time'] = pandas.to_datetime(df_data['close_time'], unit='ms')
    # Calculate if Red or Green
    df_data['RedOrGreen'] = numpy.where((df_data['open'] < df_data['close']), 'Green', 'Red')
    # Return the dataframe
    return df_data


# Function to determine a 'Buy' event for strategy
def determine_buy_event(symbol, timeframe, percentage_rise):
    # Retrieve the previous 3 candles
    candlestick_data = get_and_transform_binance_data(symbol=symbol, timeframe=timeframe, number_of_candles=3)
    # Determine if last three values were Green
    if candlestick_data.loc[0, 'RedOrGreen'] == "Green" and candlestick_data.loc[1, 'RedOrGreen'] == "Green" and candlestick_data.loc[2, 'RedOrGreen'] == "Green":
        # Determine price rise percentage
        rise_one = determine_percent_rise(candlestick_data.loc[0, 'open'], candlestick_data.loc[0, 'close'])
        rise_two = determine_percent_rise(candlestick_data.loc[1, 'open'], candlestick_data.loc[1, 'close'])
        rise_three = determine_percent_rise(candlestick_data.loc[2, 'open'], candlestick_data.loc[2, 'close'])
        # Compare price rises against stated percentage rise
        if rise_one >= percentage_rise and rise_two >= percentage_rise and rise_three >= percentage_rise:
            # We can enter a trade!
            return True
        else:
            # Return False as price not rising fast enough
            return False
    else:
        # Return False as price not rising
        return False


# Function to calculate the percentage price rise as a float
def determine_percent_rise(close_previous, close_current):
    price_change = (close_current-close_previous)/close_previous*100
    return price_change


# Function to extract symbol list into an array
def analyze_symbols(symbol_dataframe, timeframe, percentage_rise):
    # NEW: Create an array of symbols to trade
    trading_symbols = []
    # Iterate through the symbols
    for ind in symbol_dataframe.index:
        print(f"Analyzing Symbol: {symbol_dataframe['symbol'][ind]}")
        # Analyze symbol
        analysis = determine_buy_event(symbol=symbol_dataframe['symbol'][ind], timeframe=timeframe,
                                       percentage_rise=percentage_rise)
        # REMOVE: Print analysis to screen. Future update
        # REMOVE: print(symbol_dataframe['symbol'][ind], analysis)
        # NEW ####
        # If analysis == true, append data to dataframe
        if analysis:
            # Append to trading symbols
            trading_symbols.append(symbol_dataframe['symbol'][ind])
        # Sleep for one second
        time.sleep(0.1)
    # New: Return an array of symbols
    return trading_symbols


# Function defining strategy one
def strategy_one(timeframe, percentage_rise, quote_asset, project_settings):
    # Declare an array for trading symbols
    trading_symbols = []
    # Declare a value for previous time
    previous_time = 0
    # Start checking for trades
    while 1:
        # Retrieve the current time for an arbitrary asset
        test_time = get_and_transform_binance_data(symbol="BTCBUSD", timeframe=timeframe, number_of_candles=1)
        # Extract current time
        current_time = test_time.iloc[0]['time']
        # Compare against previous time
        if current_time != previous_time:
            # A new candle has arrived!
            print("New candle")
            # Update comparison
            previous_time = current_time
            # Step 1: Cancel all orders which haven't executed in the last hour
            print("Step 1: Cancelling open orders")
            # Get a list of open trades
            open_trades = binance_interaction.query_open_trades(project_settings=project_settings)
            # Iterate through the list
            for trades in open_trades:
                # Check to see if qty has been purchased
                executed_qty = float(trades['executedQty'])
                if executed_qty == 0:
                    # If none, cancel
                    binance_interaction.cancel_order_by_symbol(trades['symbol'], project_settings=project_settings)
                    print("Order Canceled")
                    # Remove from trading_array
                    trading_symbols.remove(trades['symbol'])
            # Step 2: Find new assets to purchase
            print("Step 2: Analyzing Assets")
            # Retrieve a list of assets from Binance
            asset_list = binance_interaction.query_quote_asset_list(quote_asset_symbol=quote_asset)
            # Retrieve current symbols to trade
            new_trading_symbols = analyze_symbols(symbol_dataframe=asset_list, timeframe=timeframe, percentage_rise=percentage_rise)
            # See if symbols are already being traded
            for symbol in new_trading_symbols:
                if symbol not in trading_symbols:
                    # If symbol not already being traded, calculate the trading parameters
                    print(f"Opening trade on new symbol: {symbol}")
                    trade_parameters = calculate_trade_parameters(symbol=symbol, timeframe=timeframe, asset_list=asset_list)
                    # Make a trade
                    try:
                        trade_outcome = binance_interaction.make_trade_with_params(params=trade_parameters, project_settings=project_settings)
                        print(trade_outcome)
                        # If trade successful, add to trading symbols
                        trading_symbols.append(symbol)
                    except:
                        trade_outcome = False
                        print("Error placing trade")

            # Finish up and wait for the next candle :)
            print("Analysis Completed")

        # Wait for 1 second
        time.sleep(1)


# Function to calculate trade parameters
def calculate_trade_parameters(symbol, timeframe, asset_list):
    # Retrieve the last candle
    raw_data = binance_interaction.get_candlestick_data(symbol=symbol, timeframe=timeframe, qty=1)
    # Determine the precision required on for the symbol
    precision = asset_list.loc[asset_list['symbol'] == symbol]
    precision = precision.iloc[0]['baseAssetPrecision']
    # Extract the close price
    close_price = raw_data[0]["close"]
    # Calculate the buy stop. This will be 1% of the previous closing price
    buy_stop = (close_price * 1.01)
    # Calculate the quantity. This will be the buy_stop / $100
    raw_quantity = 100/buy_stop
    # Round
    quantity = round(raw_quantity, precision)
    # Create the parameters dictionary based on assumptions
    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "STOP_LOSS_LIMIT",
        "timeInForce": "GTC",
        "quantity": quantity,
        "price": buy_stop,
        "trailingDelta": 100
    }

    return params
