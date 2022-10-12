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
    # Retrieve the previous 4 candles (need 4 candles to check 3 price rises)
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
    # Iterate through the symbols
    for ind in symbol_dataframe.index:
        # Analyze symbol
        analysis = determine_buy_event(symbol=symbol_dataframe['symbol'][ind], timeframe=timeframe,
                                       percentage_rise=percentage_rise)
        # Print analysis to screen. Future update
        print(symbol_dataframe['symbol'][ind], analysis)
        # Sleep for one second
        time.sleep(1)
