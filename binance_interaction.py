import pandas
from binance.spot import Spot


# Function to query Binance and retrieve status
def query_binance_status():
    # Query for system status
    status = Spot().system_status()
    if status['status'] == 0:
        return True
    else:
        raise ConnectionError


# Function to query Binance account
def query_account(api_key, secret_key):
    return Spot(key=api_key, secret=secret_key).account()


# Function to query Binance for candlestick data
def get_candlestick_data(symbol, timeframe, qty):
    # Retrieve the raw data
    raw_data = Spot().klines(symbol=symbol, interval=timeframe, limit=qty)
    # Set up the return array
    converted_data = []
    # Convert each element into a Python dictionary object, then add to converted_data
    for candle in raw_data:
        # Dictionary object
        converted_candle = {
            'time': candle[0],
            'open': float(candle[1]),
            'high': float(candle[2]),
            'low': float(candle[3]),
            'close': float(candle[4]),
            'volume': float(candle[5]),
            'close_time': candle[6],
            'quote_asset_volume': float(candle[7]),
            'number_of_trades': int(candle[8]),
            'taker_buy_base_asset_volume': float(candle[9]),
            'taker_buy_quote_asset_volume': float(candle[10])
        }
        # Add to converted_data
        converted_data.append(converted_candle)
    # Return converted data
    return converted_data


# Function to query Binance for all symbols with a base asset of BUSD
def query_quote_asset_list(quote_asset_symbol):
    # Retrieve a list of symbols from Binance. Returns as a dictionary
    symbol_dictionary = Spot().exchange_info()
    # Convert into a dataframe
    symbol_dataframe = pandas.DataFrame(symbol_dictionary['symbols'])
    # Extract only those symbols with a base asset of BUSD
    quote_symbol_dataframe = symbol_dataframe.loc[symbol_dataframe['quoteAsset'] == quote_asset_symbol]
    # Return base_symbol_dataframe
    return quote_symbol_dataframe
