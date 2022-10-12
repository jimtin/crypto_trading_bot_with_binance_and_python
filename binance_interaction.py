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
            'open_time': candle[0],
            'open': candle[1],
            'high': candle[2],
            'low': candle[3],
            'close': candle[4],
            'volume': candle[5],
            'close_time': candle[6],
            'quote_asset_volume': candle[7],
            'number_of_trades': candle[8],
            'taker_buy_base_asset_volume': candle[9],
            'taker_buy_quote_asset_volume': candle[10]
        }
        # Add to converted_data
        converted_data.append(converted_candle)
    # Return converted data
    return converted_data
