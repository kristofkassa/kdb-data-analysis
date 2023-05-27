import json
import websocket
import threading
from qpython import qconnection
import logging

logging.basicConfig(filename='exchanges_trades.log', encoding='utf-8', level=logging.DEBUG)

# establish connection to the Q service
q = qconnection.QConnection(host='localhost', port=5000)
q.open()

# Binance
def on_message_binance(ws, message):
    json_message = json.loads(message)
    logging.info(json_message)
    
    side = 'sell' if json_message['m'] else 'buy'  # 'm' is 'isBuyerMaker' in the actual JSON message
    print(f"Binance : {json_message['s']} Price: {json_message['p']} Size: {json_message['q']} Side: {side}")

    q_trade_data = '`trades insert (`{symbol}; {price}e; {size}e; `{side}; `Binance; .z.p)'.format(
            symbol = json_message['s'],
            price = float(json_message['p']),
            size = float(json_message['q']),
            side = side
        )

    # Send the data to the q server
    q.sendSync(q_trade_data)

def binance_websocket():
    ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws/btcusdt@trade",
                                on_message = on_message_binance)
    ws.run_forever()

# Coinbase Pro
def on_message_coinbase(ws, message):
    json_message = json.loads(message)
    logging.info(json_message)

    if json_message['type'] == 'ticker':
        print(f"Coinbase Pro : {json_message['product_id']} Price: {json_message['price']} Size: {json_message['last_size']} Side: {json_message['side']}")
    
    q_trade_data = '`trades insert (`{symbol}; {price}e; {size}e; `{side}; `CoinbasePro; .z.p)'.format(
            symbol = json_message['product_id'].replace("-", ""),
            price = float(json_message['price']),
            size = float(json_message['last_size']),
            side = json_message['side']
        )
    
    # Send the data to the q server
    q.sendSync(q_trade_data)

def on_open_coinbase(ws):
    payload = {
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]
    }
    ws.send(json.dumps(payload))

def coinbase_websocket():
    ws = websocket.WebSocketApp("wss://ws-feed.pro.coinbase.com",
                                on_message = on_message_coinbase,
                                on_open = on_open_coinbase)
    ws.run_forever()

# Kraken
def on_message_kraken(ws, message):
    message = json.loads(message)
    logging.info(message)

    if isinstance(message[1], list) and len(message[1]) > 2:  # making sure the message has the expected format
        print(f"Kraken : {message[-1]} Price: {message[1][0][0]} Size: {message[1][0][1]} Side: {message[1][0][3]}")

    q_trade_data = '`trades insert (`{symbol}; {price}e; {size}e; `{side}; `Kraken; .z.p)'.format(
            symbol = str(message[-1]).replace("/", ""),
            price = float(message[1][0][0]),
            size = float(message[1][0][1]),
            side = message[1][0][3]
        )
        # Send the data to the q server
    print(q_trade_data)
    q.sendSync(q_trade_data)

def on_open_kraken(ws):
    payload = {
        "event": "subscribe",
        "pair": ["XBT/USD"],
        "subscription": {
            "name": "trade"
        }
    }
    ws.send(json.dumps(payload))

def kraken_websocket():
    ws = websocket.WebSocketApp("wss://ws.kraken.com",
                                on_message = on_message_kraken,
                                on_open = on_open_kraken)
    ws.run_forever()

if __name__ == "__main__":
    threads = []
    threads.append(threading.Thread(target=binance_websocket))
    threads.append(threading.Thread(target=coinbase_websocket))
    threads.append(threading.Thread(target=kraken_websocket))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
