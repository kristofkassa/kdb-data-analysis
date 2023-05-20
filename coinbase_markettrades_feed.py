# Market Trade
from datetime import datetime
import hashlib
import hmac
import json
import threading
import time
from typing import List
from pydantic import BaseModel
import websocket
from qpython import qconnection
import logging

logging.basicConfig(filename='trades.log', encoding='utf-8', level=logging.DEBUG)


class MarketTrade(BaseModel):
    trade_id: str
    product_id: str
    price: int
    size: float
    side: str
    time: datetime

class MarketTradeEvent(BaseModel):
    type: str
    trades: List[MarketTrade]

class MarketTradeMessage(BaseModel):
    channel: str
    client_id: str
    timestamp: datetime
    sequence_num: int
    events: List[MarketTradeEvent]


class CoinbaseTradeUpdates:

        def __init__(self, product_ids=["BTC-USD"]):
            self.product_ids = product_ids
            self.ws_url = f"wss://advanced-trade-ws.coinbase.com"
            self.websocket = None
            self._connect()
            self.market_trades = []

            # Set up the q connection
            self.q = qconnection.QConnection(host='localhost', port=5000)
            self.q.open()

        def _connect(self):
            self.websocket = websocket.WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
            )

            ws_thread = threading.Thread(target=self.websocket.run_forever)
            ws_thread.start()
        
        def _on_open(self, ws):

            api_key = "EWsp4HAsPt4vz7gc"
            secret_key = "A2mfiJjWpnYLXrX4bpGmOVF1ijDb7C1J"
            timestamp = str(int(time.time()))
            channel_name = "market_trades"

            # Concatenate and comma-separate the timestamp, channel name, and product IDs
            message = timestamp + channel_name + ','.join(self.product_ids)
            # Hash the message with the secret key using HMAC SHA256
            signature = hmac.new(bytes(secret_key, 'utf-8'), bytes(message, 'utf-8'), hashlib.sha256).hexdigest()

            subscribe_message = {
                "type": "subscribe",
                "product_ids": self.product_ids,
                "channel": channel_name,
                "api_key": api_key,
                "timestamp": timestamp,
                "signature": signature,
            }
            self.websocket.send(json.dumps(subscribe_message))

        def _on_message(self, ws, message):
            data = json.loads(message)
            print("Trade update received.")
            logging.info(data)

            for event in data['events']:
                for trade in event['trades']:
                    # Extract the trade data
                    q_trade_data = '`trades insert ({id}i; .z.D; .z.T; `{symbol}; {price}e; {size}e; `{side})'.format(
                                id = trade['trade_id'],
                                symbol = trade['product_id'].replace("-", ""),
                                price = float(trade['price']),
                                size = float(trade['size']),
                                side = trade['side']
                                )

                    # Send the data to the q server
                    self.q.sendSync(q_trade_data)

        def _on_error(self, ws, error):
            print(f"WebSocket error: {error}")

        def _on_close(self, ws):
            print("WebSocket connection closed.")