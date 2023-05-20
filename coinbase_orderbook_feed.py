from collections import OrderedDict
from datetime import datetime, timezone
import hashlib
import hmac
import json
import logging
import threading
import time
from typing import List
from pydantic import BaseModel
import websocket
from qpython import qconnection

logging.basicConfig(filename='quotes.log', encoding='utf-8', level=logging.DEBUG)

# Quote 
class QuoteUpdate(BaseModel):
    side: str
    event_time: datetime
    price_level: float
    new_quantity: float

class QuoteEvent(BaseModel):
    type: str
    product_id: str
    updates: List[QuoteUpdate]

class QuoteMessage(BaseModel):
    channel: str
    client_id: str
    timestamp: datetime
    sequence_num: int
    events: List[QuoteEvent]


class CoinbaseQuoteUpdates:

    def __init__(self, product_ids=['BTC-USD']):
        self.buy_orders = {}  # bids
        self.sell_orders = {} # asks
        self.product_ids = product_ids
        self.ws_url = f"wss://advanced-trade-ws.coinbase.com"
        self.websocket = None
        self._connect()

        # Set up the q connection
        self.q = qconnection.QConnection(host='localhost', port=5000)
        self.q.open()

    def _connect(self):
        self.websocket = websocket.WebSocketApp(
            self.ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

        ws_thread = threading.Thread(target=self.websocket.run_forever)
        ws_thread.start()
    
    def _on_open(self, ws):
        api_key = "EWsp4HAsPt4vz7gc"
        secret_key = "A2mfiJjWpnYLXrX4bpGmOVF1ijDb7C1J"
        timestamp = str(int(time.time()))
        channel_name = "level2"

        # Concatenate and comma-separate the timestamp, channel name, and product IDs
        message = timestamp + channel_name + ','.join(self.product_ids)
        # Hash the message with the secret key using HMAC SHA256
        signature = hmac.new(bytes(secret_key, 'utf-8'), bytes(message, 'utf-8'), hashlib.sha256).hexdigest()

        subscribe_message = {
            "type": "subscribe",
            "product_ids": self.product_ids,
            "channel": channel_name,
            "signature": signature,
            "api_key": api_key,
            "timestamp": timestamp,
        }
        self.websocket.send(json.dumps(subscribe_message))

    def _on_message(self, ws, message):
        print("Quote update received.")
        data = json.loads(message)
        quote_message = QuoteMessage.parse_obj(data)

        ts = datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f")[:-4]

        for event in quote_message.events:
            if event.type == "snapshot":
                logging.info("snapshot received!")
                # logging.info(quote_message)
                for update in event.updates:
                    if update.side == "bid" and update.new_quantity > 0:
                        self.buy_orders[update.price_level] = [update.new_quantity, event.product_id, update.side, update.event_time, quote_message.timestamp]
                    elif update.side == "offer" and update.new_quantity > 0:
                        self.sell_orders[update.price_level] = [update.new_quantity, event.product_id, update.side, update.event_time, quote_message.timestamp]
                self.buy_orders = OrderedDict(sorted(self.buy_orders.items(), reverse=True))
                self.sell_orders = OrderedDict(sorted(self.sell_orders.items()))

            elif event.type == "update":
                logging.info("update received!")
                # logging.info(quote_message)
                for update in event.updates:
                    # Update or remove price levels with incoming updates
                    if update.side == "bid":
                        if update.price_level in self.buy_orders:
                            if update.new_quantity == 0:
                                # Remove price level with 0 quantity
                                del self.buy_orders[update.price_level]
                            else:
                                # Update quantity of existing order
                                self.buy_orders[update.price_level] = [update.new_quantity, event.product_id, update.side, update.event_time, quote_message.timestamp]
                        elif update.new_quantity > 0:
                            # Add new order
                            self.buy_orders[update.price_level] = [update.new_quantity, event.product_id, update.side, update.event_time, quote_message.timestamp]
                        self.buy_orders = OrderedDict(sorted(self.buy_orders.items(), reverse=True))
                        self.buy_orders = OrderedDict(list(self.buy_orders.items())[:50])

                    elif update.side == "offer":
                        if update.price_level in self.sell_orders:
                            if update.new_quantity == 0:
                                # Remove price level with 0 quantity
                                del self.sell_orders[update.price_level]
                            else:
                                # Update quantity of existing order
                                self.sell_orders[update.price_level] = [update.new_quantity, event.product_id, update.side, update.event_time, quote_message.timestamp]
                        elif update.new_quantity > 0:
                            # Add new order
                            self.sell_orders[update.price_level] = [update.new_quantity, event.product_id, update.side, update.event_time, quote_message.timestamp]
                        self.sell_orders = OrderedDict(sorted(self.sell_orders.items()))
                        self.sell_orders = OrderedDict(list(self.sell_orders.items())[:50])

                self.process_orders_q(self.sell_orders, ts)
                self.process_orders_q(self.buy_orders, ts)

    def process_orders_q(self, order_dict, ts):
        for i, (price_level, details) in enumerate(order_dict.items()):

            if details[2] == "bid":
                priority = -(i+1)
            else:
                priority = i+1


            # logging.info(f"Order {i+1}: Price level: {price_level}, Quantity: {details[0]}, Symbol: {details[1]}, Side: {details[2]}, Event Time: {details[3]}, Message Time: {details[4]}")
            q_quote = '`order_book upsert ({price}e; {size}e; `{side}; {priority}i; `{symbol}; {batch_ts}; {event_ts}; {message_ts})'.format(
                        price = float(price_level),
                        size = float(details[0]),
                        symbol = str(details[1]).replace("-", ""),
                        side = str(details[2]),
                        priority = priority,
                        batch_ts = ts,
                        event_ts = datetime.fromisoformat(str(details[3])).astimezone(timezone.utc).strftime("%Y.%m.%dD%H:%M:%S.%f")[:-4],
                        message_ts = datetime.fromisoformat(str(details[4])).astimezone(timezone.utc).strftime("%Y.%m.%dD%H:%M:%S.%f")[:-4]
                        )

            # Send the data to the q server
            self.q.sendSync(q_quote)

    def _on_error(self, ws, error):
        print(f"WebSocket error: {error}")

    def _on_close(self, ws):
        print("WebSocket connection closed.")
    