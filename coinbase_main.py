import time
from coinbase_markettrades_feed import CoinbaseTradeUpdates
from coinbase_orderbook_feed import CoinbaseQuoteUpdates

def main():

    trade_sub = CoinbaseQuoteUpdates()
    quote_sub = CoinbaseTradeUpdates()

    time.sleep(1000)
    trade_sub.websocket.close()
    quote_sub.websocket.close()

if __name__ == '__main__':
    main()