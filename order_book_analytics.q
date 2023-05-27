h:hopen `:localhost:5000;

.z.ts: {
    
    vwapResult:h("select vwap:sum price*size%sum size from order_book where priority within (-20; 20), inserted_ts = max inserted_ts");

    avgPriceResult:h("select avgPrice:avg price from order_book where priority within (-1; 1), inserted_ts = max inserted_ts");

    spreadResult:h("select spread:max price - min price from order_book where priority within (-1; 1), inserted_ts = max inserted_ts");

    bestBidAskPriceResult:h("select price from order_book where priority in (1,-1), inserted_ts = max inserted_ts");

    lastFuturePricesResult:h("select last_price from futures where instrument_name =`BTCPERPETUAL, creation_timestamp = max creation_timestamp");

    0N!"VWAP Result: ";
    0N!vwapResult `vwap;

    0N!"Average Price Result: ";
    0N!avgPriceResult `avgPrice;

    0N!"Spread Size Result: ";
    0N!spreadResult `spread;

    0N!"Best BID and ASK price: ";
    0N!bestBidAskPriceResult `price;

    0N!"Last BTCPERPETUAL future price: ";
    0N!lastFuturePricesResult `last_price;
    
    }

\t 1000
/
hclose h;
exit 0;