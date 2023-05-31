h:hopen `:localhost:5000;

.z.ts: {
    
    avgTrades: h("select price:size wavg price by exchange from (select from trades order by trade_ts desc limit 20)");

    0N!"Average Trades: ";
    0N!avgTrades;
    };

\t 1000
