
.z.ts: {
    // The current time is obtained using .z.p (current datetime), then extracting the time part.
    lastMinute: .z.p - 1D00:01;

    // Filter the trades for the last minute
    recentTrades: select from trades where trade_ts within (lastMinute; .z.p);

    // Calculate the moving average for each exchange
    avgTrades: select price:size wavg price by exchange from recentTrades;

    // Print the result
    show avgTrades;
    };

// Set the timer to call the function every 10 seconds
\t 10000
