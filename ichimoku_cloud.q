// Connect to the q process running on port 5000
h:hopen `:localhost:5000

.z.ts: {

    // Fetch the trades table from the remote q process
    trades: h"trades";

    // Calculate the Ichimoku Cloud components and identify trading opportunities
    calculateIchimoku: {
        // Calculate the Ichimoku Cloud components
        ichimoku_cloud: select sym, price, conversion_line: prev 9 mavg price,
                        base_line: prev 26 mavg price,
                        leading_span_a: (conversion_line + base_line) % 2,
                        leading_span_b: prev 52 mavg price
                        by exchange from trades;

        // Identify trading opportunities
        buy_opportunities: select from ichimoku_cloud where prev leading_span_a > prev leading_span_b, conversion_line < base_line;
        sell_opportunities: select from ichimoku_cloud where prev leading_span_a < prev leading_span_b, conversion_line > base_line;

        // Print the trading opportunities
        {[exchange; data]
            h"Buy opportunity on ",string exchange,": ",string[2#data[`sym]]," at ",string[2#data[`price]] each buy_opportunities[exchange];
            h"Sell opportunity on ",string exchange,": ",string[2#data[`sym]]," at ",string[2#data[`price]] each sell_opportunities[exchange]
        } each distinct ichimoku_cloud`exchange
    };

    // Call the calculateIchimoku function
    calculateIchimoku[];
    }

// Set the timer interval to 1000 milliseconds (1 second)
\t 1000

