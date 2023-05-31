h:hopen `:localhost:5000

.z.ts: {

    trades: h("trades");

    calculateIchimoku: {
        ichimoku_cloud: select sym, price, conversion_line: prev 9 mavg price,
                        base_line: prev 26 mavg price,
                        leading_span_a: (conversion_line + base_line) % 2,
                        leading_span_b: prev 52 mavg price
                        by exchange from trades;

        // Identify trading opportunities
        buy_opportunities: select from ichimoku_cloud where prev leading_span_a > prev leading_span_b, conversion_line < base_line;
        sell_opportunities: select from ichimoku_cloud where prev leading_span_a < prev leading_span_b, conversion_line > base_line;

        {[exchange; data]
            buy_data: buy_opportunities where exchange=x;
            sell_data: sell_opportunities where exchange=x;
            if[count buy_data; h("Buy opportunity on ",string exchange,": ",string[2#buy_data[`sym]]," at ",string[2#buy_data[`price]])];
            if[count sell_data; h("Sell opportunity on ",string exchange,": ",string[2#sell_data[`sym]]," at ",string[2#sell_data[`price]])];
        } each distinct ichimoku_cloud`exchange
    };

    calculateIchimoku[];
 }

\t 1000
