import aiohttp
import asyncio
from datetime import datetime
from qpython import qconnection

async def fetch_data():
    url = "https://test.deribit.com/api/v2/public/get_book_summary_by_currency"
    params = {
        "currency": "BTC",
        "kind": "future"
    }
    headers = {
        "Content-Type": "application/json"
    }

    # establish connection to the Q service
    q = qconnection.QConnection(host='localhost', port=5000)
    q.open()

    while True:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers) as response:
                response_json = await response.json()

                # Parse and access the relevant data from the JSON response
                if "result" in response_json:
                    result = response_json["result"]
                    for item in result:
                        # Convert 'creation_timestamp' from Unix timestamp (milliseconds) to datetime object
                        dt = datetime.fromtimestamp(int(item['creation_timestamp'])/1000.0)

                        # Format datetime object into the Q timestamp format
                        creation_timestamp = dt.strftime('%Y.%m.%dD%H:%M:%S.%f')

                        q_futures_data = '`futures insert (`{instrument_name}; {volume_btc}e; {volume_usd}e; {last_price}e; {bid_price}e; {ask_price}e; {low}e; {high}e; {creation_timestamp})'.format(
                                instrument_name = item['instrument_name'].replace("-", ""),
                                volume_btc = float(item['volume']),
                                volume_usd = float(item['volume_usd']),
                                last_price = float(item['last']),
                                bid_price = float(item['bid_price']),
                                ask_price = float(item['ask_price']),
                                low = float(item['low']),
                                high = float(item['high']),
                                creation_timestamp = creation_timestamp
                                )
                        print("Deribit Futures trade update received.")
                        # Send the data to the q server
                        q.sendSync(q_futures_data)

        # Wait for 5 seconds before fetching the data again
        await asyncio.sleep(2)

# Create an event loop and run the fetch_data function asynchronously
loop = asyncio.get_event_loop()
loop.run_until_complete(fetch_data())

if __name__ == '__main__':
    fetch_data()