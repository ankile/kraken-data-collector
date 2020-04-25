import requests
import subprocess
import time
import pandas as pd
from datetime import datetime

from constants import pairs, intervals, headers


url = 'https://api.kraken.com/0/public/OHLC'

while True:
    for pair in pairs:
        for interval in intervals:
            print(f'Getting data for {pair} {interval} minute bars')
            params = {
                'pair': pair,
                'interval': interval,
            }
            resp = requests.get(
                url,
                params=params,
                # headers=headers,
            )

            res = resp.json()

            if len(res['error']) > 0:
                print(f"Received error for {pair} at interval {interval}")

                with open('./error_log.csv', 'a') as f:
                    f.write(f'{datetime.now()};{pair};{interval};{",".join(res["error"])}\n')

            else:
                result = res['result']

                prices = list(res['result'].values())[0]
                last = res['result']['last']

                new_df = pd.DataFrame(prices, columns=['time', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'count'])
                new_df['time_fmt'] = pd.to_datetime(new_df['time'], unit='s', utc=True)
                
                csv_path = f'data/{interval}/{pair}/1.csv'
                old_df = pd.read_csv(csv_path)

                save_df = pd.concat([old_df, new_df], ignore_index=True).drop_duplicates(['time'])[headers]
                save_df.to_csv(csv_path, index=False)

            resp.close()


            time.sleep(1)
    print(f"Sleeping for an hour starting at {datetime.now()}")
    time.sleep(120)