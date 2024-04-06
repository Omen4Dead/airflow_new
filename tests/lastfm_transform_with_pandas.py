import pandas as pd
import csv
import datetime as dt
import json

today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
yesterday = today - dt.timedelta(days=1)

print(yesterday)
print(today)

songs = []

with open(f'./files/raw/lastfm_raw_text_{yesterday.date().strftime("%y%m%d")}.json',
          mode='r',
          encoding='utf-8') as f:
    file = json.load(f)
    data = file['recenttracks']['track']
    df = pd.json_normalize(data)
    print(df.head())
    df.to_csv(f'./files/tmp/lastfm_csv_{yesterday.date().strftime("%y%m%d")}.csv', sep=',')

