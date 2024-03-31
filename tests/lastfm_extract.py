import requests
import config
import datetime as dt
import csv
import json

url = config.lastfm_root_url
today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
yesterday = today - dt.timedelta(days=1)

print(yesterday)
print(today)

params = {'api_key': config.lastfm_key,
          'user': config.lastfm_usermane,
          'format': 'json',
          'limit': 200,
          'to': int(today.timestamp()),
          'from': int(yesterday.timestamp()),
          'method': 'user.getrecenttracks'
          }
print(params)

response = requests.get(url=url, params=params)
print(response)
print(response.text)

with open(f'./files/raw/lastfm_raw_text_{yesterday.date().strftime("%y%m%d")}', 'w', encoding='utf-8') as f:
    f.write(response.text)

with open(f'./files/raw/lastfm_raw_json_{yesterday.date().strftime("%y%m%d")}', 'w', encoding='utf-8') as f:
    f.write(str(response.json()))
