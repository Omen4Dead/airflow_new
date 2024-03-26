import requests
import config
import datetime as dt
import csv
import pandas as pd

url = config.lastfm_root_url
today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
yesterday = today - dt.timedelta(days=1)

print(today)
print(yesterday)

params = {'api_key': config.lastfm_key,
          'user': config.lastfm_usermane,
          'format': 'json',
          'limit': 200,
          'from': int(yesterday.timestamp()),
          'to': int(today.timestamp())}


def get_recent_tracks(url, params):
    params['method'] = 'user.getrecenttracks'
    return requests.get(url=url, params=params)


response = get_recent_tracks(url, params)
tracks = response.json()['recenttracks']['track']
attrs = response.json()['recenttracks']['@attr']
print(attrs)
print('-' * 100)
for track in tracks:
    # print(i['artist']['#text'], i['album']['#text'], i['name'])
    for k, v in track.items():
        print(k, '->', v)
    print('-' * 100)

