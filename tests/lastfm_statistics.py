import requests
import config
import datetime as dt
import csv
import json

url = config.lastfm_root_url
today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
yesterday = today - dt.timedelta(days=1)

print(today)
print(yesterday)

params = {'api_key': config.lastfm_key,
          'user': config.lastfm_usermane,
          'format': 'json',
          'limit': 200,
          'from': int(yesterday.timestamp())
          # 'to': int(today.timestamp())
          }


def get_recent_tracks(url, params):
    params['method'] = 'user.getrecenttracks'
    return requests.get(url=url, params=params)


def get_tracks_from_json(resp):
    return resp.json()['recenttracks']['track']


def get_attrs_from_json(resp):
    return resp.json()['recenttracks']['@attr']


response = get_recent_tracks(url, params)
tracks = get_tracks_from_json(response)
attrs = get_attrs_from_json(response)


data = json.loads(response.text)['recenttracks']['track']
# print(data)

if '@attr' in data[0]:
    data.pop(0)

headers = data[0].keys()
# print(data[0])


with open('file.csv', 'w') as f:
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    writer.writerows(data)

row = {}
for track in data:
    row = {'artist_mbid': track['artist']['mbid'],
           'artist_name': track['artist']['#text'],
           'mbid': track['mbid'],
           'album_mbid': track['album']['mbid'],
           'album_name': track['album']['#text'],
           'song_name': track['name'],
           'song_url': track['url'],
           'dt_listen': track['date']['#text']}
    print(row)
