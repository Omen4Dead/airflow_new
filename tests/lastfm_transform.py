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
    print(data)
    for track in data:
        row = dict()
        if '@attr' in track:
            continue
        try:
            row['artist_mbid'] = track['artist']['mbid']
            row['artist_name'] = track['artist']['#text']
            row['streamable'] = track['streamable']
            row['mbid'] = track['mbid']
            row['album_mbid'] = track['album']['mbid']
            row['album_name'] = track['album']['#text']
            row['song_name'] = track['name']
            row['song_url'] = track['url']
            row['dt_listen'] = track['date']['#text']
            row['image_url'] = track['image'][0]['#text']
            songs.append(row)
        except KeyError and IndexError as e:
            print(f'Ошибка в json-файле {f.name}. Не все атрибуты попали в словарь.')

print(songs)
headers = songs[0].keys()

with open(f'./files/tmp/lastfm_csv_{yesterday.date().strftime("%y%m%d")}.csv',
          mode='w',
          encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    writer.writerows(songs)
