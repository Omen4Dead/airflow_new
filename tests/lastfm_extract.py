import datetime as dt
import requests
import config

url = config.lastfm_root_url
today = dt.datetime(2018, 1, 1).replace(hour=0, minute=0, second=0, microsecond=0)
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

if response.status_code == 200:
    with open(f'./files/raw/lastfm_raw_text_{yesterday.date().strftime("%y%m%d")}.json',
              mode='w',
              encoding='utf-8') as f:
        f.write(response.text)
else:
    raise ValueError(f'Что-то не так с запросом. Возвращается статус {response.status_code}')
