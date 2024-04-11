import config as cfg
import requests
import pandas as pd
import psycopg2

# подключаемся к pg и помещаем в курсор нужные нам данные по исполнителю
pg_config = {
    'host': "localhost",
    'database': "postgres",
    'user': "postgres",
    'password': "postgres"}
conn = psycopg2.connect(**pg_config)

url = cfg.lastfm_root_url
artist_info = []

with conn.cursor() as curs:
    curs.execute('''select lhd.artist_mbid , lhd.artist_name 
                      from test_db.lastfm_history_data lhd 
                     where date_trunc('day', lhd.dt_listen) = current_date - 2''')

    for row in curs:
        params = {'api_key': cfg.lastfm_key,
                  'format': 'json',
                  'method': 'artist.getinfo',
                  'artist': row[1],
                  'lang': 'ru',
                  'username': cfg.lastfm_username
                  }
        # print(params)
        response = requests.get(url=url, params=params)
        # print(response.text)
        try:
            df = pd.json_normalize(response.json()['artist'])
        except KeyError as e:
            print(e, ' -> Ошибка разбора JSON, вероятно, что артист с таким именем не нашелся')
            break
        # распечатываем список атрибутов после из нормализации
        # print(df.keys())
        try:
            artist_name = df.name[0]
            # artist_mbid = df.mbid[0]
            artist_url = df.url[0]
            tags = pd.json_normalize(df['tags.tag'][0])['name'].tolist()
            dt_published = df['bio.published'][0]
        except AttributeError and KeyError as e:
            print(e, ' -> Ошибка разбора JSON, вероятно, что атрибут не пришел с источника')
            continue

        artist_info.append({'name': artist_name,
               # 'artist_mbid': artist_mbid,
               'artist_url': artist_url,
               'tags': tags,
               'dt_published': dt_published
               })

print(artist_info)

artists_df = pd.DataFrame(artist_info)

artists_df.to_csv('./files/artists.csv')
