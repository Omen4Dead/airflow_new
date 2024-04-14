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
    curs.execute('TRUNCATE TABLE test_db.lastfm_artists_data_raw')

    curs.execute('''select distinct lhd.artist_name 
                      from test_db.lastfm_history_data lhd ''')

    for row in curs:
        print(row)
        params = {'api_key': cfg.lastfm_key,
                  'format': 'json',
                  'method': 'artist.getinfo',
                  'artist': row[0],
                  'lang': 'ru',
                  'username': cfg.lastfm_username
                  }
        # print(params)
        response = requests.get(url=url, params=params)
        if response.status_code != 200:
            print(response.status_code, response.text)
            break
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
        except AttributeError as e:
            artist_name = ''
        try:
            artist_mbid = df.mbid[0]
        except AttributeError as e:
            artist_mbid = ''
        try:
            artist_url = df.url[0]
        except AttributeError as e:
            artist_url = ''
        try:
            tags = pd.json_normalize(df['tags.tag'][0])['name'].tolist()
        except AttributeError and KeyError as e:
            tags = []
        try:
            dt_published = df['bio.published'][0]
        except AttributeError and KeyError as e:
            dt_published = ''
        artist_attrs = {'name': artist_name,
                        'artist_mbid': artist_mbid,
                        'artist_url': artist_url,
                        'tags': tags,
                        'dt_published': dt_published
                        }
        print(artist_attrs)

        artist_info.append(artist_attrs)

    for row in artist_info:
        curs.execute("""INSERT INTO test_db.lastfm_artists_data_raw (
                                  artist_name, artist_mbid, artist_url,
                                  tags, dt_published)
                                VALUES (
                                  %s, %s, %s,
                                  %s, to_timestamp(%s, 'DD Mon YYYY, HH24:MI'))""",
                     [row['name'],
                      row['artist_mbid'],
                      row['artist_url'],
                      row['tags'],
                      row['dt_published']
                      ]
                     )
    conn.commit()

    curs.execute("""INSERT INTO test_db.lastfm_artists_data
                    (surrogate_key, artist_name, artist_mbid, artist_url, tags, dt_published)
                    SELECT surrogate_key, artist_name, artist_mbid, artist_url, tags, dt_published
                      FROM test_db.v_lastfm_artist_unsaved_rows""")
    conn.commit()

