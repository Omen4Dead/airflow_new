# Import models
from airflow.models.dag import DAG
from airflow.models import Variable

# Import operators, utils
from airflow.operators.python import PythonOperator, get_current_context

# Other imports
import requests
import csv
import json
import datetime as dt
from datetime import timedelta
import psycopg2
import pandas as pd

default_args = {
    "owner": "Nick",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=10)
}


def get_context():
    """
    Получение лога с контекстом выполнения. Удобно для дебага
    :return:
    """
    context = get_current_context()
    for k, v in context.items():
        print(k, '->', v)


def lastfm_extract():
    """
    Получение данных по API и сохранение в исходном виде как JSON
    :return:
    """
    context = get_current_context()
    date_from = context['prev_execution_date'].replace(hour=0, minute=0, second=0, microsecond=0)
    date_to = context['execution_date'].replace(hour=0, minute=0, second=0, microsecond=0)
    print(date_from, '->', date_to)

    params = {'api_key': Variable.get('lastfm_key'),
              'user': Variable.get('lastfm_username'),
              'format': 'json',
              'limit': 200,
              'to': int(date_to.timestamp()),
              'from': int(date_from.timestamp()),
              'method': 'user.getrecenttracks'
              }

    response = requests.get(url=Variable.get('lastfm_root_url'), params=params)
    print(response.headers)

    if response.status_code == 200:
        with open(f'./files/raw/lastfm_raw_text_{date_from.date().strftime("%y%m%d")}.json',
                  mode='w',
                  encoding='utf-8') as f:
            f.write(response.text)
    else:
        raise ValueError(f'Что-то не так с запросом. Возвращается статус {response.status_code}')


def lastfm_transform():
    """
    Парсинг JSON и раскладывание его в CSV с использованием стандартных библиотек
    :return:
    """
    context = get_current_context()
    date_from = context['prev_execution_date'].replace(hour=0, minute=0, second=0, microsecond=0)
    date_to = context['execution_date'].replace(hour=0, minute=0, second=0, microsecond=0)
    print(date_from, '->', date_to)

    songs = []

    with open(f'./files/raw/lastfm_raw_text_{date_from.date().strftime("%y%m%d")}.json',
              mode='r',
              encoding='utf-8') as f:
        file = json.load(f)
        data = file['recenttracks']['track']
        print(data)
        if len(data) == 0:
            return None
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

    headers = songs[0].keys()

    with open(f'./files/tmp/lastfm_csv_{date_from.date().strftime("%y%m%d")}_old.csv',
              mode='w',
              encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(songs)


def lasfm_transform_pandas():
    """
    Парсинг JSON с использованием pandas
    :return:
    """
    context = get_current_context()
    date_from = context['prev_execution_date'].replace(hour=0, minute=0, second=0, microsecond=0)
    date_to = context['execution_date'].replace(hour=0, minute=0, second=0, microsecond=0)
    print(date_from, '->', date_to)

    with open(file=f'./files/raw/lastfm_raw_text_{date_from.date().strftime("%y%m%d")}.json',
              mode='r',
              encoding='utf-8') as f:
        file = json.load(f)
        data = file['recenttracks']['track']
        df = pd.json_normalize(data)
        print(df.head())
        df.to_csv(path_or_buf=f'./files/tmp/lastfm_csv_{date_from.date().strftime("%y%m%d")}.csv',
                  sep=',')


def lastfm_load():
    """
    Загрузка данных из CSV d
    :return:
    """
    context = get_current_context()
    date_from = context['prev_execution_date'].replace(hour=0, minute=0, second=0, microsecond=0)
    date_to = context['execution_date'].replace(hour=0, minute=0, second=0, microsecond=0)
    print(date_from, '->', date_to)
    config = {
        'host': "host.docker.internal",
        'database': "postgres",
        'user': "postgres",
        'password': "postgres"}

    path = f'./files/tmp/lastfm_csv_{date_from.date().strftime("%y%m%d")}.csv'

    insert_query = """INSERT INTO test_db.lastfm_raw_data (
                        artist_mbid, artist_name, streamable, mbid,
                        album_mbid, album_name, song_name,
                        song_url, dt_listen)
                      VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, to_timestamp(%s, 'DD Mon YYYY HH24:MI'))"""

    insert_hist_query = """insert into test_db.lastfm_history_data
                           select *
                             from test_db.v_lastfm_unsaved_rows vlur"""

    conn = psycopg2.connect(**config)
    cursor = conn.cursor()

    cursor.execute("""TRUNCATE TABLE test_db.lastfm_raw_data""")

    with open(path, encoding='utf-8', mode='r', ) as f:
        file = csv.DictReader(f, delimiter=',')
        print(file.fieldnames)
        for row in file:
            try:
                if row['date.#text'] == '':
                    continue
            except KeyError as e:
                print(e, '->', 'За этот период не было прослушиваний')
                break
            cursor.execute(insert_query,
                           [row['artist.mbid'],
                            row['artist.#text'],
                            row['streamable'],
                            row['mbid'],
                            row['album.mbid'],
                            row['album.#text'],
                            row['name'],
                            row['url'],
                            row['date.#text']
                            ])
        conn.commit()

    cursor.execute(insert_hist_query)
    conn.commit()
    conn.close()


def lastfm_get_artists_info():
    pg_config = {
        'host': "host.docker.internal",
        'database': "postgres",
        'user': "postgres",
        'password': "postgres"}
    conn = psycopg2.connect(**pg_config)

    url = Variable.get('lastfm_root_url')
    artist_info = []

    with conn.cursor() as curs:
        curs.execute('TRUNCATE TABLE test_db.lastfm_artists_data_raw')

        curs.execute('''select distinct lhd.artist_name 
                          from test_db.lastfm_history_data lhd 
                         where date_trunc('day', lhd.dt_listen) = current_date - 2
                           and not exists (select 1
                                             from test_db.lastfm_artists_data lad
                                            where lhd.artist_name = lad.artist_name)''')

        for row in curs:
            # print(row)
            params = {'api_key': Variable.get('lastfm_key'),
                      'format': 'json',
                      'method': 'artist.getinfo',
                      'artist': row[0],
                      'lang': 'ru',
                      'username': Variable.get('lastfm_username')
                      }
            # print(params)
            response = requests.get(url=url, params=params)
            if response.status_code != 200:
                print(response.status_code, response.text)
                raise ResourceWarning
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
            # print(artist_attrs)

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
                        select md5(raw.artist_name || raw.artist_mbid || raw.artist_url) surrogate_key, 
                               raw.artist_name, raw.artist_mbid,
                               raw.artist_url,  raw.tags,
                               raw.dt_published
                          from test_db.lastfm_artists_data_raw raw
                            ON conflict (surrogate_key) DO UPDATE 
                           SET tags = EXCLUDED.tags,
                               dt_published = EXCLUDED.dt_published,
                               dt_insert = EXCLUDED.dt_insert""")
        conn.commit()

        
with DAG(
        dag_id='dag_lastfm_get_songs',  # Название - должно совпадать с назвнием файла .py
        default_args=default_args,
        description='Сбор истории прослушиваний с Last.fm',  # Описание
        # schedule_interval='@hourly',
        schedule='@daily',
        start_date=dt.datetime(2018, 1, 1),  # Обязательно дата в прошлом
        max_active_runs=1,
        # max_active_tasks=1,
        tags=['my']
) as dag:

    start_context = PythonOperator(
        task_id='start_context',
        python_callable=get_context
    )

    extract_base = PythonOperator(
        task_id='extract_base',
        python_callable=lastfm_extract
    )

    transform_base = PythonOperator(
        task_id='transform_base',
        python_callable=lastfm_transform
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=lasfm_transform_pandas
    )

    load_base = PythonOperator(
        task_id='load_base',
        python_callable=lastfm_load,
        trigger_rule='one_success'
    )

    load_artist_info = PythonOperator(
        task_id='load_artist_info',
        python_callable=lastfm_get_artists_info,
        trigger_rule='one_success'
    )

    start_context >> extract_base >> [transform_base, transform] >> load_base >> load_artist_info
