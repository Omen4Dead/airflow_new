# Import models
from airflow.models.dag import DAG
from airflow.models import Variable

# Import operators, utils
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.dates import days_ago

# Other imports
import requests
import csv
import json
import datetime as dt
from datetime import timedelta
import psycopg2

default_args = {
    "owner": "Nick",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=10)
}


def lastfm_extract():
    context = get_current_context()
    date_from = context['prev_execution_date']
    date_to = context['execution_date']
    print(date_from, '->', date_to)

    params = {'api_key': Variable.get('lastfm_key'),
              'user': Variable.get('lastfm_username'),
              'format': 'json',
              'limit': 200,
              'to': int(date_to.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()),
              'from': int(date_from.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()),
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
    context = get_current_context()
    date_from = context['prev_execution_date']
    date_to = context['execution_date']
    print(date_from, '->', date_to)

    songs = []

    with open(f'./files/raw/lastfm_raw_text_{date_from.date().strftime("%y%m%d")}.json',
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
    if len(songs) == 0:
        return None
    headers = songs[0].keys()

    with open(f'./files/tmp/lastfm_csv_{date_from.date().strftime("%y%m%d")}.csv',
              mode='w',
              encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(songs)


def lastfm_load():
    context = get_current_context()
    date_from = context['prev_execution_date']
    date_to = context['execution_date']
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
                        song_url, dt_listen, image_url)
                      VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, to_timestamp(%s, 'DD Mon YYYY HH24:MI'), %s)"""

    insert_hist_query = """insert into test_db.lastfm_history_data
                           select *
                             from test_db.v_lastfm_unsaved_rows vlur"""

    conn = psycopg2.connect(**config)
    cursor = conn.cursor()

    cursor.execute("""TRUNCATE TABLE test_db.lastfm_raw_data""")

    try:
        with open(path, encoding='utf-8', mode='r', ) as f:
            file = csv.DictReader(f, delimiter=',')
            print(file.fieldnames)
            for row in file:
                cursor.execute(insert_query,
                               [row['artist_mbid'],
                                row['artist_name'],
                                row['streamable'],
                                row['mbid'],
                                row['album_mbid'],
                                row['album_name'],
                                row['song_name'],
                                row['song_url'],
                                row['dt_listen'],
                                row['image_url']
                                ])
            conn.commit()
    except FileNotFoundError as e:
        print(e, '->', 'Файл не создался на предыдущем шаге. Возможно в этот день не было записанных песен')

    cursor.execute(insert_hist_query)
    conn.commit()
    conn.close()


with DAG(
        dag_id='dag_lastfm_get_songs',  # Название - должно совпадать с назвнием файла .py
        default_args=default_args,
        description='Сбор истории прослушиваний с Last.fm',  # Описание
        # schedule_interval='@hourly',
        schedule='0 * * * *',
        start_date=dt.datetime(2018, 1, 1),  # Обязательно дата в прошлом
        max_active_runs=1,
        # max_active_tasks=1,
        tags=['my']
) as dag:
    extract_base = PythonOperator(
        task_id='extract_base',
        python_callable=lastfm_extract
    )

    transform_base = PythonOperator(
        task_id='transform_base',
        python_callable=lastfm_transform
    )

    load_base = PythonOperator(
        task_id='load_base',
        python_callable=lastfm_load
    )

    extract_base >> transform_base >> load_base
