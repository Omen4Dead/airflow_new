from airflow.models.dag import DAG
from airflow.models import Variable

from airflow.operators.python import PythonOperator, get_current_context

import requests
import datetime as dt
import csv
import json

url = Variable.get('lastfm_root_url')
default_args = {
    "owner": "Nick",
    "depends_on_past": False,
    "email": ["al@ex.com"]
}


def get_context():
    context = get_current_context()
    for k, v in context.items():
        print(k, ' -> ', v)


def get_tracks_from_api():
    context = get_current_context()
    execution_date = context['logical_date']
    date_from = execution_date - dt.timedelta(days=1)
    date_to = execution_date
    params = {'api_key': Variable.get('lastfm_key'),
              'user': Variable.get('lastfm_username'),
              'format': 'json',
              'limit': 200,
              'method': 'user.getrecenttracks',
              'to': int(date_to.timestamp()),
              'from': int(date_from.timestamp())
              }
    response = requests.get(url=url, params=params)

    data = json.loads(response.text)['recenttracks']['track']
    print(data)
    if not len(data):
        print('В этот день ничего не слушали')
    elif '@attr' in data[0]:
        data.pop(0)
        songs = []
        for track in data:
            row = {'artist_mbid': track['artist']['mbid'],
                   'artist_name': track['artist']['#text'],
                   'mbid': track['mbid'],
                   'album_mbid': track['album']['mbid'],
                   'album_name': track['album']['#text'],
                   'song_name': track['name'],
                   'song_url': track['url'],
                   'dt_listen': track['date']['#text']}
            songs.append(row)
        headers = songs[0].keys()
        print('headers -> ', headers)
        with open(f'./files/tmp/songs_{context["ds_nodash"]}.csv', 'w') as f:
            print('filename -> ', f.name)
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(songs)


with DAG(
    dag_id='lastfm_get_from_api',  # Название - должно совпадать с назвнием файла .py
    default_args=default_args,  # Словарь аргументов, созданный в 14 строке
    description='Получение данных о песнях через API',  # Описание
    schedule_interval='@daily',  # Расписание запусков (можно в формате Cron-выражения)
    start_date=dt.datetime(2024, 3, 10),  # Обязательно дата в прошлом
    max_active_runs=1,
    max_active_tasks=1,
    tags=['my']  # тег - удобно для разбивки DAG на группы
) as dag:

    get_context = PythonOperator(
        task_id='get_context',
        python_callable=get_context,
        dag=dag
    )

    get_tracks = PythonOperator(
        task_id='get_tracks',
        python_callable=get_tracks_from_api,
        dag=dag
    )

    get_context >> get_tracks