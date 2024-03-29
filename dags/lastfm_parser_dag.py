# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG
from airflow.models import Variable

# Operators;
from airflow.operators.python import PythonOperator, get_current_context

# Other imports
import requests
import datetime as dt
import csv
import json

url = Variable.get('lastfm_root_url')
default_args = {
    "owner": "Nick",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(seconds=10)
}


def get_tracks():
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
        print('songs -> ', songs)

        headers = songs[0].keys()
        print(headers)

        with open(f'./files/songs_{execution_date}.csv', 'w') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(songs)


with DAG(
    dag_id='lastfm_parser_dag',  # Название - должно совпадать с назвнием файла .py
    default_args=default_args,  # Словарь аргументов, созданный в 14 строке
    description='',  # Описание
    schedule_interval='@daily',  # Расписание запусков (можно в формате Cron-выражения)
    start_date=dt.datetime(2023, 1, 1),  # Обязательно дата в прошлом
    tags=['my']  # тег - удобно для разбивки DAG на группы
) as dag:

    get_tracks = PythonOperator(
        task_id='get_tracks',
        python_callable=get_tracks,
        dag=dag,
    )

    get_tracks
