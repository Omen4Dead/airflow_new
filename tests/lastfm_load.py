import psycopg2
import datetime as dt
import csv

today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
yesterday = today - dt.timedelta(days=1)

config = {
    'host': "localhost",
    'database': "postgres",
    'user': "postgres",
    'password': "postgres"}

path = f'./files/tmp/lastfm_csv_{yesterday.date().strftime("%y%m%d")}.csv'

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

with open(path, encoding='utf-8', mode='r') as f:
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

cursor.execute(insert_hist_query)
conn.commit()
conn.close()
