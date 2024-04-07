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

path = f'./files/tmp/lastfm_csv_from_pd_{yesterday.date().strftime("%y%m%d")}.csv'

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

with open(path, encoding='utf-8', mode='r') as f:
    file = csv.DictReader(f, delimiter=',')
    print(file.fieldnames)
    for row in file:
        if row['date.#text'] == '':
            continue
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

# cursor.execute(insert_hist_query)
# conn.commit()
conn.close()
