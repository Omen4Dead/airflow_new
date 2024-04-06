-- Новый скрипт в postgres.
-- Дата: 02 апр. 2024 г.

-- Таблица для приема сырых данных
create table test_db.lastfm_raw_data
(
  artist_mbid text,
  artist_name text,
  streamable smallint,
  mbid text,
  album_mbid text,
  album_name text,
  song_name text,
  song_url text,
  dt_listen timestamp,
  image_url text
);

-- Историческая таблица
create table test_db.lastfm_history_data
as
select md5(lrd.artist_mbid || lrd.artist_name || lrd.streamable || lrd.mbid ||
           lrd.album_mbid || lrd.album_name || lrd.song_name || lrd.song_url ||
           lrd.dt_listen || lrd.image_url) surrogate_key, 
       lrd.*
from test_db.lastfm_raw_data lrd;

-- Добавляем функциональный b-tree индекс для индексного поиска по датам в истории
CREATE INDEX idx_lastfm_history_data_dt_listen ON test_db.lastfm_history_data (date_trunc('day', dt_listen));


-- Сравнение наличия данных и вставка недостающих строк
insert into test_db.lastfm_history_data
 (surrogate_key, artist_mbid, artist_name, streamable, mbid, 
  album_mbid, album_name, song_name, song_url, dt_listen, image_url)
select md5(lrd.artist_mbid || lrd.artist_name || lrd.streamable || lrd.mbid ||
           lrd.album_mbid || lrd.album_name || lrd.song_name || lrd.song_url ||
           lrd.dt_listen || lrd.image_url) surrogate_key, 
       lrd.artist_mbid, lrd.artist_name, lrd.streamable, lrd.mbid,
       lrd.album_mbid, lrd.album_name, lrd.song_name, lrd.song_url,
       lrd.dt_listen, lrd.image_url
from test_db.lastfm_raw_data lrd
where 1=1
  and not exists (select 1
                    from test_db.lastfm_history_data lhd
                   where 1=1
                     and date_trunc('day', lhd.dt_listen) = date_trunc('day', lrd.dt_listen)
                     and lhd.surrogate_key = md5(lrd.artist_mbid || lrd.artist_name || lrd.streamable || lrd.mbid ||
                                                 lrd.album_mbid || lrd.album_name || lrd.song_name || lrd.song_url ||
                                                 lrd.dt_listen || lrd.image_url));


create or replace view test_db.v_lastfm_unsaved_rows
as
select md5(lrd.artist_mbid || lrd.artist_name || lrd.streamable || lrd.mbid ||
           lrd.album_mbid || lrd.album_name || lrd.song_name || lrd.song_url ||
           lrd.dt_listen || lrd.image_url) surrogate_key, 
       lrd.artist_mbid, lrd.artist_name, lrd.streamable, lrd.mbid,
       lrd.album_mbid, lrd.album_name, lrd.song_name, lrd.song_url,
       lrd.dt_listen, lrd.image_url
from test_db.lastfm_raw_data lrd
where 1=1
  and not exists (select 1
                    from test_db.lastfm_history_data lhd
                   where 1=1
                     and date_trunc('day', lhd.dt_listen) = date_trunc('day', lrd.dt_listen)
                     and lhd.surrogate_key = md5(lrd.artist_mbid || lrd.artist_name || lrd.streamable || lrd.mbid ||
                                                 lrd.album_mbid || lrd.album_name || lrd.song_name || lrd.song_url ||
                                                 lrd.dt_listen || lrd.image_url));

                                               
-- Вставка недостающих строк с использованием view
insert into test_db.lastfm_history_data
select *
  from test_db.v_lastfm_unsaved_rows vlur  ;


select lhd.dt_listen,
       lhd.artist_name|| ' :: ' || lhd.album_name || ' :: ' || lhd.song_name as song,
       lhd.dt_listen + interval '5 hour' as dt_local
from test_db.lastfm_history_data lhd 
order by dt_listen desc 
;