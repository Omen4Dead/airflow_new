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
  dt_listen timestamp
);

alter table test_db.lastfm_history_data add column dt_insert timestamptz default now();

-- Историческая таблица
create table test_db.lastfm_history_data
as
select md5(lrd.artist_mbid || lrd.artist_name || lrd.streamable || lrd.mbid ||
           lrd.album_mbid || lrd.album_name || lrd.song_name || lrd.song_url ||
           lrd.dt_listen) surrogate_key, 
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
           lrd.dt_listen) surrogate_key, 
       lrd.artist_mbid, lrd.artist_name, lrd.streamable, lrd.mbid,
       lrd.album_mbid, lrd.album_name, lrd.song_name, lrd.song_url,
       lrd.dt_listen
from test_db.lastfm_raw_data lrd
where 1=1
  and not exists (select 1
                    from test_db.lastfm_history_data lhd
                   where 1=1
                     and date_trunc('day', lhd.dt_listen) = date_trunc('day', lrd.dt_listen)
                     and lhd.surrogate_key = md5(lrd.artist_mbid || lrd.artist_name || lrd.streamable || lrd.mbid ||
                                                 lrd.album_mbid || lrd.album_name || lrd.song_name || lrd.song_url ||
                                                 lrd.dt_listen));

drop view test_db.v_lastfm_unsaved_rows;                        
create view test_db.v_lastfm_unsaved_rows
as
select md5(lrd.artist_mbid || lrd.artist_name || lrd.streamable || lrd.mbid ||
           lrd.album_mbid || lrd.album_name || lrd.song_name || lrd.song_url ||
           lrd.dt_listen) surrogate_key, 
       lrd.artist_mbid, lrd.artist_name, lrd.streamable, lrd.mbid,
       lrd.album_mbid, lrd.album_name, lrd.song_name, lrd.song_url,
       lrd.dt_listen
from test_db.lastfm_raw_data lrd
where 1=1
  and not exists (select 1
                    from test_db.lastfm_history_data lhd
                   where 1=1
                     and date_trunc('day', lhd.dt_listen) = date_trunc('day', lrd.dt_listen)
                     and lhd.surrogate_key = md5(lrd.artist_mbid || lrd.artist_name || lrd.streamable || lrd.mbid ||
                                                 lrd.album_mbid || lrd.album_name || lrd.song_name || lrd.song_url ||
                                                 lrd.dt_listen));

                                               
-- Вставка недостающих строк с использованием view
insert into test_db.lastfm_history_data
select *
  from test_db.v_lastfm_unsaved_rows vlur  ;


select lhd.dt_listen,
       lhd.artist_name|| ' :: ' || lhd.album_name || ' :: ' || lhd.song_name as song,
       lhd.dt_listen + interval '5 hour' as dt_local,
       lhd.dt_insert 
from test_db.lastfm_history_data lhd 
order by dt_listen desc 
limit 10
;

update test_db.lastfm_history_data lrd
set surrogate_key = md5(lrd.artist_mbid || lrd.artist_name || lrd.streamable || lrd.mbid ||
           lrd.album_mbid || lrd.album_name || lrd.song_name || lrd.song_url ||
           lrd.dt_listen);

select *
  from test_db.v_lastfm_unsaved_rows vlur;
  
select lhd.artist_mbid , lhd.artist_name 
from test_db.lastfm_history_data lhd 
where date_trunc('day', lhd.dt_listen) = current_date - 2;


drop table test_db.lastfm_artists_data;
create table test_db.lastfm_artists_data (
  artist_name text,
  artist_mbid text,
  artist_url text,
  tags text [],
  dt_published timestamp,
  dt_insert timestamptz default now()
);

select raw.artist_name, raw.artist_mbid,
       raw.artist_url,  raw.tags,
       raw.dt_published
from test_db.lastfm_artists_data_raw raw;

create table test_db.lastfm_artists_data
as
select md5(raw.artist_name || raw.artist_mbid || raw.artist_url) surrogate_key,
       raw.artist_name, raw.artist_mbid,
       raw.artist_url,  raw.tags,
       raw.dt_published, raw.dt_insert
  from test_db.lastfm_artists_data_raw raw;


create unique INDEX lastfm_artists_data_pk ON test_db.lastfm_artists_data (surrogate_key);

create view test_db.v_lastfm_artist_unsaved_rows
as
select md5(raw.artist_name || raw.artist_mbid || raw.artist_url) surrogate_key, 
       raw.artist_name, raw.artist_mbid,
       raw.artist_url,  raw.tags,
       raw.dt_published
from test_db.lastfm_artists_data_raw raw
where 1=1
  and not exists (select 1
                    from test_db.lastfm_artists_data lad
                   where 1=1
                     and lad.surrogate_key = md5(raw.artist_name || raw.artist_mbid || raw.artist_url));

select extract(year from dt_listen) as "year",
       to_char(dt_listen, 'Day') as "day",
       count(1) as "cnt"
  from test_db.lastfm_history_data lhd
  where 1=1
    and extract(hour from dt_listen + '5 hours') < 22
    and extract(hour from dt_listen + '5 hours') > 7
  group by extract(year from dt_listen), to_char(dt_listen, 'Day')
  order by cnt, year, day
;

select *
  from test_db.lastfm_artists_data_raw ladr ;

select * from test_db.lastfm_artists_data lad
where artist_url in (select ladr.artist_url from test_db.lastfm_artists_data_raw ladr);

INSERT INTO test_db.lastfm_artists_data
  (surrogate_key, artist_name, artist_mbid, artist_url, tags, dt_published)
select md5(raw.artist_name || raw.artist_mbid || raw.artist_url) surrogate_key, 
       raw.artist_name, raw.artist_mbid,
       raw.artist_url,  raw.tags,
       raw.dt_published
from test_db.lastfm_artists_data_raw raw
  ON conflict (surrogate_key) DO UPDATE 
 SET tags = EXCLUDED.tags,
     dt_published = EXCLUDED.dt_published,
     dt_insert = EXCLUDED.dt_insert
  ;
  
  
  
