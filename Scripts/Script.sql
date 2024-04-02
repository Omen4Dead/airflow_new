-- Новый скрипт в postgres.
-- Дата: 30 мар. 2024 г.
-- Время: 00:49:53
create table public.lastfm_raw as
select '3cce738b-7c61-4c84-a770-b4b6b5ee6233' artist_mbid,
       'fdgdfhfdhdfhrhddgdgrdgdrgrdgrdgdrgrg'artist_name,
       '3cce738b-7c61-4c84-a770-b4b6b5ee6233' mbid,
  '3cce738b-7c61-4c84-a770-b4b6b5ee6233' album_mbid,
  '3cce738b-7c61-4c84-a770-b4b6b5ee6233' album_name,
  '3cce738b-7c61-4c84-a770-b4b6b5ee6233' song_name,
  '3cce738b-7c61-4c84-a770-b4b6b5ee6233' song_url,
  '3cce738b-7c61-4c84-a770-b4b6b5ee6233' dt_listen
;

truncate table public.lastfm_raw ;

select * 
from public.lastfm_raw lr ;

create table test_db.lastfm_raw
(
  artist_mbid varchar(1000),
  artist_name varchar(1000),
  streamable varchar(1000),
  mbid varchar(1000),
  album_mbid varchar(1000),
  album_name varchar(1000),
  song_name varchar(1000),
  song_url varchar(1000),
  dt_listen varchar(1000),
  image_url varchar(1000)
);