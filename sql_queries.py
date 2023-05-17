import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA  = config.get("S3","LOG_DATA")
LOG_PATH  = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE  = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
      artist        VARCHAR,
      auth          VARCHAR,
      firstName     VARCHAR,
      gender        CHAR,
      itemInSession INTEGER,
      lastName      VARCHAR,
      length        FLOAT,
      level         VARCHAR,
      location      VARCHAR,
      method        VARCHAR,
      page          VARCHAR,
      registration  VARCHAR,
      sessionId     INTEGER,
      song          VARCHAR,
      status        INTEGER,
      ts            BIGINT, 
      userAgent     VARCHAR, 
      userId        INTEGER
      )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
       num_songs         INTEGER,
       artist_id         VARCHAR,
       artist_latitude   VARCHAR,
       artist_longitude  VARCHAR,
       artist_location   VARCHAR,
       artist_name       VARCHAR,
       song_id           VARCHAR,
       title             VARCHAR,
       duration          FLOAT,
       year              INTEGER
        )
""")

songplay_table_create = ("""

CREATE TABLE IF NOT EXISTS songplay(
      songplay_id   INTEGER identity(0,1) PRIMARY KEY,
      start_time    TIMESTAMP NOT NULL sortkey,
      user_id       INTEGER NOT NULL,
      level         VARCHAR,
      song_id       VARCHAR NOT NULL,
      artist_id     VARCHAR NOT NULL,
      session_id    INTEGER,
      location      VARCHAR,
      user_agent    VARCHAR)     
""")

user_table_create = ("""

CREATE TABLE IF NOT EXISTS users(
      user_id       VARCHAR PRIMARY KEY,
      first_name    VARCHAR,
      last_name     VARCHAR,
      gender        VARCHAR,
      level         VARCHAR)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
       song_id     VARCHAR PRIMARY KEY,
       title       VARCHAR NOT NULL,
       artist_id   VARCHAR NOT NULL,
       year        INTEGER,
       duration    FLOAT)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
       artist_id  VARCHAR PRIMARY KEY,
       name       VARCHAR NOT NULL,
       location   VARCHAR,
       latitude   VARCHAR,
       longitude  VARCHAR)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
       start_time TIMESTAMP sortkey PRIMARY KEY,
       hour       INTEGER NOT NULL,
       day        INTEGER NOT NULL,
       week       INTEGER NOT NULL,
       month      INTEGER NOT NULL,
       year       INTEGER NOT NULL,
       weekday    INTEGER NOT NULL)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
     iam_role      {}
     region       'us-west-2'
     format        as JSON {}
""").format(LOG_DATA,IAM_ROLE,LOG_PATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
     iam_role     {}
     region       'us-west-2'
     format as json 'auto'
""").format(SONG_DATA,IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time, 
                  e.userId AS user_id,
                  e.level,
                  s.song_id,
                  s.artist_id,
                  e.sessionId AS session_id,
                  e.location,
                  e.userAgent AS user_agent
FROM staging_events e
JOIN staging_songs s 
ON  e.artist = s.artist_name
AND e.song = s.title
AND e.length = s.duration
WHERE e.page = 'NextSong'

""")

user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level)
SELECT DISTINCT   userId AS user_id,
                  firstName AS first_name,
                  lastName AS last_name,
                  gender,
                  level
                  FROM staging_events
WHERE user_id IS NOT NULL
AND page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs(song_id,title,artist_id,year,duration)
SELECT DISTINCT  song_id,
                 title,
                 artist_id,
                 year,
                 duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,latitude,longitude)
SELECT DISTINCT artist_id,
                  artist_name AS name,
                  artist_location AS location,
                  artist_latitude AS latitude,
                  artist_longitude AS longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT   TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time,
                  EXTRACT(hour from start_time) AS hour,
                  EXTRACT(day from start_time) AS day,
                  EXTRACT(week from start_time) AS week,
                  EXTRACT(month from start_time) AS month,
                  EXTRACT(year from start_time) AS year,
                  EXTRACT(weekday from start_time) AS weekday
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
