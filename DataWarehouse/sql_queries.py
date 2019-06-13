#***********************************************************************************
# Importing Libraries
#***********************************************************************************

import configparser

#***********************************************************************************
# CONFIG (Creating and configuring configparser)
#***********************************************************************************

config = configparser.ConfigParser()
config.read('dwh.cfg')

#***********************************************************************************
# DROP TABLES (Dropping tables)
#***********************************************************************************

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

#***********************************************************************************
# CREATE TABLES (Creating tables)
#***********************************************************************************

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
    artist_name VARCHAR(512),
    auth VARCHAR(16),
    user_first_name VARCHAR(256),
    user_gender VARCHAR(2),
    item_in_session INTEGER,
    user_last_name VARCHAR(256),
    song_length FLOAT,
    user_level VARCHAR(8),
    location VARCHAR(512),
    method VARCHAR(4),
    page VARCHAR(16),
    registration FLOAT,
    session_id INTEGER,
    song_name VARCHAR(512),
    status INTEGER,
    ts VARCHAR(50),
    user_agent TEXT,
    user_id INTEGER,
    PRIMARY KEY (session_id))
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
    song_id VARCHAR(256) PRIMARY KEY,
    title VARCHAR(512),
    duration FLOAT,
    year INTEGER,
    num_songs INTEGER,
    artist_id VARCHAR(256),
    artist_name VARCHAR(512),
    artist_location VARCHAR(512),
    artist_longitude FLOAT,
    artist_latitude FLOAT)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP REFERENCES time(start_time),
    user_id INTEGER REFERENCES users(user_id),
    level VARCHAR(8),
    song_id VARCHAR(256) REFERENCES songs(song_id),
    artist_id VARCHAR(256) REFERENCES artists(artist_id),
    session_id INTEGER,
    location VARCHAR(512),
    user_agent TEXT)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER UNIQUE PRIMARY KEY,
    first_name VARCHAR(256),
    last_name VARCHAR(256),
    gender VARCHAR(2),
    level VARCHAR(8))
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(256) PRIMARY KEY,
    title VARCHAR(512),
    artist_id VARCHAR(256),
    year INTEGER,
    duration FLOAT)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(256) PRIMARY KEY,
    name VARCHAR(256),
    location VARCHAR(512),
    latitude FLOAT,
    longitude FLOAT)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER)
""")

#***********************************************************************************
# STAGING TABLES (Creating staging tables)
#***********************************************************************************

staging_events_copy = ("""
    COPY staging_events FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON '{}'
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON 'auto'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

#***********************************************************************************
# FINAL TABLES (Loading data to the created tables)
#***********************************************************************************

songplay_table_insert = ("""INSERT INTO songplays (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent) 
    SELECT DISTINCT
        TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time, 
        e.user_id, 
        e.user_level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM staging_events e, staging_songs s
    WHERE e.page = 'NextSong'
        AND e.song_name = s.title
        AND e.artist_name = s.artist_name
        AND e.user_id NOT IN (SELECT DISTINCT user_id FROM songplays)
""")

user_table_insert = ("""INSERT INTO users (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level)  
SELECT DISTINCT e.user_id,
       e.user_first_name,
       e.user_last_name,
       e.user_gender,
       e.user_level
FROM staging_events e
    WHERE e.page = 'NextSong' AND e.user_id IS NOT NULL AND e.user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration)
SELECT DISTINCT s.song_id,
    s.title,
    s.artist_id,
    s.year,
    s.duration
FROM staging_songs s
    WHERE s.song_id NOT IN (SELECT song_id FROM songs)
""")

artist_table_insert = ("""INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude) 
SELECT DISTINCT s.artist_id,
    s.artist_name,
    s.artist_location,
    s.artist_latitude,
    s.artist_longitude
FROM staging_songs s
    WHERE s.artist_id NOT IN (SELECT artist_id FROM artists)
""")

time_table_insert = ("""INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
SELECT start_time,
    EXTRACT(HOUR from start_time) AS hour,
    EXTRACT(DAY from start_time) AS day,
    EXTRACT(WEEK from start_time) AS week,
    EXTRACT(MONTH from start_time) AS month,
    EXTRACT(YEAR from start_time) AS year, 
    EXTRACT(WEEKDAY from start_time) AS weekday 
    FROM (SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time 
          FROM staging_events e)
""")

#***********************************************************************************
# QUERY LISTS
#***********************************************************************************

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

#***********************************************************************************
# FIN
#***********************************************************************************