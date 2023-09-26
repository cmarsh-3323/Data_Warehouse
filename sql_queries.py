import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

# drops all tables if they exist
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

#collected from log_data located in S3 for the column names and data_type
staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events
(
    artist VARCHAR(250),
    auth VARCHAR(250),
    firstName VARCHAR(250),
    gender CHAR,
    itemInSession INT,
    lastName VARCHAR(250),
    length FLOAT,
    level VARCHAR(250),
    location VARCHAR(250),
    method VARCHAR(250),
    page VARCHAR(250),
    registration BIGINT,
    sessionId INT,
    song VARCHAR(250),
    status INT,
    ts bigINT,
    userAgent VARCHAR(250),
    userId INT
);    
""")

#collected from song_data located in S3 for the column names and data_type
staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs INT,
    artist_id VARCHAR(250),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(250),
    artist_name VARCHAR(250),
    song_id VARCHAR(250),
    title VARCHAR(250),
    duration FLOAT,
    year INT
);    
""")

# fact table songplays 
songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR(250) NOT NULL,
    level VARCHAR(250),
    song_id VARCHAR(250) NOT NULL,
    artist_id VARCHAR(250) NOT NULL,
    session_id INT,
    location VARCHAR(250),
    user_agent VARCHAR(250)
);    
""")

#dim table users
user_table_create = (""" CREATE TABLE IF NOT EXISTS users
(
    user_id VARCHAR(250) PRIMARY KEY,
    first_name VARCHAR(250),
    last_name VARCHAR(250),
    gender CHAR,
    level VARCHAR(250)
);
""")

#dim table songs
song_table_create = (""" CREATE TABLE IF NOT EXISTS songs
(
    song_id VARCHAR(250) PRIMARY KEY,
    title VARCHAR(250),
    artist_id VARCHAR(250) NOT NULL,
    year INT,
    duration FLOAT
);
""")

#dim table artists
artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR(250) PRIMARY KEY,
    name VARCHAR(250),
    location VARCHAR(250),
    latitude FLOAT,
    longitude FLOAT
);
""")

#dim table time
time_table_create = (""" CREATE TABLE IF NOT EXISTS time
(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);    
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
REGION 'us-west-2'
FORMAT JSON AS {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])
# lesson 12 redshift etl examples, this is my guess so far 
staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
REGION 'us-west-2'
FORMAT JSON AS 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES
# etl data from s3 to facts table and the four dimensional tables
songplay_table_insert = ("""INSERT INTO songplays 
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT(DATEADD(s, e.ts/1000, '19700101')) AS start_time,
e.userId AS user_id,
e.level,
s.song_id,
s.artist_id,
e.sessionId AS session_id,
e.location,
e.userAgent AS user_agent
FROM staging_events AS e
JOIN staging_songs AS s
ON (s.duration = e.length
AND s.title = e.song
AND s.artist_name = e.artist)
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId AS user_id,
firstName AS first_name,
lastName AS last_name,
gender,
level
FROM staging_events
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
title,
artist_id,
year,
duration
FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
artist_name AS name,
artist_location AS location,
artist_latitude as latitude,
artist_longitude AS longitude
FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT timestamp 'epoch' + start_time/1000 * interval '1 second' AS start_time,
EXTRACT(HOUR FROM start_time) AS hour,
EXTRACT(DAY FROM start_time) AS day,
EXTRACT(WEEK FROM start_time) AS week,
EXTRACT(MONTH FROM start_time) AS month,
EXTRACT(YEAR FROM start_time) AS year,
EXTRACT(DOW FROM start_time) AS weekday
FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
