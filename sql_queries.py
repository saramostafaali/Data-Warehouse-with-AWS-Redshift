import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES used in copy command
#LOG_DATA = config.get("S3", "LOG_DATA")
#LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
#SONG_DATA = config.get("S3", "SONG_DATA")
#IAM_ROLE = config.get("IAM_ROLE", "ARN")

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
CREATE TABLE IF NOT EXISTS staging_events (
    event_id          int              IDENTITY(0, 1)  NOT NULL,
    artist            varchar(256), 
    auth              varchar(256), 
    firstName         varchar(256), 
    gender            text, 
    itemInSession     int, 
    lastName          varchar(256), 
    length            float, 
    level             varchar(256), 
    location          varchar(256), 
    method            varchar(256), 
    page              varchar(256)       SORTKEY, 
    registration      bigint, 
    sessionId         int, 
    song              varchar(256)      DISTKEY, 
    status            int, 
    ts                bigint, 
    userAgent         varchar(256), 
    userId            int)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs ( 
    num_songs         int,
    artist_id         varchar(256),
    artist_latitude   DECIMAL(10, 8), 
    artist_longitude  DECIMAL(11, 8), 
    artist_location   varchar(256), 
    artist_name       varchar(256), 
    song_id           varchar(256), 
    title             varchar(256)      DISTKEY,
    duration          float, 
    year              int)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id       int               IDENTITY(0,1)   PRIMARY KEY   DISTKEY, 
    start_time        timestamp         NOT NULL        SORTKEY , 
    user_id           int               NOT NULL, 
    level             text, 
    song_id           varchar(256)       NOT NULL, 
    artist_id         varchar(256)       NOT NULL, 
    session_id        int               NOT NULL, 
    location          varchar(256),
    user_agent        varchar(256)) 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id           int               PRIMARY KEY,
    first_name        varchar(256)       NOT NULL,
    last_name         varchar(256),
    gender            text,
    level             text)
DISTSTYLE ALL
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id           varchar(256)       PRIMARY KEY     SORTKEY DISTKEY, 
    title             varchar(256)      NOT NULL, 
    artist_id         varchar(256)       NOT NULL, 
    year              int,               
    duration          float)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id         varchar(256)        PRIMARY KEY,
    name              varchar(256)        NOT NULL, 
    location          varchar(256), 
    latitude          DECIMAL(10, 8), 
    longitude         DECIMAL(11, 8))
DISTSTYLE ALL
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time        timestamp          PRIMARY KEY    SORTKEY, 
    hour              int                NOT NULL, 
    day               int                NOT NULL, 
    week              int                NOT NULL, 
    month             int                NOT NULL, 
    year              int                NOT NULL, 
    weekday           int                NOT NULL) 
DISTSTYLE ALL
""")


# STAGING TABLES
staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          TIMEFORMAT 'epochmillisecs'
                          json {};
                       """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          json 'auto';
                      """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts/1000) * INTERVAL '1 second' AS start_time,
                    se.userId AS user_id,
                    se.level,
                    ss.song_id AS song_id,
                    ss.artist_id AS artist_id,
                    se.sessionId AS session_id,
                    se.location,
                    se.userAgent AS user_agent
    FROM staging_events se
    LEFT JOIN staging_songs ss 
        ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
    WHERE se.page='NextSong'
        AND ss.song_id IS NOT NULL
""")


#ensure no duplicates were added and in case a user upgrades their level from free to paid
#use DISTINCT and add another condition to ensure there is no duplicate data 
#as there is no upsert statements including ON CONFLICT, And It is not enough to only use SELECT DISTINCT.

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId AS user_id,
                    firstName AS first_name,
                    lastName AS last_name,
                    gender,
                    level
    FROM staging_events
    WHERE page = 'NextSong'
        AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
                    title,
                    artist_id,
                    year,
                    duration
    FROM staging_songs  
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
                    artist_name AS name, 
                    artist_location AS location, 
                    artist_latitude AS latitude, 
                    artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
                    EXTRACT(HOUR FROM start_time) AS hour,
                    EXTRACT(DAY FROM start_time) AS day,
                    EXTRACT(WEEKS FROM start_time) AS week,
                    EXTRACT(MONTH FROM start_time) AS month,
                    EXTRACT(YEAR FROM start_time) AS year,
                    EXTRACT(WEEKDAY FROM start_time)
    FROM staging_events
    WHERE ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
