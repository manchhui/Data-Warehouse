import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN= config.get("IAM_ROLE","ARN")


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_logs;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_logs (artist varchar,
                                            auth varchar,
                                            firstName varchar,
                                            gender varchar,
                                            itemInSession varchar,
                                            lastName varchar,
                                            length float,
                                            level varchar,
                                            location varchar,
                                            method varchar,
                                            page varchar,
                                            registration float,
                                            sessionId int,
                                            song varchar,
                                            status varchar,
                                            ts bigint,
                                            userAgent varchar,
                                            userId int);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (artist_id varchar,
                                            artist_latitude float,
                                            artist_location varchar,
                                            artist_longitude float,
                                            artist_name varchar,
                                            duration float,
                                            num_songs int,
                                            song_id varchar,
                                            title varchar,
                                            year int);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id bigint SORTKEY, 
                                        start_time timestamp NOT NULL,
                                        user_id int NOT NULL,
                                        level varchar(4) NOT NULL,
                                        song_id varchar,
                                        artist_id varchar,
                                        session_id int NOT NULL,
                                        location varchar NOT NULL,
                                        user_agent varchar NOT NULL,
                                        PRIMARY KEY (songplay_id),
                                        FOREIGN KEY (song_id) REFERENCES songs (song_id),
                                        FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
                                        FOREIGN KEY (start_time) REFERENCES time (start_time),
                                        FOREIGN KEY (user_id) REFERENCES users (user_id),
                                        UNIQUE (start_time, user_id, session_id))
                                        DISTSTYLE ALL;
                                        
""")                                                 

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id int SORTKEY,
                                    first_name varchar,
                                    last_name varchar,
                                    gender varchar(1),
                                    level varchar(4),
                                    PRIMARY KEY (user_id))
                                    DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id varchar SORTKEY,
                                    title varchar,
                                    artist_id varchar,
                                    year int,
                                    duration float,
                                    PRIMARY KEY (song_id))
                                    DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id varchar SORTKEY,
                                    name varchar,
                                    location varchar,
                                    latitude float,
                                    longitude float,
                                    PRIMARY KEY (artist_id))
                                    DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time timestamp SORTKEY,
                                    hour int,
                                    day int,
                                    week int,
                                    month int,
                                    year int,
                                    weekday int,
                                    PRIMARY KEY (start_time))
                                    DISTSTYLE ALL;
""")


# STAGING TABLES
staging_events_copy = ("""
copy staging_logs 
from 's3://udacity-dend/log_data' 
credentials 'aws_iam_role={}'
format as json 's3://udacity-dend/log_json_path.json'
compupdate off
emptyasnull
blanksasnull
region 'us-west-2';
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
copy staging_songs 
from 's3://udacity-dend/song_data' 
credentials 'aws_iam_role={}'
format as json 'auto'
compupdate off
emptyasnull
blanksasnull
region 'us-west-2';
""").format(DWH_ROLE_ARN)


# FINAL TABLES

# Staging table technique used as replacement for ON CONFLICT which is not available on REDSHIFT.
songplay_table_insert = ("""
CREATE TEMP TABLE songplays_staging (songplay_id bigint NOT NULL, 
                                        start_time timestamp NOT NULL,
                                        user_id int NOT NULL,
                                        level varchar(4) NOT NULL,
                                        song_id varchar,
                                        artist_id varchar,
                                        session_id int NOT NULL,
                                        location varchar NOT NULL,
                                        user_agent varchar NOT NULL,
                                        song varchar,
                                        artist varchar,
                                        length float,
                                        ts bigint,
                                        concat_id varchar NOT NULL);

CREATE TEMP TABLE songplays_staging1 (song_id varchar NOT NULL,
                                        song varchar NOT NULL,
                                        artist_id varchar NOT NULL,
                                        artist varchar NOT NULL,
                                        length float NOT NULL,
                                        concat_id varchar NOT NULL);
                                        
INSERT INTO songplays_staging
SELECT DISTINCT
    1, 
    DATEADD(ms, ts,'1970-1-1') AS start_time, 
    userid AS user_id, 
    level, 
    'None', 
    'None', 
    sessionid AS session_id, 
    location, 
    useragent AS user_agent,
    song,
    artist,
    length,
    ts,
    'None'
FROM staging_logs
WHERE page = 'NextSong';

UPDATE songplays_staging
SET concat_id = song || artist || CAST(length AS text)
WHERE (song || artist || CAST(length AS text)) IS NOT NULL;

INSERT INTO songplays_staging1
SELECT 
    s.song_id, 
    s.title AS song, 
    a.artist_id, 
    a.name AS artist, 
    s.duration AS length,
    (SELECT song || artist || CAST(length AS text)) AS concat_id
FROM songs s 
JOIN artists a ON s.artist_id = a.artist_id;

UPDATE songplays_staging
SET song_id = st1.song_id,
    artist_id = st1.artist_id
FROM songplays_staging st
    JOIN songplays_staging1 st1
        ON st.concat_id = st1.concat_id;
        
INSERT INTO songplays 
SELECT CAST((ROW_NUMBER() OVER (ORDER BY st.ts) || st.ts) AS bigint) AS songplay_id, 
    st.start_time, 
    st.user_id, 
    st.level, 
    st.song_id, 
    st.artist_id, 
    st.session_id, 
    st.location, 
    st.user_agent
FROM (SELECT st.*,
             ROW_NUMBER() OVER (PARTITION BY ts, 
                                             user_id, 
                                             level, 
                                             song_id, 
                                             artist_id, 
                                             location, 
                                             song, 
                                             artist, 
                                             length,
                                             user_agent ORDER BY ts, 
                                                             user_id, 
                                                             level, 
                                                             song_id, 
                                                             artist_id, 
                                                             location, 
                                                             song, 
                                                             artist, 
                                                             length,
                                                             user_agent DESC) seqnum
      FROM songplays_staging st) st
LEFT JOIN songplays s
ON st.songplay_id = s.songplay_id
WHERE seqnum = 1 AND s.songplay_id IS NULL;

DROP TABLE IF EXISTS songplays_staging;
DROP TABLE IF EXISTS songplays_staging1;
""")

# Staging table technique used as replacement for ON CONFLICT which is not available on REDSHIFT.
user_table_insert = ("""
CREATE TABLE IF NOT EXISTS "users_staging" ("user_id" int NOT NULL,
                                    "first_name" varchar,
                                    "last_name" varchar,
                                    "gender" varchar(1),
                                    "level" varchar(4),
                                    "start_time" bigint,
                                    PRIMARY KEY ("user_id"));

INSERT INTO users_staging 
    SELECT DISTINCT 
        CAST(userId AS int) AS user_id, 
        firstName AS first_name, 
        lastName AS last_name, 
        gender, 
        level,
        ts AS "start_time"
FROM staging_logs
WHERE page = 'NextSong';

INSERT INTO users 
    SELECT st.user_id, st.first_name, st.last_name, st.gender, st.level
FROM (SELECT st.*,
             ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY start_time DESC) seqnum
      FROM users_staging st) st
LEFT JOIN users u
ON st.user_id = u.user_id
WHERE seqnum = 1 AND u.user_id IS NULL;

UPDATE users
SET level = st.level
FROM (SELECT st.*,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY start_time DESC) seqnum
        FROM users_staging st) st
JOIN users u
ON st.user_id = u.user_id
WHERE seqnum = 1 
AND (u.user_id = st.user_id AND u.first_name = st.first_name AND u.last_name = st.last_name AND u.gender = st.gender AND u.level <> st.level);
""")

# Staging table technique used as replacement for ON CONFLICT which is not available on REDSHIFT.
song_table_insert = ("""
CREATE TEMP TABLE songs_staging (LIKE songs);

INSERT INTO songs_staging 
    SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs;

INSERT INTO songs 
    SELECT st.song_id, st.title, st.artist_id, st.year, st.duration
FROM (SELECT st.*,
             ROW_NUMBER() OVER (PARTITION BY song_id, title, artist_id, year, duration ORDER BY song_id, title, artist_id, year, duration DESC) seqnum
      FROM songs_staging st) st
LEFT JOIN songs s
ON st.song_id = s.song_id
WHERE seqnum = 1 AND s.song_id IS NULL;   

DROP TABLE songs_staging;
""")

# Staging table technique used as replacement for ON CONFLICT which is not available on REDSHIFT.
artist_table_insert = ("""
CREATE TEMP TABLE artists_staging (LIKE artists);

INSERT INTO artists_staging 
    SELECT DISTINCT 
        artist_id, 
        artist_name AS name, 
        artist_location AS location, 
        artist_latitude AS latitude, 
        artist_longitude AS longitude
FROM staging_songs;

INSERT INTO artists 
    SELECT st.artist_id, st.name, st.location, st.latitude, st.longitude
FROM (SELECT st.*,
             ROW_NUMBER() OVER (PARTITION BY artist_id, name, location, latitude, longitude ORDER BY artist_id, name, location, latitude, longitude DESC) seqnum
      FROM artists_staging st) st
LEFT JOIN artists a
ON st.artist_id = a.artist_id
WHERE seqnum = 1 AND a.artist_id IS NULL;    

DROP TABLE artists_staging;
""")

# Staging table technique used as replacement for ON CONFLICT which is not available on REDSHIFT.
time_table_insert = ("""
CREATE TEMP TABLE time_staging (LIKE time);

INSERT INTO time_staging 
    SELECT DISTINCT 
        start_time, 
        DATE_PART(hour, start_time) AS hour, 
        DATE_PART(day, start_time) AS day, 
        DATE_PART(week, start_time) AS week, 
        DATE_PART(month, start_time) AS month, 
        DATE_PART(year, start_time) AS year, 
        DATE_PART(weekday, start_time) AS weekday
FROM songplays;

INSERT INTO time 
    SELECT st.start_time, st.hour, st.day, st.week, st.month, st.year, st.weekday
FROM (SELECT st.*,
             ROW_NUMBER() OVER (PARTITION BY start_time ORDER BY start_time DESC) seqnum
      FROM time_staging st) st
LEFT JOIN time t
ON st.start_time = t.start_time
WHERE seqnum = 1 AND t.start_time IS NULL;

DROP TABLE time_staging;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]

