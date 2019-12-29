import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3","LOG_DATA")
ARN = config.get("IAM_ROLE","ARN")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
                event_id      BIGINT IDENTITY(0,1) NOT NULL,
                artist        VARCHAR,
                auth          VARCHAR,
                firstName     VARCHAR,
                gender        VARCHAR,
                itemInSession BIGINT,
                lastName      VARCHAR,
                length        FLOAT,
                level         VARCHAR,
                location      VARCHAR,
                method        VARCHAR,
                page          VARCHAR,
                registration  FLOAT,
                sessionId     BIGINT,
                song          VARCHAR,
                status        INT,
                ts            BIGINT,
                userAgent     VARCHAR,
                userId        INT
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (num_songs          INT     NOT NULL,
                                          artist_id          VARCHAR NOT NULL, 
                                          artist_latitude    FLOAT,
                                          artist_longitude   FLOAT, 
                                          artist_location    VARCHAR,
                                          artist_name        VARCHAR,
                                          song_id            VARCHAR,
                                          title              VARCHAR,
                                          duration           FLOAT,
                                          year               INT
                                          );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (songplay_id     INT IDENTITY(0,1) PRIMARY KEY,
                                     start_time      TIMESTAMP         DISTKEY SORTKEY, 
                                     user_id         INT, 
                                     level           VARCHAR, 
                                     song_id         VARCHAR, 
                                     artist_id       VARCHAR, 
                                     session_id      VARCHAR, 
                                     location        VARCHAR, 
                                     user_agent      VARCHAR);
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id         INT PRIMARY KEY SORTKEY, 
                                  first_name      VARCHAR, 
                                  last_name       VARCHAR, 
                                  gender          VARCHAR, 
                                  level           VARCHAR);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id        VARCHAR PRIMARY KEY SORTKEY, 
                                  title          VARCHAR, 
                                  artist_id      VARCHAR, 
                                  year           INT, 
                                  duration       FLOAT);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id       VARCHAR PRIMARY KEY SORTKEY, 
                                    name            VARCHAR, 
                                    location        VARCHAR, 
                                    lattitude       FLOAT, 
                                    longitude       FLOAT);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time   TIMESTAMP PRIMARY KEY DISTKEY SORTKEY, 
                                 hour         INT, 
                                 day          INT, 
                                 week         INT, 
                                 month        INT, 
                                 year         INT, 
                                 weekday      INT);
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    iam_role {}
    format as json {}
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)


staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    iam_role {} 
    region 'us-west-2'
    format as json 'auto'
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time,
                          user_id,
                          level,
                          song_id,
                          artist_id,
                          session_id,
                          location,
                          user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000* INTERVAL '1 Second' AS start_time, 
            se.userid as user_id, 
            se.level AS level, 
            ss.song_id AS song_id, 
            ss.artist_id AS artist_id, 
            se.sessionid AS session_id, 
            se.location AS location, 
            se.userAgent AS user_agent
     FROM staging_events se
     JOIN staging_songs  ss
     ON (ss.title = se.song AND ss.artist_name = se.artist)
     WHERE  se.page = 'NextSong';
""")


user_table_insert = ("""
    INSERT INTO users (user_id,
                       first_name,
                       last_name,
                       gender,
                       level)
    SELECT  DISTINCT userid AS user_id, 
            firstName AS first_name, 
            lastName AS last_name, 
            gender, 
            level
    FROM staging_events
    WHERE staging_events.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id,
                       title,
                       artist_id,
                       year,
                       duration)
    SELECT song_id, 
           title, 
           artist_id, 
           year, 
           duration
    FROM staging_songs;
""")


artist_table_insert = ("""
    INSERT INTO artists (artist_id,
                         name,
                         location,
                         lattitude,
                         longitude)
    SELECT  artist_id, 
            artist_name AS name, 
            artist_location AS location, 
            artist_latitude AS lattitude, 
            artist_longitude AS longitude
    FROM staging_songs;
""")


time_table_insert = ("""
    INSERT INTO time (start_time,
                      hour,
                      day,
                      week,
                      month,
                      year,
                      weekday)
    SELECT  start_time,
            date_part(hour, start_time)                  AS hour,
            date_part(day, start_time)                   AS day,
            date_part(week, start_time)                  AS week,
            date_part(month, start_time)                 AS month,
            date_part(year, start_time)                  AS year,
            CAST(date_part(weekday, start_time) AS INT)  AS weekday 
    FROM songplay;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy] #staging_events_copy, 
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
