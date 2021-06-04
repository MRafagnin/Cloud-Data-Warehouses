import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("CREATE TABLE IF NOT EXISTS staging_events \
                                    (artist        VARCHAR, \
                                     auth          VARCHAR, \
                                     firstName     VARCHAR, \
                                     gender        VARCHAR, \
                                     ItemInSession INT, \
                                     lastName      VARCHAR, \
                                     length        NUMERIC, \
                                     level         VARCHAR, \
                                     location      VARCHAR, \
                                     method        VARCHAR, \
                                     page          VARCHAR, \
                                     registration  VARCHAR, \
                                     sessionId     INT, \
                                     song          VARCHAR, \
                                     status        INT, \
                                     ts            BIGINT, \
                                     userAgent     VARCHAR, \
                                     userId        INT);")

staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS staging_songs \
                                  (song_id          VARCHAR PRIMARY KEY, \
                                   artist_id        VARCHAR, \
                                   artist_latitude  NUMERIC, \
                                   artist_longitude NUMERIC, \
                                   artist_location  VARCHAR, \
                                   artist_name      VARCHAR, \
                                   duration         NUMERIC, \
                                   num_songs        INT, \
                                   title            VARCHAR, \
                                   year             NUMERIC);")

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplay \
                            (songplay_id INT IDENTITY(0,1) SORTKEY, \
                             start_time  TIMESTAMP NOT NULL, \
                             user_id     INT NOT NULL, \
                             level       VARCHAR, \
                             song_id     VARCHAR NOT NULL, \
                             artist_id   VARCHAR NOT NULL, \
                             session_id  INT NOT NULL, \
                             location    VARCHAR, \
                             user_agent  VARCHAR);")

user_table_create = ("CREATE TABLE IF NOT EXISTS users \
                            (user_id    INT PRIMARY KEY, \
                             first_name VARCHAR NOT NULL, \
                             last_name  VARCHAR NOT NULL, \
                             gender     VARCHAR(1), \
                             level      VARCHAR NOT NULL);")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs \
                            (song_id    VARCHAR PRIMARY KEY, \
                             title      VARCHAR, \
                             artist_id  VARCHAR NOT NULL, \
                             year       VARCHAR, \
                             duration   NUMERIC);")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists \
                            (artist_id VARCHAR PRIMARY KEY, \
                             name      VARCHAR NOT NULL, \
                             location  VARCHAR, \
                             latitude  NUMERIC, \
                             longitude NUMERIC);")

time_table_create = ("CREATE TABLE IF NOT EXISTS time \
                            (start_time TIMESTAMP PRIMARY KEY, \
                             hour       NUMERIC NOT NULL, \
                             day        NUMERIC NOT NULL, \
                             week       NUMERIC NOT NULL, \
                             month      NUMERIC NOT NULL, \
                             year       NUMERIC NOT NULL, \
                             weekday    VARCHAR);")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                            from {} credentials  
                            'aws_iam_role={}'   
                            json {}  
                            compupdate off
                            region 'us-west-2';
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""copy staging_songs from {} credentials \'aws_iam_role={}\' JSON 'auto' truncatecolumns compupdate off region \'us-west-2\';
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplay (start_time, user_id, level, song_id, \
                                                    artist_id, session_id, location, user_agent) \
                                SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time, \
                                    se.userId AS user_id, \
                                    se.level, \
                                    ss.song_id AS song_id, \
                                    ss.artist_id AS artist_id, \
                                    se.sessionId AS session_id, \
                                    se.location AS location, \
                                    se.userAgent AS user_agent  
                                        FROM staging_events se, staging_songs ss \
                                            WHERE se.page = 'NextSong' AND se.song = ss.title \
                                                AND se.userId NOT IN (SELECT DISTINCT sp.user_id FROM songplay sp
                                                                        WHERE sp.user_id = se.userId 
                                                                            AND sp.session_id = se.sessionId);
                        """)

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level) \
                            SELECT DISTINCT userId AS user_id, \
                                            firstName AS first_name, \
                                            lastName AS last_name, \
                                            gender, \
                                            level
                                    FROM staging_events
                                        WHERE page = 'NextSong' AND userId NOT IN (SELECT DISTINCT user_id FROM users)

                        """)

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration) \
                            SELECT DISTINCT \
                            song_id, \
                            title, \
                            artist_id, \
                            year, \
                            duration
                                FROM staging_songs
                                    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
                        """)

artist_table_insert = (""" INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                            SELECT DISTINCT \
                            artist_id, \
                            artist_name AS name, \
                            artist_location AS location, \
                            artist_latitude AS latitude, \
                            artist_longitude AS longitude
                                FROM staging_songs
                                    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
                        """)

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                            SELECT ts AS start_time,
                            EXTRACT(hr FROM ts) AS hour, \
                            EXTRACT(d FROM ts) AS day, \
                            EXTRACT(w FROM ts) AS week, \
                            EXTRACT(m FROM ts) AS month, \
                            EXTRACT(yr FROM ts) AS year, \
                            EXTRACT(weekday FROM ts) AS weekday
                                FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS ts 
                                    FROM staging_events)
                                        WHERE start_time NOT IN (SELECT DISTINCT start_time from time)
                        """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
