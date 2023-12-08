class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                songs.song_id,
                songs.artist_id,
                events.sessionid, 
                events.level,
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)


create_table_sql = (
    """
    BEGIN;
    DROP TABLE IF EXISTS staging_events;
    DROP TABLE IF EXISTS staging_songs;
    DROP TABLE IF EXISTS songplays;
    DROP TABLE IF EXISTS users;
    DROP TABLE IF EXISTS song;
    DROP TABLE IF EXISTS artist;
    DROP TABLE IF EXISTS time;

    CREATE TABLE staging_events(
        artist TEXT NULL,
        auth   VARCHAR NULL,
        firstName VARCHAR  NULL,
        gender VARCHAR NULL,
        itemInsession INTEGER  NULL,
        lastName VARCHAR  NULL,
        length FLOAT  NULL,
        level VARCHAR NULL,
        location text NULL,
        method VARCHAR NULL,
        page VARCHAR NULL,
        registration FLOAT  NULL,
        sessionId INTEGER  sortkey distkey,
        song TEXT NULL,
        status INTEGER NULL,
        ts BIGINT NOT NULL,
        userAgent TEXT NULL,
        userId INTEGER NULL
    );

    CREATE TABLE staging_songs(
        num_songs INTEGER  NULL,
        artist_id VARCHAR NOT NULL  distkey,
        artist_latitude VARCHAR,
        artist_longitude VARCHAR,
        artist_location TEXT,
        artist_name VARCHAR,
        song_id VARCHAR NOT NULL sortkey,
        title  text  NULL,
        duration FLOAT  NULL,
        year INTEGER NULL 
    );

CREATE TABLE songplays (
    songplay_id VARCHAR NOT NULL ,
    start_time TIMESTAMP NOT NULL distkey,
    user_id  INTEGER NOT NULL,
    song_id  VARCHAR NOT NULL sortkey,
    artist_id  VARCHAR NOT NULL,
    session_id  INTEGER NOT NULL,
    level  VARCHAR NOT NULL,
    location   text NULL,
    user_agent text NULL
  );

CREATE TABLE users(
      user_id INTEGER PRIMARY KEY NOT NULL sortkey,
      firstname  VARCHAR  NULL,
      lastname VARCHAR  NULL,
      gender VARCHAR,
      level VARCHAR NULL
  ) diststyle all;

   CREATE TABLE song(
       song_id  VARCHAR PRIMARY KEY NOT NULL,
       title  text NOT NULL,
       artist_id  VARCHAR NOT NULL sortkey,
       year  INTEGER NOT NULL,
       duration FLOAT NOT NULL
   ) diststyle all;

   CREATE TABLE artist(
       artist_id  VARCHAR PRIMARY KEY NOT NULL sortkey,
       artist_name  TEXT,
       artist_location  TEXT,
       artist_longitude DECIMAL(9),
       artist_latitude DECIMAL(9)
   ) diststyle all;

CREATE TABLE time(
    start_time TIMESTAMP NOT NULL,
    hour SMALLINT NOT NULL,
    day  SMALLINT NOT NULL,
    week  SMALLINT NOT NULL,
    month   SMALLINT NOT NULL,
    year   SMALLINT NOT NULL,
    weekday SMALLINT NOT NULL
 ) diststyle all;

COMMIT;
"""
)
