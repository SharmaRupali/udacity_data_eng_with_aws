import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (
    """
    CREATE TABLE staging_events
    (
        artist              varchar,
        auth                varchar,
        first_name          varchar,
        gender              varchar,
        item_in_session     integer,
        last_name           varchar,
        length              double precision,
        level               varchar,
        location            varchar,
        method              varchar,
        page                varchar,
        registration        bigint,
        session_id          integer sortkey,
        song                varchar,
        status              integer,
        ts                  bigint,
        user_agent          varchar,
        user_id             varchar distkey
    )
    """
)


staging_songs_table_create = (
    """
    CREATE TABLE staging_songs
    (
        num_songs           integer,
        artist_id           varchar distkey,
        artist_latitude     double precision,
        artist_longitude    double precision,
        artist_location     varchar(max),
        artist_name         varchar(max),
        song_id             varchar sortkey, ??
        title               varchar(max),
        duration            double precision,
        year                integer
    )
    """
)

songplay_table_create = (
    """
    CREATE TABLE songplays 
    (
        songplay_id         bigint identity(1,1) primary key sortkey, 
        start_time          timestamp, 
        user_id             integer distkey, 
        level               varchar,
        song_id             varchar,
        artist_id           varchar, 
        session_id          integer,
        location            varchar,
        user_agent          varchar
    )
    """
)

user_table_create = (
    """
    CREATE TABLE users 
    (
        user_id             varchar primary key sortkey, 
        first_name          varchar, 
        last_name           varchar, 
        gender              varchar, 
        level               varchar
    )
    diststyle all;
    """
)

song_table_create = (
    """
    CREATE TABLE songs 
    (
        song_id             varchar primary key sortkey, 
        title               varchar(max), 
        artist_id           varchar,
        year                integer,
        duration            double precision
    )
    diststyle all;
    """
)


artist_table_create = (
    """
    CREATE TABLE artists 
    (
        artist_id           varchar primary key sortkey, 
        name                varchar(max), 
        location            varchar(max),
        latitude            double precision,
        longitude           double precision
    )
    diststyle all;
    """
)

time_table_create = (
    """
    CREATE TABLE time 
    (
        start_time          timestamp primary key sortkey, 
        hour                integer,  
        day                 integer,  
        week                integer,  
        month               integer, 
        year                integer, 
        weekday             integer
    )
    diststyle all;
    """
)

# STAGING TABLES

staging_events_copy = (
    """
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    json {} region '{}';
    """
).format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH'),
    config.get('CLUSTER_CONFIG', 'REGION')
)

staging_songs_copy = (
    """
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto' region '{}';
    """
).format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('CLUSTER_CONFIG', 'REGION')
)

# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO songplays
        SELECT 
            TIMESTAMP 'epoch' + (events.ts/1000 * INTERVAL '1 second'),
            events.user_id,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.session_id,
            events.location,
            events.user_agent
        FROM staging_events events
            LEFT JOIN staging_songs songs 
                ON events.song = songs.title AND events.artist = songs.artist_name
        WHERE events.page = 'NextSong'
    """
)

user_table_insert = (
    """
    INSERT INTO users
        SELECT DISTINCT
            user_id,
            first_name,
            last_name,
            gender,
            level  
        FROM staging_events;
    """
)

song_table_insert = (
    """
    INSERT INTO songs
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs;
    """
)

artist_table_insert = (
    """
    INSERT INTO artists
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs;
    """
)

time_table_insert = (
    """
    INSERT INTO TIME
        WITH tmp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events)
        SELECT DISTINCT
            ts,
            extract(hour from ts),
            extract(day from ts),
            extract(week from ts),
            extract(month from ts),
            extract(year from ts),
            extract(weekday from ts), 
        FROM tmp_time;
    """
)

# QUERY LISTS

create_table_queries = [
    staging_events_table_create, 
    staging_songs_table_create, 
    songplay_table_create, 
    user_table_create, 
    song_table_create, 
    artist_table_create, 
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop, 
    staging_songs_table_drop, 
    songplay_table_drop, 
    user_table_drop, 
    song_table_drop, 
    artist_table_drop, 
    time_table_drop
]

copy_table_queries = [
    staging_events_copy, 
    staging_songs_copy
]

insert_table_queries = [
    songplay_table_insert, 
    user_table_insert, 
    song_table_insert, 
    artist_table_insert, 
    time_table_insert
]
