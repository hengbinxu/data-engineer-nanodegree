import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg', encoding='utf-8')

# DROP TABLES
drop_table_stmt_template = "drop table if exists {table_name}"

staging_events_table_drop = drop_table_stmt_template.format(table_name='staging_events')
staging_songs_table_drop = drop_table_stmt_template.format(table_name='staging_songs')
songplay_table_drop = drop_table_stmt_template.format(table_name='song_play')
user_table_drop = drop_table_stmt_template.format(table_name='users')
song_table_drop = drop_table_stmt_template.format(table_name='songs')
artist_table_drop = drop_table_stmt_template.format(table_name='artists')
time_table_drop = drop_table_stmt_template.format(table_name='time')

# FINAL TABLES
# CREATE STAGING TABLES
staging_events_table_create= ("""
    create table if not exists staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender char(1),
        itemInSession int,
        lastName varchar,
        length numeric(20, 5),
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration numeric,
        sessionId varchar,
        song varchar,
        status smallint,
        ts timestamp,
        userAgent varchar,
        userId integer
    );
""")

staging_songs_table_create = ("""
    create table if not exists staging_songs (
        song_id varchar,
        title varchar,
        year int4,
        num_songs smallint,
        artist_id varchar,
        artist_name varchar,
        artist_location varchar,
        artist_latitude numeric(10, 5),
        artist_longitude numeric(10, 5),
        duration numeric(20, 5)
    );
""")

# Dimension Tables
user_table_create = ("""
    create table if not exists users (
        user_id integer primary key sortkey,
        first_name varchar(64),
        last_name varchar(64),
        gender char(1),
        level varchar(16)
    );
""")

song_table_create = ("""
    create table if not exists songs (
        song_id varchar(18) primary key sortkey,
        title varchar(256),
        artist_id varchar(18),
        year smallint,
        duration numeric(20, 5)
    ) diststyle all;
""")

artist_table_create = ("""
    create table if not exists artists (
        artist_id varchar(18) primary key sortkey,
        name varchar(512),
        location varchar(512),
        latitude numeric(10, 5),
        longitude numeric(10, 5)
    ) diststyle all;
""")

time_table_create = ("""
    create table if not exists time (
        start_time timestamp primary key sortkey,
        hour smallint not null,
        day smallint not null,
        week smallint not null,
        month smallint not null,
        year smallint not null,
        weekday smallint not null
    ) diststyle all;
""")

# Fact table 
songplay_table_create = ("""
    create table if not exists song_play (
        song_play_id bigint identity(0, 1) primary key,
        start_time timestamp not null references time(start_time) sortkey,
        user_id integer not null references users(user_id) distkey,
        level varchar(16),
        song_id varchar(18) not null references songs(song_id),
        artist_id varchar(18) not null references artists(artist_id),
        session_id integer,
        location varchar(256),
        user_agent varchar(512)
    );
""")

# Copy statement
staging_events_copy = (
    """
    copy staging_events
    from {source_files}
    credentials 'aws_iam_role={role_arn}'
    format as json {log_json_path}
    region '{region_name}'
    timeformat 'epochmillisecs'
    ;
    """
).format(
    source_files=config.get('S3', 'LOG_DATA'),
    role_arn=config.get('IAM_ROLE', 'ARN'),
    log_json_path=config.get('S3', 'LOG_JSONPATH'),
    region_name=config.get('CLUSTER', 'REGION_NAME')
)
# TIMEFORMAT 'epochmillisecs'
# Documentation
# https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-timeformat

staging_songs_copy = (
    """
    copy staging_songs
    from {source_files}
    credentials 'aws_iam_role={role_arn}'
    region '{region_name}'
    format as json 'auto'
    ;
    """
).format(
    source_files=config.get('S3', 'SONG_DATA'),
    role_arn=config.get('IAM_ROLE', 'ARN'),
    region_name=config.get('CLUSTER', 'REGION_NAME')
)


# ETL statements
songplay_table_insert = ("""
    insert into song_play (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select distinct se.ts as timestamp,
        se.userId as user_id,
        se.level as level,
        ss.song_id as song_id,
        ss.artist_id as artist_id,
        cast(se.sessionId as integer) as session_id,
        se.location as location,
        se.userAgent as user_agent
    from staging_events se
    join staging_songs ss
    on se.song = ss.title and se.artist = ss.artist_name
    where se.page = 'NextSong';
""")

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    select distinct userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender, 
        level
    from staging_events
    where page='NextSong';
""")

song_table_insert = ("""
    insert into songs(song_id, title, artist_id, year, duration)
    select distinct song_id, title, artist_id, year, duration
    from staging_songs;
""")

artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude)
    select distinct artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    from staging_songs;
""")

time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    select distinct ts as start_time,
        extract(hour from ts) as hour,
        extract(day from ts) as day,
        extract(week from ts) as week,
        extract(month from ts) as month,
        extract(year from ts) as year,
        extract(dow from ts) as weekday
    from staging_events
    where page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]

############
### Note ###
############
# ========================================================
# |  If the data type of ts in staging table is bigint,  |
# |  the insert queries need to be changed as following. |
# ========================================================
#
# songplay_table_insert = ("""
#     insert into song_play (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
#     select distinct timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time,
#         se.userId as user_id,
#         se.level as level,
#         ss.song_id as song_id,
#         ss.artist_id as artist_id,
#         se.sessionId as session_id,
#         se.location as location,
#         se.userAgent as user_agent
#     from staging_events se
#     join staging_songs ss
#     on se.song = ss.title and se.artist = ss.artist_name
#     where se.page = 'NextSong';
# """)

# time_table_insert = ("""
#     insert into time (start_time, hour, day, week, month, year, weekday)
#     select distinct timestamp 'epoch' + ts/1000 * interval '1 second' as start_time,
#         extract(hour from (timestamp 'epoch' + ts/1000 * interval '1 second')) as hour,
#         extract(day from (timestamp 'epoch' + ts/1000 * interval '1 second')) as day,
#         extract(week from (timestamp 'epoch' + ts/1000 * interval '1 second')) as week,
#         extract(month from (timestamp 'epoch' + ts/1000 * interval '1 second')) as month,
#         extract(year from (timestamp 'epoch' + ts/1000 * interval '1 second')) as year,
#         extract(dow from (timestamp 'epoch' + ts/1000 * interval '1 second')) as weekday
#     from staging_events
#     where page = 'NextSong';
# """)