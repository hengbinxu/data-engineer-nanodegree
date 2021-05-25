# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS song_plays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# DROP DATA TYPE
level_options_drop = "DROP TYPE IF EXISTS level_options"
gender_options_drop = "DROP TYPE IF EXISTS gender_options"

# Create data type for song_plays.level, users.level and users.gender.
level_options_create = ("""
    CREATE TYPE level_options AS ENUM ('free', 'paid')
""")

gender_options_create = ("""
    CREATE TYPE gender_options AS ENUM ('F', 'M')
""")

# CREATE TABLES
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_plays(
        song_play_id serial primary key,
        start_time timestamp not null,
        user_id int not null,
        level level_options,
        song_id varchar(18),
        artist_id varchar(18),
        session_id int,
        location varchar(256),
        user_agent varchar(256),
        unique(start_time, user_id, session_id)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id int primary key,
        first_name varchar(64),
        last_name varchar(64),
        gender gender_options,
        level level_options
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar(18) primary key,
        title varchar(256),
        artist_id varchar(18),
        year smallint,
        duration numeric(20, 5)
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar(18) primary key,
        name varchar(256),
        location varchar(256),
        latitude decimal(10, 5),
        longitude decimal(10, 5)
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time timestamp primary key,
        hour smallint,
        day smallint,
        week smallint,
        month smallint,
        year smallint,
        weekday smallint
    )
""")

# INSERT RECORDSs

songplay_table_insert = ("""
    insert into song_plays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    values(%s, %s, %s, %s, %s, %s, %s, %s)
    on conflict(start_time, user_id, session_id)
    do update set level=EXCLUDED.level, song_id=EXCLUDED.song_id, artist_id=EXCLUDED.artist_id,
                location=EXCLUDED.location, user_agent=EXCLUDED.user_agent
""")

user_table_insert = ("""
    insert into users(user_id, first_name, last_name, gender, level)
    values(%s, %s, %s, %s, %s)
    on conflict(user_id)
    do update set first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name,
                gender=EXCLUDED.gender, level=EXCLUDED.level
""")

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration)
    values(%s, %s, %s, %s, %s)
    on conflict(song_id)
    do update set title=EXCLUDED.title, artist_id=EXCLUDED.artist_id,
                year=EXCLUDED.year, duration=EXCLUDED.duration
""")

artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude)
    values(%s, %s, %s, %s, %s)
    on conflict(artist_id)
    do update set name=EXCLUDED.name, location=EXCLUDED.location, latitude=EXCLUDED.latitude,
                longitude=EXCLUDED.longitude
""")


time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    values(%s, %s, %s, %s, %s, %s, %s)
    on conflict(start_time)
    do update set hour=EXCLUDED.hour, day=EXCLUDED.day, week=EXCLUDED.week,
                month=EXCLUDED.month, year=EXCLUDED.year, weekday=EXCLUDED.weekday
""")

# FIND SONGS

song_select = ("""
    select s.song_id, s.artist_id
    from songs s
    left join artists a
    on s.artist_id = a.artist_id
    where s.title = %s and a.name = %s and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [
    level_options_create, gender_options_create,
    songplay_table_create, user_table_create,
    song_table_create, artist_table_create,
    time_table_create,
]
drop_table_queries = [
    songplay_table_drop, user_table_drop,
    song_table_drop, artist_table_drop,
    time_table_drop, level_options_drop,
    gender_options_drop
]