import os
import glob
import psycopg2

import numpy as np
import pandas as pd
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import register_adapter, AsIs
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    df['artist_latitude'] = df[['artist_longitude']].replace({np.nan: None})
    df['artist_longitude'] = df[['artist_longitude']].replace({np.nan: None})
    song_data = df.loc[0, ['song_id', 'title', 'artist_id', 'year', 'duration']].tolist()
    try:
        cur.execute(song_table_insert, song_data)
    except Exception as e:
        print(f'Upsert song data occur error.\n upsert_data: {song_data}')
        raise e
    print(f'Successfully upsert song data. upsert_data: {song_data}')
    
    # insert artist record
    artist_data = df.loc[0, ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].tolist()
    try:
        cur.execute(artist_table_insert, artist_data)
    except Exception as e:
        print(f'Upsert artist data occur error.\n upsert_data: {artist_data}')
        raise e
    print(f'Successfully upsert artist data. upsert_data: {artist_data}')
    print(f'Finish process song_data. file_path: {filepath}')


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (
        t, t.dt.hour, t.dt.day, t.dt.weekofyear,
        t.dt.month, t.dt.year, t.dt.weekday
    )
    column_labels = (
        'timestamp', 'hour', 'day', 'week of year',
        'month', 'year', 'weekday'
    )
    # Concat all information into DataFrame
    time_df = pd.concat(time_data, axis=1, keys=column_labels)

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, row.tolist())
        except Exception as e:
            print(f'Upsert time data occur error.\n upsert_data: {row.tolist()}')
            raise e
    print('Successfully upsert time data.')

    # load user table
    user_df = df.loc[:, ['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row.tolist())
        except Exception as e:
            print(f'Upsert user record occur error.\n upsert_data: {row.tolist()}')
            raise e
    print('Successfully upsert user data.')

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results['song_id'], results['artist_id']
        else:
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, song_id, artist_id,
            row.sessionId, row.location, row.userAgent
        )
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except Exception as e:
            print(f'Upsert song_plays data occur error.\n upsert_data: {songplay_data}')
            raise e
    print('Successfully upsert song_plays data.')
    print(f'Finish process log_data. file_path: {filepath}')


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    # Set psycopg2 adapter, let it allow np.int64 and np.float64.
    register_adapter(np.int64, psycopg2._psycopg.AsIs)
    register_adapter(np.float64, psycopg2._psycopg.AsIs)

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor(cursor_factory=RealDictCursor)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()