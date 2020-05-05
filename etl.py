import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def indexing(df,lst):
    """
    I'm using indexing function just to reduce typing time.
    This function accepts a df and a list of columns indices.
    It will create a list of values of the df columns selected by corresponding indices.
    """
    l = []
    for i in lst:
        l.append(df.columns[i])
    return df[l].values.tolist()[0]
####

def process_song_file(cur, filepath):
    """This function reads json file and then inserts selected columns in the tables songs and artists."""
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """This function reads json file and then inserts selected columns in the tables time, users and songplays."""
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    is_nextsong = df["page"]=='NextSong'
    df = df[is_nextsong]

    # convert timestamp column to datetime
    t =  pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    list_of_time = t
    hour         = list_of_time.dt.hour.tolist()
    day          = list_of_time.dt.day.tolist()
    week_of_year = list_of_time.dt.week.tolist()
    month        = list_of_time.dt.month.tolist()
    year         = list_of_time.dt.year.tolist()
    weekday      = list_of_time.dt.weekday.tolist()
    
    time_data = (t.tolist(), hour, day, week_of_year, month, year, weekday)
    column_labels = ('timestamp','hour', 'day', 'week_of_year', 'month', 'year', 'weekday')
    time_data_df = {}
    for i in range(len(column_labels)):
        time_data_df.update({column_labels[i]:time_data[i]})
    time_df = pd.DataFrame(time_data_df)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (songid, t[index], row.userId, row.level, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """This function reads and processes all json files from directory."""
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
