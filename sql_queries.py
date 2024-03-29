# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
#time modified not null removed
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY, start_time timestamp NOT NULL, user_id int NOT NULL, level varchar NOT NULL, song_id varchar, artist_id varchar, session_id int NOT NULL, location varchar NOT NULL, user_agent varchar NOT NULL);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY NOT NULL, first_name varchar, last_name varchar, gender CHAR(1), level varchar );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY NOT NULL, title varchar, artist_id varchar NOT NULL, year int, duration numeric);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY NOT NULL, name varchar, location varchar, latitude numeric, longitude numeric);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int);

""")

# INSERT RECORDS

songplay_table_insert = ("""
            INSERT INTO songplays (song_id, start_time, user_id, level, artist_id, session_id, location, user_agent) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);

            """)

user_table_insert = ("""
            INSERT INTO users (user_id, first_name, last_name, gender, level) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id) 
            DO UPDATE 
            SET level = EXCLUDED.level;
            """)

song_table_insert = ("""
            INSERT INTO songs (song_id, title, artist_id, year, duration) 
            VALUES (%s, %s, %s, %s, %s) 
            ON CONFLICT (song_id) 
            DO NOTHING;
            """)

artist_table_insert = ("""
            INSERT INTO artists (artist_id, name, location, latitude, longitude) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (artist_id) 
            DO NOTHING;
            """)


time_table_insert = (""" 
            INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (start_time) 
            DO NOTHING;
            """)

# FIND SONGS

song_select = (""" 
            SELECT songs.song_id, artists.artist_id FROM songs
            LEFT JOIN artists
            ON artists.artist_id = songs.artist_id
            WHERE title = (%s) AND name = (%s) AND ROUND(duration,2)=ROUND((%s),2);
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
