import configparser
import os


# CONFIG
AWS_REGION = os.environ.get("UDACITY_AWS_REGION")
config = configparser.ConfigParser()
config.read("dwh.cfg")


# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

staging_events_table_create = """
    create table if not exists staging_events (
        artist text,
        auth text,
        first_name text,
        gender varchar(1),
        item_in_session integer,
        last_name text,
        length float,
        level text,
        location text,
        method text,
        page text,
        registration float,
        session_id integer,
        song text,
        status integer,
        ts bigint,
        user_agent  text,
        user_id integer
    )
"""

staging_songs_table_create = """
    create table if not exists staging_songs (
        num_songs integer,
        artist_id text,
        artist_latitude text,
        artist_longitude text,
        artist_location text,
        artist_name text,
        song_id text,
        title text,
        duration float,
        year integer
    )
"""

songplay_table_create = """
"""

user_table_create = """
"""

song_table_create = """
"""

artist_table_create = """
"""

time_table_create = """
"""

# STAGING TABLES

staging_events_copy = (
    """
    copy staging_events
    from '{}'
    iam_role '{}'
    region '{}'
    json '{}'
"""
).format(
    config["S3"]["LOG_DATA"],
    config["IAM_ROLE"]["ARN"],
    AWS_REGION,
    config["S3"]["LOG_JSONPATH"],
)

staging_songs_copy = (
    """
    copy staging_songs
    from '{}'
    iam_role '{}'
    region '{}'
    json 'auto'
"""
).format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"], AWS_REGION)

# FINAL TABLES

songplay_table_insert = """
"""

user_table_insert = """
"""

song_table_insert = """
"""

artist_table_insert = """
"""

time_table_insert = """
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
]  # , songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
]  # , songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
