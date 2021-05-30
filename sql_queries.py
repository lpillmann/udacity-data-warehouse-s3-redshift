import configparser
import os

# CONFIG
AWS_REGION = os.environ.get("UDACITY_AWS_REGION")
config = configparser.ConfigParser()
config.read("dwh.cfg")


# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

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
    create table if not exists songplays (
        songplay_id text,
        start_time timestamp with time zone,
        user_id text,
        level text,
        song_id text,
        artist_id text,
        session_id text,
        location text,
        user_agent text,
        
        primary key(songplay_id),
        foreign key(user_id) references users(user_id),
        foreign key(song_id) references songs(song_id),
        foreign key(artist_id) references artists(artist_id),
        foreign key(start_time) references time(start_time)
    )
    distkey(songplay_id)
    interleaved sortkey(start_time, user_id, song_id, artist_id)
"""

user_table_create = """
    create table if not exists users (
        user_id text not null,
        first_name text,
        last_name text,
        gender text,
        level text,

        primary key(user_id)
    )
    diststyle all
    sortkey(user_id)
"""

song_table_create = """
    create table if not exists songs (
        song_id text,
        title text,
        artist_id text,
        year integer,
        duration float,

        primary key(song_id),
        foreign key(artist_id) references artists(artist_id)
    )
    distkey(song_id)
    interleaved sortkey(artist_id, song_id)
"""

artist_table_create = """
    create table if not exists artists (
        artist_id text,
        name text,
        location text,
        latitude text,
        longitude text,

        primary key(artist_id)
    )
    diststyle all
    sortkey(artist_id)
"""

time_table_create = """
    create table if not exists time (
        start_time timestamp with time zone,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday text,

        primary key(start_time)
    )
    diststyle all
    sortkey(start_time)
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
    insert into songplays (
        with events as
        (
            select * from staging_events
        ),

        songs as
        (
            select * from staging_songs
        ),

        only_next_song_events as
        (
            select
                *
            from
                events
            where
                page = 'NextSong'
        ),
        
        events_with_converted_timestamp as
        (
            select
                *,
                timestamp 'epoch' + ts * interval '1 second' / 1000 as start_time
            from
                only_next_song_events
        ),

        joined as
        (
            select
                e.start_time,
                e.user_id,
                e.level,
                s.song_id,
                s.artist_id,
                e.session_id,
                e.location,
                e.user_agent
            from
                events_with_converted_timestamp e
                left outer join songs s on e.song = s.title
        ),

        final as (
            select
                -- create songplay id based on other columns that form a unique combination
                md5(
                    coalesce(cast(user_id as text), '') || coalesce(song_id, '') || coalesce(cast(start_time as text), '')
                ) as songplay_id,
                *
            from
                joined
        )

        select * from final
    )
"""

user_table_insert = """
    insert into users (
        with events as
        (
            select * from staging_events
        ),
        
        add_row_number_and_remove_null_ids as
        (
            select
                *,
                row_number() over (partition by user_id order by ts desc) as row_number_by_user
            from
                events
            where
                user_id is not null
        ),

        latest_user_events as
        (
            -- this is done to get latest user event since `level` can change over time
            select
                *
            from
                add_row_number_and_remove_null_ids
            where
                row_number_by_user = 1
        )
        
        select
            user_id,
            first_name,
            last_name,
            gender,
            level
        from
            latest_user_events
    )
"""

song_table_insert = """
    insert into songs (
        select
            song_id,
            title,
            artist_id,
            year,
            duration
        from
            staging_songs
    )
"""

artist_table_insert = """
    insert into artists (
        select
            artist_id,
            max(artist_name) as name,
            max(artist_location) as location,
            max(artist_latitude) as latitude,
            max(artist_longitude) as longitude
        from
            staging_songs
        group by 1
    )
"""

time_table_insert = """
    insert into time (
        select
            start_time,
            extract(hour from start_time) as hour,
            extract(day from start_time) as day,
            extract(week from start_time) as week,
            extract(month from start_time) as month,
            extract(year from start_time) as year,
            extract(weekday from start_time) as weekday
        from
            songplays
    )
"""

# DATA QUALITY CHECKS
staging_events_table_quality_checks = {
    "id uniqueness": """
        select
            songplay_id,
            count(*)
        from
            songplays
        group by 1
        having count(*) > 1
    """,
    "id not null": """
        select
            songplay_id
        from
            songplays
        where
            songplay_id is null
    """,
}

songplay_table_quality_checks = {
    "id uniqueness": """
        select
            songplay_id,
            count(*)
        from
            songplays
        group by 1
        having count(*) > 1
    """,
    "id not null": """
        select
            songplay_id
        from
            songplays
        where
            songplay_id is null
    """,
}

songplay_table_quality_checks = {
    "id uniqueness": """
        select
            songplay_id,
            count(*)
        from
            songplays
        group by 1
        having count(*) > 1
    """,
    "id not null": """
        select
            songplay_id
        from
            songplays
        where
            songplay_id is null
    """,
}

user_table_quality_checks = {
    "id uniqueness": """
        select
            user_id,
            count(*)
        from
            users
        group by 1
        having count(*) > 1
    """,
    "id not null": """
        select
            user_id
        from
            users
        where
            user_id is null
    """,
}

song_table_quality_checks = {
    "id uniqueness": """
        select
            song_id,
            count(*)
        from
            songs
        group by 1
        having count(*) > 1
    """,
    "id not null": """
        select
            song_id
        from
            songs
        where
            song_id is null
    """,
}

artist_table_quality_checks = {
    "id uniqueness": """
        select
            artist_id,
            count(*)
        from
            artists
        group by 1
        having count(*) > 1
    """,
    "id not null": """
        select
            artist_id
        from
            artists
        where
            artist_id is null
    """,
}

time_table_quality_checks = {
    "id uniqueness": """
        select
            start_time,
            count(*)
        from
            time
        group by 1
        having count(*) > 1
    """,
    "id not null": """
        select
            start_time
        from
            time
        where
            start_time is null
    """,
}


# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    song_table_insert,
    user_table_insert,
    artist_table_insert,
    time_table_insert,
    songplay_table_insert,
]
quality_check_definitions = [
    songplay_table_quality_checks,
    user_table_quality_checks,
    song_table_quality_checks,
    artist_table_quality_checks,
    time_table_quality_checks,
]
