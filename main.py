import requests
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import json
from datetime import datetime
import datetime
import sys
import sqlite3

DATABASE = 'sqlite:///spotify_tracks.sqlite'
USER = 'peteroko'
TOKEN = '''BQBtca5kHtXEp-o1U2SBm1sXl_m9xS2SLiX_1dk0CvRlCux6FJybIl5_QHVsfIg1EYwtQyCSllFU3w9vK3OjwOtPxNnzeC5XXKg7zGh5Zk9b6jdQCufIqm8r11PFewCSciUxb403XjWrPlxf3g'''
if __name__ == "__main__":

    headers = {
        "Accept" : "application/json",
        "Content-Type": "application/json",
        "Authorization" : "Bearer %s"%(TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday = int(yesterday.timestamp())*1000


    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday), headers=headers)
    

    data = r.json()
    songs =[]
    artist = []
    played_at =[]
    timestamp =[]
 
    for song in data['items']:
        songs.append(song['track']['name'])
        artist.append(song['track']['album']['artists'][0]['name'])
        played_at .append(song['played_at'])
        timestamp.append(song['played_at'][0:10])

    songs_dict = {
        'Name':songs,
        'Artist':artist,
        'Played at': played_at,
        'timestamp' : timestamp
    }

    df = pd.DataFrame(songs_dict)

    if df.empty:
        sys.exit()
    
    if not pd.Series(df['Played at']).is_unique:
        raise Exception('Primary Key is violated')

    engine = sqlalchemy.create_engine(DATABASE)

    if not database_exists(engine.url):
        create_database(engine.url)
    df.to_sql('songs',engine, if_exists='append', index=False)

    
    