import pandas as pd
import numpy as np
import mysql.connector
import os
from unidecode import unidecode
import re
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), 'scripts')))
from extract_grammys import create_db_connection, read_query
from extract_spotify import extract_spotify_data

df_spotify= extract_spotify_data("../data/spotify_dataset.csv")

data_grammys = "SELECT * FROM db_grammys.grammys"

connection = create_db_connection("localhost", "root", "annie", "db_grammys") # Connect to the Database
results = read_query(connection, data_grammys)

df_grammys= pd.DataFrame(results, columns=['id','year','title','published_at','updated_at', 'category', 'nominee','artist', 'workers','img','winner'])
df_grammys.head()

def clean_text(text):
    
    if pd.isna(text):
        return ""
    text = str(text).lower()
    text = unidecode(text)  
    text = re.sub(r'[^\w\s]', '', text)  
    text = re.sub(r'\s+', ' ', text).strip()  
    return text

def transform_grammys(df_grammys):
    df = df_grammys.copy()
    
    # fill img and nominee null values 
    df['img'] = df['img'].fillna("no image available")
    df['nominee'] = df['nominee'].fillna("Unknown")

    #Categories where artist can be inferred from nominee
    artist_from_nominee_categories = [
        'Best New Artist',
        'Best Pop Solo Performance',
        'Best Pop Duo/Group Performance',
        'Best Traditional Pop Vocal Album',
        'Best Pop Vocal Album',
        'Best Dance Recording',
        'Best Dance/Electronic Album',
        'Best Contemporary Instrumental Album',
        'Best Rock Performance',
        'Best Metal Performance',
        'Best Rock Album',
        'Best Alternative Music Album',
        'Best R&B Performance',
        'Best Traditional R&B Performance',
        'Best R&B Album',
        'Best Rap Performance',
        'Best Rap Album',
        'Best Country Solo Performance',
        'Best Country Duo/Group Performance',
        'Best Country Album',
        'Best Jazz Vocal Album',
        'Best Jazz Instrumental Album',
        'Best Latin Pop Album',
        'Best Regional Mexican Music Album',
        'Best Gospel Performance/Song',
        'Best Contemporary Christian Music Album',
        'Best Reggae Album',
        'Best World Music Album',
        'Best Children\'s Album',
        'Best Comedy Album',
        'Best Musical Theater Album',
        'Best Compilation Soundtrack For Visual Media'
    ]
    
    # Categories where artist cannot be inferred from nominee
    technical_categories = [
        'Song Of The Year',  
        'Best Opera Recording',  
        'Best Album Notes',  
        'Best Country Song',  
        'Best Instrumental Composition',  
        'Best Historical Album',  
        'Best Chamber Music Performance',  
        'Best Instrumental Arrangement',  
        'Best Orchestral Performance',  
        'Best Classical Album',  
        'Best Rock Song',  
        'Best Rhythm & Blues Song',  
        'Best Recording Package',  
        'Best Choral Performance',  
        'Best Engineered Album, Classical',  
        'Producer Of The Year, Non-Classical',  
        'Producer Of The Year, Classical',  
        'Best Engineered Recording - Non-Classical',  
        'Best R&B Song'  
    ]
    
    mask_nominee = (df['artist'].isna()) & (df['category'].isin(artist_from_nominee_categories))
    df.loc[mask_nominee, 'artist'] = df.loc[mask_nominee, 'nominee']
    
    # For technical categories, fill with "Various Artists"
    df['artist'] = df['artist'].fillna("Varius Artists")

    #For workers column, fill NaN with "Unknown"
    df['workers'] = df['workers'].fillna("Unknown")

    # clean and convert date columns
    df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
    
    # normalize category column 
    df['category'] = df['category'].str.strip()

    # split and clean artist names
    df['artist_list'] = df['artist'].str.split(',|&|feat\.|featuring', regex=True)
    df['artist_list'] = df['artist_list'].apply(
        lambda x: [clean_text(artist) for artist in x] if isinstance(x, list) else [clean_text(x)]
    )
    #convert winner column to boolean
    df['winner'] = df['winner'].astype(bool)

    return df

df_grammys_clean = transform_grammys(df_grammys)

def transform_spotify(df_spotify):
    df = df_spotify.copy()
    
    # delete Unnamed: 0 column
    if 'Unnamed: 0' in df.columns:
        df = df.drop('Unnamed: 0', axis=1)
    # rename columns for consistency
    df = df.rename(columns={
        'artists': 'artist',  # Singular para consistencia con Grammys
        'album_name': 'album',  # Más conciso
        'track_name': 'track',  # Más conciso
        'track_genre': 'genre'})
    
    # fill missing values
    df['artist'] = df['artist'].fillna("Unknown")
    df['album'] = df['album'].fillna("Unknown")
    df['track'] = df['track'].fillna("Unknown")

    # split and clean artist 
    df['artists_list'] = df['artist'].str.split(',|;', regex=True)
    df['artists_list'] = df['artists_list'].apply(
        lambda x: [clean_text(artist) for artist in x] if isinstance(x, list) else [clean_text(x)]
    )
    
    # convert duration from ms to min
    df['duration_min'] = df['duration_ms'] / 60000

    #normalize genre column
    df['genre'] = df['genre'].str.lower().str.strip()

    # remove duplicates based on track_id, keeping the first occurrence
    df = df.drop_duplicates(subset=['track_id'], keep='first')

    return df

df_spotify_clean = transform_spotify(df_spotify)

def merge_grammys_spotify(df_grammys_clean, df_spotify_clean):
    
    # Explore grammys artist list
    df_grammys_exploded = df_grammys_clean.explode('artist_list').copy()
    df_grammys_exploded = df_grammys_exploded.rename(columns={'artist_list': 'artist_clean'})
    
    # Explore spotify artist list
    df_spotify_exploded = df_spotify_clean.explode('artists_list').copy()
    df_spotify_exploded = df_spotify_exploded.rename(columns={'artists_list': 'artist_clean'})
    
    # select relevant columns
    grammys_cols = ['artist_clean', 'id', 'year', 'title', 'published_at', 'updated_at', 
                    'category', 'nominee', 'artist', 'workers', 'winner']
    
    spotify_cols = ['artist_clean', 'track_id', 'artist', 'album', 'track', 'popularity', 
                    'duration_ms', 'explicit', 'danceability', 'energy', 'key', 'loudness', 
                    'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 
                    'valence', 'tempo', 'time_signature', 'genre', 'duration_min']
    
    df_grammys_selected = df_grammys_exploded[grammys_cols]
    df_spotify_selected = df_spotify_exploded[spotify_cols]
    
    # Rename original artist columns to avoid confusion
    df_grammys_selected = df_grammys_selected.rename(columns={'artist': 'artist_original_grammy'})
    df_spotify_selected = df_spotify_selected.rename(columns={'artist': 'artist_original_spotify'})
    
    # Merge
    df_merged = pd.merge(
        df_grammys_selected,
        df_spotify_selected,
        on='artist_clean',
        how='inner'
    )
    
    # organize columns
    cols_order = [
        
        'artist_clean', 'artist_original_grammy', 'artist_original_spotify',
        'id', 'year', 'category', 'nominee', 'winner', 'workers',
        'title', 'published_at', 'updated_at',
        'track_id', 'track', 'album', 'genre',
        'popularity', 'duration_min', 'explicit',
        'danceability', 'energy', 'valence', 'tempo', 
        'loudness', 'speechiness', 'acousticness', 'instrumentalness', 'liveness',
        'key', 'mode', 'time_signature'
    ]
    
    df_merged = df_merged[cols_order]

    return df_merged
df_final = merge_grammys_spotify(df_grammys_clean, df_spotify_clean)

df_final.to_csv("../data/grammys_spotify_merged.csv", index=False, encoding='utf-8')
