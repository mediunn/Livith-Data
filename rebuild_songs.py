#!/usr/bin/env python3
"""
setlist_songs.csv에서 songs.csv 재구성
"""
import csv
import pandas as pd
from collections import OrderedDict
from pathlib import Path

def get_artist_from_setlists():
    """setlists.csv에서 아티스트 정보 매핑"""
    setlists_file = Path('output/setlists.csv')
    artist_map = {}
    
    if setlists_file.exists():
        df = pd.read_csv(setlists_file)
        if 'title' in df.columns and 'artist' in df.columns:
            artist_map = dict(zip(df['title'], df['artist']))
    
    return artist_map

# setlist_songs.csv 읽기
songs_dict = OrderedDict()
artist_map = get_artist_from_setlists()

with open('output/setlist_songs.csv', 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        song_title = row.get('song_title', '').strip()
        if song_title and song_title not in songs_dict:
            setlist_title = row.get('setlist_title', '')
            
            # setlists.csv에서 정확한 아티스트 정보 가져오기
            artist = artist_map.get(setlist_title, "")
            
            songs_dict[song_title] = {
                'title': song_title,
                'artist': artist,
                'lyrics': '',
                'pronunciation': '',
                'translation': '',
                'youtube_id': ''
            }

# songs.csv 쓰기
with open('output/songs.csv', 'w', encoding='utf-8-sig', newline='') as f:
    fieldnames = ['title', 'artist', 'lyrics', 'pronunciation', 'translation', 'youtube_id']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    
    for song in songs_dict.values():
        writer.writerow(song)

print(f"✅ songs.csv 재구성 완료: {len(songs_dict)}곡")

# 결과 미리보기
print("\n처음 10곡:")
for i, (title, info) in enumerate(list(songs_dict.items())[:10], 1):
    print(f"{i}. {title} - {info['artist']}")