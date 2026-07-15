#!/usr/bin/env python3
import pandas as pd
import os
import re
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))
from lib.config import Config

def debug_artist_song_search(artist_name_input: str):
    """
    Debugs the process of finding songs for a given artist.
    """
    print(f"--- 아티스트 곡 검색 디버깅 시작: '{artist_name_input}' ---")
    
    # 1. 아티스트 이름 후보 생성
    possible_artist_names = {artist_name_input.strip()}
    
    try:
        # artists.csv 경로 설정
        artists_path = Config.OUTPUT_DIR / "artists.csv"
        if not artists_path.exists():
            print(f"⚠️ artists.csv 파일을 찾을 수 없습니다: {artists_path}")
            # Try to read from the test directory as a fallback
            artists_path = Config.TEST_OUTPUT_DIR / "artists.csv"
            if not artists_path.exists():
                print(f"⚠️ 테스트 디렉토리에서도 artists.csv 파일을 찾을 수 없습니다: {artists_path}")
                return

        artists_df = pd.read_csv(artists_path, encoding='utf-8')
        artists_df_filtered = artists_df[artists_df['artist'].notna()]
        
        # 입력된 이름이 포함된 행 찾기
        artist_row = artists_df_filtered[artists_df_filtered['artist'].str.contains(artist_name_input, na=False, regex=False)]
        
        if not artist_row.empty:
            full_artist_name = artist_row.iloc[0]['artist']
            print(f"ℹ️ artists.csv에서 매치되는 이름 발견: '{full_artist_name}'")
            possible_artist_names.add(full_artist_name.strip())
            
            # '원어 (한국어)' 형식에서 이름 추출
            match = re.match(r'(.+?)\s*\((.+?)\)', full_artist_name)
            if match:
                possible_artist_names.add(match.group(1).strip())
                possible_artist_names.add(match.group(2).strip())
        else:
            print(f"ℹ️ artists.csv에서 '{artist_name_input}'을(를) 포함하는 아티스트를 찾지 못했습니다.")

    except Exception as e:
        print(f"❌ artists.csv 처리 중 오류 발생: {e}")

    print(f"\n[단계 1] 생성된 아티스트 이름 후보: {possible_artist_names}")

    # 2. songs.csv에서 곡 검색
    try:
        songs_path = Config.OUTPUT_DIR / "songs.csv"
        if not songs_path.exists():
            print(f"⚠️ songs.csv 파일을 찾을 수 없습니다: {songs_path}")
            songs_path = Config.TEST_OUTPUT_DIR / "songs.csv"
            if not songs_path.exists():
                print(f"⚠️ 테스트 디렉토리에서도 songs.csv 파일을 찾을 수 없습니다: {songs_path}")
                return
            
        songs_df = pd.read_csv(songs_path, encoding='utf-8')
        songs_df['artist'] = songs_df['artist'].astype(str) # Handle potential non-string data
        
        artist_songs = songs_df[songs_df['artist'].isin(possible_artist_names)]
        
        print(f"\n[단계 2] songs.csv 검색 결과:")
        if artist_songs.empty:
            print("❌ 위 후보 이름으로 songs.csv에서 어떠한 곡도 찾지 못했습니다.")
            
            # 어떤 이름들이 있는지 확인하기 위해 유사한 이름들을 출력
            all_song_artists = songs_df['artist'].unique()
            similar_artists = [name for name in all_song_artists if artist_name_input.lower() in name.lower()]
            if similar_artists:
                print("\n💡 songs.csv에 있는 유사한 아티스트 이름:")
                for name in similar_artists[:10]: # 최대 10개
                    print(f"  - {name}")

        else:
            print(f"✅ 총 {len(artist_songs)}개의 곡을 찾았습니다.")
            print("\n찾은 곡 목록 (최대 5개):")
            for title in artist_songs['title'].head(5).tolist():
                print(f"  - {title}")

    except Exception as e:
        print(f"❌ songs.csv 처리 중 오류 발생: {e}")
        
    print("\n--- 디버깅 종료 ---")


if __name__ == "__main__":
    artist_name = input("디버깅할 아티스트 이름을 입력하세요: ").strip()
    if artist_name:
        debug_artist_song_search(artist_name)
    else:
        print("아티스트 이름이 입력되지 않았습니다.")
