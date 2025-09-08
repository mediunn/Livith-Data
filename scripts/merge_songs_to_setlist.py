import pandas as pd
import os
from datetime import datetime

def merge_songs_to_setlist():
    songs_path = 'output/main_output/songs.csv'
    setlist_songs_path = 'output/main_output/setlist_songs.csv'
    concerts_path = 'output/main_output/concerts.csv'
    setlists_path = 'output/main_output/setlists.csv'
    
    songs_df = pd.read_csv(songs_path, encoding='utf-8-sig')
    setlist_songs_df = pd.read_csv(setlist_songs_path, encoding='utf-8-sig')
    concerts_df = pd.read_csv(concerts_path, encoding='utf-8-sig')
    setlists_df = pd.read_csv(setlists_path, encoding='utf-8-sig')
    
    print(f"기존 songs.csv 데이터: {len(songs_df)}개")
    print(f"기존 setlist_songs.csv 데이터: {len(setlist_songs_df)}개")
    
    # 백업
    backup_dir = 'output/main_output/backups'
    os.makedirs(backup_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    setlist_songs_df.to_csv(f'{backup_dir}/setlist_songs_backup_{timestamp}.csv', index=False, encoding='utf-8-sig')
    
    # songs 데이터를 setlist_songs 형식으로 변환
    new_setlist_songs = []
    
    for _, song_row in songs_df.iterrows():
        artist = song_row['artist']
        
        # 해당 아티스트의 셋리스트 찾기
        artist_setlists = setlists_df[setlists_df['artist'] == artist]
        
        if artist_setlists.empty:
            # 아티스트의 콘서트 찾기
            artist_concerts = concerts_df[concerts_df['artist'] == artist]
            if not artist_concerts.empty:
                concert = artist_concerts.iloc[0]
                # 기본 셋리스트 제목 생성
                setlist_title = f"{artist} 콘서트 셋리스트"
                setlist_date = concert['concert_date'] if 'concert_date' in concert else '2025-01-01'
            else:
                setlist_title = f"{artist} 콘서트 셋리스트"
                setlist_date = '2025-01-01'
        else:
            # 첫 번째 셋리스트 사용
            setlist = artist_setlists.iloc[0]
            setlist_title = setlist['title']  # 'title' 컬럼 사용
            setlist_date = setlist['start_date']  # 'start_date' 컬럼 사용
        
        # 이미 setlist_songs에 있는지 확인
        existing = setlist_songs_df[
            (setlist_songs_df['song_title'] == song_row['title']) & 
            (setlist_songs_df['setlist_title'] == setlist_title)
        ]
        
        if existing.empty:
            # 새로운 setlist_song 추가
            new_setlist_songs.append({
                'setlist_title': setlist_title,
                'song_title': song_row['title'],
                'setlist_date': setlist_date,
                'order_index': len(new_setlist_songs) + 1,
                'fanchant': '',
                'fanchant_point': ''
            })
    
    if new_setlist_songs:
        new_df = pd.DataFrame(new_setlist_songs)
        # 기존 데이터와 병합 (중복 제거, setlist_songs 우선)
        combined_df = pd.concat([setlist_songs_df, new_df], ignore_index=True)
        
        # 중복 제거 (setlist_title과 song_title 기준, 첫 번째 값(기존 setlist_songs) 유지)
        combined_df = combined_df.drop_duplicates(subset=['setlist_title', 'song_title'], keep='first')
        
        # 저장
        combined_df.to_csv(setlist_songs_path, index=False, encoding='utf-8-sig')
        print(f"\n병합 완료! 새로운 setlist_songs.csv 데이터: {len(combined_df)}개")
        print(f"추가된 데이터: {len(new_setlist_songs)}개")
    else:
        print("\n추가할 데이터가 없습니다.")

if __name__ == "__main__":
    merge_songs_to_setlist()