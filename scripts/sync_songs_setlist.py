#!/usr/bin/env python3
"""
songs.csv와 setlist_songs.csv 싱크 맞추기
songs에 있는데 setlist_songs에 없는 곡들을 setlist_songs에 추가
"""
import pandas as pd
from pathlib import Path
import time

def sync_songs_setlist():
    """songs.csv와 setlist_songs.csv 싱크 맞추기"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output')
    
    # 파일 읽기
    print("📁 파일 로드 중...")
    songs_df = pd.read_csv(csv_path / 'songs.csv')
    setlist_songs_df = pd.read_csv(csv_path / 'setlist_songs.csv')
    
    print(f"  songs.csv: {len(songs_df)}개 곡")
    print(f"  setlist_songs.csv: {len(setlist_songs_df)}개 레코드")
    
    # songs에 있는 곡 목록
    songs_set = set(zip(songs_df['title'], songs_df['artist']))
    
    # setlist_songs에 있는 곡 목록 (빈 값 제외)
    valid_setlist = setlist_songs_df[
        (setlist_songs_df['title'].notna()) & 
        (setlist_songs_df['title'] != '') &
        (setlist_songs_df['artist'].notna()) & 
        (setlist_songs_df['artist'] != '')
    ]
    setlist_set = set(zip(valid_setlist['title'], valid_setlist['artist']))
    
    print(f"  setlist_songs 유효 곡: {len(setlist_set)}개")
    
    # 누락된 곡들
    missing_songs = songs_set - setlist_set
    print(f"  누락된 곡: {len(missing_songs)}개")
    
    if not missing_songs:
        print("✅ 이미 모든 곡이 싱크되어 있습니다.")
        return
    
    # 백업 생성
    backup_path = csv_path / f"setlist_songs_backup_{int(time.time())}.csv"
    setlist_songs_df.to_csv(backup_path, index=False, encoding='utf-8')
    print(f"💾 백업 생성: {backup_path}")
    
    # 누락된 곡들을 setlist_songs에 추가
    print("🔄 누락된 곡들 추가 중...")
    
    # 최대 setlist_id 찾기
    max_setlist_id = setlist_songs_df['setlist_id'].max() if not setlist_songs_df.empty else 0
    
    # songs.csv에서 곡 정보 가져오기
    songs_info = {}
    for _, row in songs_df.iterrows():
        key = (row['title'], row['artist'])
        songs_info[key] = row
    
    # 새로운 레코드들 생성
    new_records = []
    current_setlist_id = max_setlist_id + 1
    order = 1
    
    for title, artist in missing_songs:
        song_info = songs_info[(title, artist)]
        
        new_record = {
            'title': title,
            'artist': artist,
            'setlist_id': current_setlist_id,
            'order': order,
            'lyrics': song_info.get('lyrics', ''),
            'pronunciation': song_info.get('pronunciation', ''),
            'translation': song_info.get('translation', ''),
            'musixmatch_url': song_info.get('musixmatch_url', '')
        }
        new_records.append(new_record)
        
        order += 1
        # 20곡마다 새 setlist_id
        if order > 20:
            current_setlist_id += 1
            order = 1
    
    # 기존 데이터와 병합
    new_df = pd.DataFrame(new_records)
    combined_df = pd.concat([setlist_songs_df, new_df], ignore_index=True)
    
    # 저장
    combined_df.to_csv(csv_path / 'setlist_songs.csv', index=False, encoding='utf-8')
    
    print(f"✅ 완료!")
    print(f"  추가된 곡: {len(new_records)}개")
    print(f"  총 레코드: {len(combined_df)}개")
    print(f"  고유 곡 수: {len(combined_df[['title', 'artist']].drop_duplicates())}개")

if __name__ == "__main__":
    print("=" * 60)
    print("🔄 songs.csv ↔ setlist_songs.csv 싱크 맞추기")
    print("=" * 60)
    sync_songs_setlist()
    print("=" * 60)