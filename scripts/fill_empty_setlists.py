#!/usr/bin/env python3
"""
기존 셋리스트 중 비어있는 것들을 songs.csv에서 채우기
"""
import pandas as pd
from pathlib import Path
import time

def fill_empty_setlists():
    """기존 셋리스트 중 비어있는 것들 채우기"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output')
    
    print("📁 파일 로드 중...")
    songs_df = pd.read_csv(csv_path / 'songs.csv')
    setlists_df = pd.read_csv(csv_path / 'setlists.csv')
    setlist_songs_df = pd.read_csv(csv_path / 'setlist_songs.csv')
    
    print(f"  songs.csv: {len(songs_df)}개 곡")
    print(f"  setlists.csv: {len(setlists_df)}개 셋리스트")
    print(f"  setlist_songs.csv: {len(setlist_songs_df)}개 레코드")
    
    # 각 셋리스트별로 유효한 곡 개수 확인
    print("\n🔍 셋리스트별 곡 개수 분석...")
    
    # 유효한 setlist_songs 필터링
    valid_setlist_songs = setlist_songs_df[
        (setlist_songs_df['title'].notna()) & 
        (setlist_songs_df['title'] != '') &
        (setlist_songs_df['artist'].notna()) & 
        (setlist_songs_df['artist'] != '')
    ]
    
    # 셋리스트별 곡 개수
    setlist_song_counts = valid_setlist_songs.groupby('setlist_id').size().to_dict()
    
    # 비어있거나 곡이 적은 셋리스트 찾기
    empty_setlists = []
    
    for _, setlist in setlists_df.iterrows():
        setlist_id = setlist['id']
        song_count = setlist_song_counts.get(setlist_id, 0)
        
        if song_count == 0:
            empty_setlists.append({
                'setlist_id': setlist_id,
                'title': setlist['title'],
                'artist': setlist['artist'],
                'song_count': song_count
            })
    
    print(f"  📊 비어있는 셋리스트: {len(empty_setlists)}개")
    
    if not empty_setlists:
        print("✅ 모든 셋리스트에 곡이 있습니다.")
        return
    
    # 비어있는 셋리스트들 출력
    print("\n📋 비어있는 셋리스트 목록:")
    for setlist in empty_setlists[:10]:  # 처음 10개만 출력
        print(f"  • ID {setlist['setlist_id']}: {setlist['title']} - {setlist['artist']}")
    
    if len(empty_setlists) > 10:
        print(f"  ... 및 {len(empty_setlists) - 10}개 더")
    
    # 백업 생성
    backup_path = csv_path / f"setlist_songs_backup_{int(time.time())}.csv"
    setlist_songs_df.to_csv(backup_path, index=False, encoding='utf-8')
    print(f"\n💾 백업 생성: {backup_path}")
    
    # 비어있는 셋리스트들 채우기
    print("\n🎵 비어있는 셋리스트들 채우는 중...")
    
    new_records = []
    filled_count = 0
    
    for empty_setlist in empty_setlists:
        setlist_id = empty_setlist['setlist_id']
        setlist_artist = empty_setlist['artist']
        
        # 해당 아티스트의 곡들 찾기
        artist_songs = songs_df[songs_df['artist'] == setlist_artist]
        
        if len(artist_songs) == 0:
            print(f"  ⚠️ {setlist_artist}의 곡이 없음 (ID: {setlist_id})")
            continue
        
        # 해당 셋리스트에 곡들 추가
        for order_idx, (_, song) in enumerate(artist_songs.iterrows(), 1):
            new_record = {
                'title': song['title'],
                'artist': song['artist'],
                'setlist_id': setlist_id,
                'order': order_idx,
                'lyrics': song.get('lyrics', ''),
                'pronunciation': song.get('pronunciation', ''),
                'translation': song.get('translation', ''),
                'musixmatch_url': song.get('musixmatch_url', '')
            }
            new_records.append(new_record)
        
        print(f"  ✅ {empty_setlist['title']}: {len(artist_songs)}곡 추가")
        filled_count += 1
    
    if not new_records:
        print("  ❌ 추가할 곡이 없습니다.")
        return
    
    # 기존 데이터와 병합
    new_df = pd.DataFrame(new_records)
    combined_df = pd.concat([setlist_songs_df, new_df], ignore_index=True)
    
    # 저장
    combined_df.to_csv(csv_path / 'setlist_songs.csv', index=False, encoding='utf-8')
    
    print(f"\n✅ 완료!")
    print(f"  채워진 셋리스트: {filled_count}개")
    print(f"  추가된 곡: {len(new_records)}개")
    print(f"  총 레코드: {len(combined_df)}개")
    print(f"  고유 곡 수: {len(combined_df[['title', 'artist']].drop_duplicates())}개")

if __name__ == "__main__":
    print("=" * 60)
    print("🎵 비어있는 셋리스트 채우기")
    print("=" * 60)
    fill_empty_setlists()
    print("=" * 60)