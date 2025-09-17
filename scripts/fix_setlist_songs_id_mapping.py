#!/usr/bin/env python3
"""
setlist_songs의 setlist_id를 올바른 값으로 매핑 수정
원본: setlist_id 1000부터 시작하는 가상 ID
실제: setlists.csv의 실제 id와 매칭
"""
import pandas as pd
from pathlib import Path
import time
from datetime import datetime

def fix_setlist_songs_id_mapping():
    """setlist_songs의 setlist_id를 올바르게 매핑"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output')
    
    print("📁 파일 로드 중...")
    # 원본 파일들 로드
    original_setlist_songs = pd.read_csv(csv_path / 'setlist_songs_original_backup.csv')
    setlists_df = pd.read_csv(csv_path / 'setlists.csv')
    concert_setlists_df = pd.read_csv(csv_path / 'concert_setlists.csv')
    songs_df = pd.read_csv(csv_path / 'songs.csv')
    
    print(f"  원본 setlist_songs: {len(original_setlist_songs)}개")
    print(f"  setlists: {len(setlists_df)}개")
    print(f"  concert_setlists: {len(concert_setlists_df)}개")
    print(f"  songs: {len(songs_df)}개")
    
    # 1. song_id 매핑 생성
    song_mapping = {}
    for i, (_, song) in enumerate(songs_df.iterrows(), 1):
        key = (song['title'], song['artist'])
        song_mapping[key] = i
    
    # 2. 원본 setlist_songs의 가상 setlist_id → 실제 setlists id 매핑 생성
    print("\n🔍 setlist_id 매핑 생성...")
    
    # 아티스트별로 곡 그룹핑
    artist_to_virtual_setlist = {}
    for _, row in original_setlist_songs.iterrows():
        virtual_id = row['setlist_id']
        artist = row['artist']
        if artist not in artist_to_virtual_setlist:
            artist_to_virtual_setlist[artist] = virtual_id
    
    print(f"  아티스트별 가상 setlist_id: {len(artist_to_virtual_setlist)}개")
    
    # 실제 setlists에서 아티스트별 id 찾기
    virtual_to_real_mapping = {}
    for artist, virtual_id in artist_to_virtual_setlist.items():
        # setlists에서 해당 아티스트의 실제 id 찾기
        matching_setlists = setlists_df[setlists_df['artist'] == artist]
        if not matching_setlists.empty:
            real_id = matching_setlists.iloc[0]['id']  # 첫 번째 매칭된 setlist id 사용
            virtual_to_real_mapping[virtual_id] = real_id
            print(f"    {artist}: {virtual_id} → {real_id}")
        else:
            print(f"    ⚠️ {artist}에 대한 setlist를 찾을 수 없음 (virtual_id: {virtual_id})")
    
    # 3. 매핑된 정보로 MySQL 구조 생성
    print(f"\n🔄 MySQL 구조로 변환 중... (매핑된 setlist_id: {len(virtual_to_real_mapping)}개)")
    
    new_records = []
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    skipped = 0
    
    for _, row in original_setlist_songs.iterrows():
        virtual_setlist_id = row['setlist_id']
        real_setlist_id = virtual_to_real_mapping.get(virtual_setlist_id)
        
        if not real_setlist_id:
            skipped += 1
            continue
        
        # song_id 찾기
        song_key = (row['title'], row['artist'])
        song_id = song_mapping.get(song_key)
        
        if not song_id:
            skipped += 1
            continue
        
        # concert_setlists에서 setlist_title 찾기
        matching_cs = concert_setlists_df[concert_setlists_df['setlist_id'] == real_setlist_id]
        setlist_title = matching_cs.iloc[0]['setlist_title'] if not matching_cs.empty else ''
        
        # setlists에서 날짜 정보 찾기
        matching_setlist = setlists_df[setlists_df['id'] == real_setlist_id]
        setlist_date = matching_setlist.iloc[0]['start_date'] if not matching_setlist.empty else ''
        
        new_record = {
            'setlist_id': real_setlist_id,
            'song_id': song_id,
            'order_index': row.get('order', 0),
            'fanchant': '',
            'created_at': current_time,
            'setlist_date': setlist_date,
            'setlist_title': setlist_title,
            'song_title': row['title'],
            'updated_at': current_time,
            'fanchant_point': ''
        }
        
        new_records.append(new_record)
    
    # 4. 새로운 CSV 저장
    if new_records:
        new_df = pd.DataFrame(new_records)
        
        # 기존 setlist_songs 백업
        backup_path = csv_path / f"setlist_songs_backup_{int(time.time())}.csv"
        if (csv_path / 'setlist_songs.csv').exists():
            pd.read_csv(csv_path / 'setlist_songs.csv').to_csv(backup_path, index=False, encoding='utf-8')
            print(f"\n💾 기존 파일 백업: {backup_path}")
        
        # 새 파일 저장
        new_df.to_csv(csv_path / 'setlist_songs.csv', index=False, encoding='utf-8')
        
        print(f"\n✅ setlist_id 매핑 수정 완료!")
        print(f"  처리된 레코드: {len(new_records)}개")
        print(f"  스킵된 레코드: {skipped}개")
        print(f"  사용된 실제 setlist_id 범위: {min([r['setlist_id'] for r in new_records])} ~ {max([r['setlist_id'] for r in new_records])}")
        
        # 샘플 출력
        print(f"\n📋 변환된 데이터 샘플:")
        for i, record in enumerate(new_records[:3]):
            print(f"  {i+1}. {record['song_title']} (setlist_id: {record['setlist_id']}, song_id: {record['song_id']})")
    else:
        print("❌ 변환할 데이터가 없습니다.")

if __name__ == "__main__":
    print("=" * 60)
    print("🔧 setlist_songs setlist_id 매핑 수정")
    print("=" * 60)
    fix_setlist_songs_id_mapping()
    print("=" * 60)