#!/usr/bin/env python3
"""
setlist_songs.csv를 MySQL 구조에 맞게 변환
MySQL 필드: id, setlist_id, song_id, order_index, fanchant, created_at, setlist_date, setlist_title, song_title, updated_at, fanchant_point
CSV 필드: title, artist, setlist_id, order, lyrics, pronunciation, translation, musixmatch_url
"""
import pandas as pd
from pathlib import Path
import time
from datetime import datetime

def convert_setlist_songs_format():
    """setlist_songs CSV를 MySQL 구조에 맞게 변환"""
    csv_path = Path('/Users/youz2me/Xcode/Livith-Data/output/main_output')
    
    print("📁 파일 로드 중...")
    setlist_songs_df = pd.read_csv(csv_path / 'setlist_songs.csv')
    setlists_df = pd.read_csv(csv_path / 'setlists.csv')
    concert_setlists_df = pd.read_csv(csv_path / 'concert_setlists.csv')
    songs_df = pd.read_csv(csv_path / 'songs.csv')
    
    print(f"  setlist_songs.csv: {len(setlist_songs_df)}개 레코드")
    print(f"  setlists.csv: {len(setlists_df)}개")
    print(f"  concert_setlists.csv: {len(concert_setlists_df)}개")
    print(f"  songs.csv: {len(songs_df)}개")
    
    # song_id 매핑 생성 (title, artist → song_id 가정)
    print("\n🔍 song_id 매핑 생성 중...")
    song_mapping = {}
    for i, (_, song) in enumerate(songs_df.iterrows(), 1):
        key = (song['title'], song['artist'])
        song_mapping[key] = i  # MySQL에서 song_id는 1부터 시작한다고 가정
    
    # setlist 정보 매핑 (concert_setlists에서 가져오기)
    setlist_info = {}
    for _, cs in concert_setlists_df.iterrows():
        setlist_info[cs['setlist_id']] = {
            'title': cs['setlist_title'],  # concert_setlists의 setlist_title 사용
            'start_date': ''  # 날짜는 별도로 처리
        }
    
    # setlists에서 날짜 정보 보완
    for _, setlist in setlists_df.iterrows():
        if setlist['id'] in setlist_info:
            setlist_info[setlist['id']]['start_date'] = setlist.get('start_date', '')
    
    # 백업 생성
    backup_path = csv_path / f"setlist_songs_backup_{int(time.time())}.csv"
    setlist_songs_df.to_csv(backup_path, index=False, encoding='utf-8')
    print(f"\n💾 백업 생성: {backup_path}")
    
    # 새로운 구조로 변환
    print("\n🔄 MySQL 구조로 변환 중...")
    
    new_records = []
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for _, row in setlist_songs_df.iterrows():
        # song_id 찾기
        song_key = (row['title'], row['artist'])
        song_id = song_mapping.get(song_key)
        
        if not song_id:
            print(f"  ⚠️ 곡을 찾을 수 없음: {row['title']} - {row['artist']}")
            continue
        
        # setlist 정보 가져오기
        setlist_id = row['setlist_id']
        setlist_data = setlist_info.get(setlist_id, {})
        
        new_record = {
            # id는 MySQL에서 AUTO_INCREMENT로 생성
            'setlist_id': setlist_id,
            'song_id': song_id,
            'order_index': row.get('order', 0),
            'fanchant': '',  # 빈 값으로 초기화
            'created_at': current_time,
            'setlist_date': setlist_data.get('start_date', ''),
            'setlist_title': setlist_data.get('title', ''),
            'song_title': row['title'],
            'updated_at': current_time,
            'fanchant_point': ''  # 빈 값으로 초기화
        }
        
        new_records.append(new_record)
    
    # 새로운 DataFrame 생성
    new_df = pd.DataFrame(new_records)
    
    # 저장
    new_path = csv_path / 'setlist_songs_mysql_format.csv'
    new_df.to_csv(new_path, index=False, encoding='utf-8')
    
    print(f"\n✅ 변환 완료!")
    print(f"  원본: {len(setlist_songs_df)}개 레코드")
    print(f"  변환됨: {len(new_df)}개 레코드")
    print(f"  저장 위치: {new_path}")
    
    # 샘플 출력
    print(f"\n📋 변환된 데이터 샘플 (처음 3개):")
    for i, (_, row) in enumerate(new_df.head(3).iterrows()):
        print(f"  {i+1}. {row['song_title']} (setlist_id: {row['setlist_id']}, song_id: {row['song_id']}, order: {row['order_index']})")

if __name__ == "__main__":
    print("=" * 60)
    print("🔄 setlist_songs CSV → MySQL 구조 변환")
    print("=" * 60)
    convert_setlist_songs_format()
    print("=" * 60)