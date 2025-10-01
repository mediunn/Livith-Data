#!/usr/bin/env python3
"""
CSV 파일을 MySQL 데이터베이스에 업서트하는 스크립트
"""
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from lib.db_utils import get_db_manager


def upsert_table(table_name, csv_file):
    """CSV 파일을 MySQL 테이블에 업서트"""
    db = get_db_manager()
    
    # 연결
    if not db.connect_with_ssh():
        return False
    
    try:
        # CSV 읽기
        csv_path = db.get_data_path(csv_file)
        if not os.path.exists(csv_path):
            print(f"⚠️ {csv_file} 파일이 없습니다.")
            return True
            
        df = pd.read_csv(csv_path, encoding='utf-8').fillna('')
        print(f"📁 {csv_file} → {table_name} ({len(df)}개 레코드)")
        
        # 테이블별 처리
        if table_name == "artists":
            return _upsert_artists(db, df)
        elif table_name == "concerts":
            return _upsert_concerts(db, df)
        elif table_name == "songs":
            return _upsert_songs(db, df)
        elif table_name == "setlists":
            return _upsert_setlists(db, df)
        else:
            print(f"❌ 지원하지 않는 테이블: {table_name}")
            return False
            
    except Exception as e:
        print(f"❌ 업서트 실패: {e}")
        return False
    finally:
        db.disconnect()


def _upsert_artists(db, df):
    """아티스트 테이블 업서트"""
    for _, row in df.iterrows():
        query = """
        INSERT INTO artists (artist, debut_date, category, detail, instagram_url, keywords, img_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        debut_date = VALUES(debut_date),
        category = VALUES(category),
        detail = VALUES(detail),
        instagram_url = VALUES(instagram_url),
        keywords = VALUES(keywords),
        img_url = VALUES(img_url)
        """
        db.cursor.execute(query, (
            row.get('artist', ''),
            row.get('debut_date', ''),
            row.get('group_type', ''),
            row.get('introduction', ''),
            row.get('social_media', ''),
            row.get('keywords', ''),
            row.get('img_url', '')
        ))
    
    db.commit()
    print(f"✅ artists 테이블 업데이트 완료")
    return True


def _upsert_concerts(db, df):
    """콘서트 테이블 업서트"""
    for _, row in df.iterrows():
        query = """
        INSERT INTO concerts (id, title, artist, venue, start_date, end_date, status, poster)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        title = VALUES(title),
        artist = VALUES(artist),
        venue = VALUES(venue),
        start_date = VALUES(start_date),
        end_date = VALUES(end_date),
        status = VALUES(status),
        poster = VALUES(poster)
        """
        db.cursor.execute(query, (
            row.get('id', ''),
            row.get('title', ''),
            row.get('artist', ''),
            row.get('venue', ''),
            row.get('start_date', ''),
            row.get('end_date', ''),
            row.get('status', ''),
            row.get('poster', '')
        ))
    
    db.commit()
    print(f"✅ concerts 테이블 업데이트 완료")
    return True


def _upsert_songs(db, df):
    """곡 테이블 업서트"""
    for _, row in df.iterrows():
        query = """
        INSERT INTO songs (id, title, artist, lyrics, translation, pronunciation)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        title = VALUES(title),
        artist = VALUES(artist),
        lyrics = VALUES(lyrics),
        translation = VALUES(translation),
        pronunciation = VALUES(pronunciation)
        """
        db.cursor.execute(query, (
            row.get('id', ''),
            row.get('title', ''),
            row.get('artist', ''),
            row.get('lyrics', ''),
            row.get('translation', ''),
            row.get('pronunciation', '')
        ))
    
    db.commit()
    print(f"✅ songs 테이블 업데이트 완료")
    return True


def _upsert_setlists(db, df):
    """세트리스트 테이블 업서트"""
    for _, row in df.iterrows():
        query = """
        INSERT INTO setlist_songs (id, song_order, title, artist)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        title = VALUES(title),
        artist = VALUES(artist)
        """
        db.cursor.execute(query, (
            row.get('id', ''),
            row.get('song_order', 0),
            row.get('title', ''),
            row.get('artist', '')
        ))
    
    db.commit()
    print(f"✅ setlists songs 테이블 업데이트 완료")
    return True


def main():
    """전체 업서트 실행"""
    print("🚀 CSV → MySQL 업서트 시작")
    
    tables = [
        ("artists", "artists.csv"),
        ("concerts", "concerts.csv"), 
        ("songs", "songs.csv"),
        ("setlists", "setlist.csv")
    ]
    
    for table_name, csv_file in tables:
        if not upsert_table(table_name, csv_file):
            print(f"❌ {table_name} 업서트 실패")
            return False
    
    print("🎉 모든 테이블 업서트 완료!")
    return True


if __name__ == "__main__":
    main()