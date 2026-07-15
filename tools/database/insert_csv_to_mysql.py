#!/usr/bin/env python3
"""
CSV 파일을 MySQL 데이터베이스에 INSERT IGNORE로 삽입하는 스크립트
(중복 시 무시)
"""
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from lib.db_utils import get_db_manager


def insert_ignore_table(table_name, csv_file):
    """CSV 파일을 MySQL 테이블에 INSERT IGNORE로 삽입"""
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
        print(f"📁 {csv_file} → {table_name} ({len(df)}개 레코드) INSERT IGNORE 시작")
        
        # 테이블별 처리
        if table_name == "artists":
            return _insert_ignore_artists(db, df)
        elif table_name == "concerts":
            return _insert_ignore_concerts(db, df)
        elif table_name == "songs":
            return _insert_ignore_songs(db, df)
        elif table_name == "setlists":
            return _insert_ignore_setlists(db, df)
        else:
            print(f"❌ 지원하지 않는 테이블: {table_name}")
            return False
            
    except Exception as e:
        print(f"❌ 삽입 실패: {e}")
        return False
    finally:
        db.disconnect()


def _insert_ignore_artists(db, df):
    """아티스트 테이블 INSERT IGNORE"""
    for _, row in df.iterrows():
        query = """
        INSERT IGNORE INTO artists (artist, debut_date, category, detail, instagram_url, keywords, img_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
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
    print(f"✅ artists 테이블 INSERT IGNORE 완료")
    return True


def _get_or_create_artist_id(db, artist_name):
    if not artist_name:
        return None
    # 1. Look up artist_id by artist_name
    query = "SELECT id FROM artists WHERE artist = %s"
    db.cursor.execute(query, (artist_name,))
    result = db.cursor.fetchone() # Use fetchone() for single result
    if result:
        return result[0]

    # 2. If not found, insert the artist and return the new id
    insert_query = "INSERT INTO artists (artist) VALUES (%s)"
    db.cursor.execute(insert_query, (artist_name,))
    db.commit()
    return db.cursor.lastrowid


def _insert_ignore_concerts(db, df):
    """콘서트 테이블 INSERT IGNORE"""
    for _, row in df.iterrows():
        artist_name = row.get('artist', '')
        artist_id = _get_or_create_artist_id(db, artist_name) # Get or create artist_id

        query = """
        INSERT IGNORE INTO concerts (code, title, artist, venue, start_date, end_date, status, poster, artist_id, introduction, label, ticket_site, ticket_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        db.cursor.execute(query, (
            row.get('code', ''),
            row.get('title', ''),
            artist_name,
            row.get('venue', ''),
            row.get('start_date', ''),
            row.get('end_date', ''),
            row.get('status', ''),
            row.get('poster', ''),
            artist_id,
            row.get('introduction', ''),
            row.get('label', ''),
            row.get('ticket_site', ''),
            row.get('ticket_url', '')
        ))
    
    db.commit()
    print(f"✅ concerts 테이블 INSERT IGNORE 완료")
    return True


def _insert_ignore_songs(db, df):
    """곡 테이블 INSERT IGNORE"""
    for _, row in df.iterrows():
        query = """
        INSERT IGNORE INTO songs (id, title, artist, lyrics, translation, pronunciation)
        VALUES (%s, %s, %s, %s, %s, %s)
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
    print(f"✅ songs 테이블 INSERT IGNORE 완료")
    return True


def _insert_ignore_setlists(db, df):
    """세트리스트 테이블 INSERT IGNORE"""
    for _, row in df.iterrows():
        query = """
        INSERT IGNORE INTO setlist_songs (id, song_order, title, artist)
        VALUES (%s, %s, %s, %s)
        """
        db.cursor.execute(query, (
            row.get('id', ''),
            row.get('song_order', 0),
            row.get('title', ''),
            row.get('artist', '')
        ))
    
    db.commit()
    print(f"✅ setlists songs 테이블 INSERT IGNORE 완료")
    return True


def main():
    """전체 INSERT IGNORE 실행"""
    print("🚀 CSV → MySQL INSERT IGNORE 시작")
    
    tables = [
        ("artists", "artists.csv"),
        ("concerts", "concerts.csv"), 
        ("songs", "songs.csv"),
        ("setlists", "setlist.csv")
    ]
    
    for table_name, csv_file in tables:
        if not insert_ignore_table(table_name, csv_file):
            print(f"❌ {table_name} INSERT IGNORE 실패")
            return False
    
    print("🎉 모든 테이블 INSERT IGNORE 완료!")
    return True


if __name__ == "__main__":
    main()
