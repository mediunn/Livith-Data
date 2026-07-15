#!/usr/bin/env python3
"""
CSV 파일을 MySQL 데이터베이스에 업서트하는 스크립트
"""
import pandas as pd
import os
import re
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from lib.db_utils import get_db_manager, get_dev_db_manager, get_stage_db_manager

def upsert_table(table_name, csv_file, db=None):
    """CSV 파일을 MySQL 테이블에 업서트"""
    if db is None:
        db = get_db_manager()
    
    if not db.connect_with_ssh():
        return False

    
    try:
        csv_path = db.get_data_path(csv_file)
        if not os.path.exists(csv_path):
            print(f"⚠️ {csv_file} 파일이 없습니다.")
            return True
            
        df = pd.read_csv(csv_path, encoding='utf-8').fillna('')
        print(f"📁 {csv_file} → {table_name} ({len(df)}개 레코드)")
        
        if table_name == "artists":
            return _upsert_artists(db, df)
        elif table_name == "concerts":
            return _upsert_concerts(db, df)
        elif table_name == "songs":
            return _upsert_songs(db, df)
        elif table_name == "setlists":
            return _upsert_setlists(db, df)
        elif table_name == "schedule":
            return _upsert_schedule(db, df)
        elif table_name == "concert_genres":
            return _upsert_concert_genres(db, df)
        else:
            print(f"❌ 지원하지 않는 테이블: {table_name}")
            return False
            
    except Exception as e:
        print(f"❌ 업서트 실패: {e}")
        return False
    finally:
        db.disconnect()

def _find_existing_artist(db, artist_name: str):
    """이름 기반 아티스트 중복 검사 (공백·대소문자 무시). 있으면 (id, artist명) 반환, 없으면 None."""
    # 1차: 완전 일치
    db.cursor.execute("SELECT id, artist FROM artists WHERE artist = %s", (artist_name,))
    row = db.cursor.fetchone()
    if row:
        return row

    # 2차: 한국어명(괄호 안) 매칭
    korean_match = re.search(r'\(([가-힣\s]+)\)', artist_name)
    if korean_match:
        korean_name = korean_match.group(1).strip()
        korean_name_no_space = korean_name.replace(' ', '')

        db.cursor.execute(
            "SELECT id, artist FROM artists WHERE artist LIKE %s LIMIT 1",
            (f'%({korean_name})%',)
        )
        row = db.cursor.fetchone()
        if row:
            return row

        if korean_name_no_space != korean_name:
            db.cursor.execute(
                "SELECT id, artist FROM artists WHERE REPLACE(artist, ' ', '') LIKE %s LIMIT 1",
                (f'%({korean_name_no_space})%',)
            )
            row = db.cursor.fetchone()
            if row:
                return row

    # 3차: 영문명만 공백·대소문자 무시
    english_part = artist_name.split('(')[0].strip()
    if english_part:
        english_normalized = english_part.replace(' ', '').lower()
        db.cursor.execute(
            "SELECT id, artist FROM artists "
            "WHERE LOWER(REPLACE(TRIM(SUBSTRING_INDEX(artist, '(', 1)), ' ', '')) = %s LIMIT 1",
            (english_normalized,)
        )
        row = db.cursor.fetchone()
        if row:
            return row

    return None


def _upsert_artists(db, df):
    """아티스트 테이블 업서트"""
    skipped = 0
    for _, row in df.iterrows():
        artist_name = row.get('artist', '')
        artist_id = _parse_id(row.get('id', ''))
        current_time = datetime.now()

        # id 없는 신규 아티스트만 이름 기반 중복 검사
        if not artist_id:
            existing = _find_existing_artist(db, artist_name)
            if existing:
                existing_id, existing_name = existing
                if existing_name != artist_name:
                    print(f"  → 중복 스킵: '{artist_name}' = DB의 '{existing_name}' (id={existing_id})")
                skipped += 1
                continue

        query = """
        INSERT INTO artists (id, artist, category, detail, instagram_url, twitter_url, keywords, img_url, debut_date, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        category = VALUES(category),
        detail = VALUES(detail),
        instagram_url = VALUES(instagram_url),
        twitter_url = VALUES(twitter_url),
        keywords = VALUES(keywords),
        img_url = VALUES(img_url),
        debut_date = VALUES(debut_date),
        updated_at = VALUES(updated_at)
        """
        params = (
            artist_id,
            artist_name,
            row.get('category', ''),
            row.get('detail', ''),
            row.get('instagram_url', ''),
            row.get('twitter_url', ''),
            row.get('keywords', ''),
            row.get('img_url', ''),
            row.get('debut_date', ''),
            current_time,
            current_time
        )
        db.cursor.execute(query, params)

    db.commit()
    print(f"✅ artists 테이블 업데이트 완료 (중복 스킵: {skipped}개)")
    return True

def _get_or_create_artist_id(db, artist_name):
    if not artist_name:
        return None
    query = "SELECT id FROM artists WHERE artist = %s"
    db.cursor.execute(query, (artist_name,))
    result = db.cursor.fetchone()
    if result:
        return result[0]

    insert_query = "INSERT INTO artists (artist) VALUES (%s)"
    db.cursor.execute(insert_query, (artist_name,))
    db.commit()
    return db.cursor.lastrowid

def _get_concert_id_by_title(db, concert_title):
    if not concert_title:
        return None
    query = "SELECT id FROM concerts WHERE title = %s"
    db.cursor.execute(query, (concert_title,))
    result = db.cursor.fetchone()
    return result[0] if result else None

def _parse_id(raw):
    val = str(raw).strip()
    return int(float(val)) if val not in ('', 'nan', 'None') else None

def _upsert_concerts(db, df):
    """콘서트 테이블 업서트 및 ID 동기화"""
    try:
        # Step 1: Upsert data to the database
        print("  - Upserting concert data to MySQL...")
        for _, row in df.iterrows():
            artist_name = row.get('artist', '')
            artist_id = _get_or_create_artist_id(db, artist_name)
            concert_id = _parse_id(row.get('id', ''))
            code = row.get('code', '')
            start_date = row.get('start_date', '')

            # artist_id + start_date로 기존 콘서트 조회 (code가 다른 경우도 매칭)
            if artist_id and start_date:
                db.cursor.execute(
                    "SELECT id FROM concerts WHERE artist_id = %s AND start_date = %s LIMIT 1",
                    (artist_id, start_date)
                )
                existing = db.cursor.fetchone()
                if existing and existing[0] != concert_id:
                    existing_id = existing[0]
                    db.cursor.execute("""
                        UPDATE concerts SET
                            code = %s, title = %s, artist = %s, venue = %s, end_date = %s,
                            status = %s, poster = %s, introduction = %s,
                            label = %s, ticket_site = %s, ticket_url = %s
                        WHERE id = %s
                    """, (
                        code, row.get('title', ''), artist_name, row.get('venue', ''),
                        row.get('end_date', ''), row.get('status', ''),
                        row.get('poster', ''), row.get('introduction', ''),
                        row.get('label', ''), row.get('ticket_site', ''),
                        row.get('ticket_url', ''), existing_id
                    ))
                    continue

            query = """
            INSERT INTO concerts (id, code, title, artist, venue, start_date, end_date, status, poster, artist_id, introduction, label, ticket_site, ticket_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            artist = VALUES(artist),
            venue = VALUES(venue),
            start_date = VALUES(start_date),
            end_date = VALUES(end_date),
            status = VALUES(status),
            poster = VALUES(poster),
            artist_id = VALUES(artist_id),
            introduction = VALUES(introduction),
            label = VALUES(label),
            ticket_site = VALUES(ticket_site),
            ticket_url = VALUES(ticket_url)
            """
            db.cursor.execute(query, (
                concert_id,
                code,
                row.get('title', ''),
                artist_name,
                row.get('venue', ''),
                start_date,
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
        print(f"  ✅ concerts 테이블 업데이트 완료")

        # Step 2: Sync back IDs to the local CSV file
        print("  - Syncing back concert IDs to local CSV file...")
        
        codes = df['code'].dropna().unique().tolist()
        if not codes:
            print("  ℹ️ No codes found in the dataframe to sync.")
            return True

        query_format = ','.join(['%s'] * len(codes))
        db.cursor.execute(f"SELECT id, code FROM concerts WHERE code IN ({query_format})", codes)
        
        code_to_id_map = {code: id for id, code in db.cursor.fetchall()}
        
        if not code_to_id_map:
            print("  ⚠️ Could not find any matching IDs in the database for the given codes.")
            return True

        df['id'] = pd.to_numeric(df['code'].map(code_to_id_map), errors='coerce').fillna(0).astype(int)
        
        csv_path = db.get_data_path('concerts.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        print(f"  ✅ Successfully synced back {len(code_to_id_map)} IDs to concerts.csv.")
        return True

    except Exception as e:
        print(f"❌ _upsert_concerts 실패: {e}")
        db.rollback()
        return False

def _upsert_schedule(db, df):
    """스케줄 테이블 업서트"""
    for _, row in df.iterrows():
        concert_id = row.get('concert_id')
        if pd.isna(concert_id):
            print(f"⚠️ concert_id가 비어있어 스킵합니다.")
            continue

        try:
            scheduled_at_value = pd.to_datetime(row.get('scheduled_at'))
        except (ValueError, TypeError):
            print(f"⚠️ 잘못된 날짜 형식, 스킵: {row.get('scheduled_at')}")
            continue

        schedule_id = _parse_id(row.get('id', ''))

        query = """
        INSERT INTO schedule (id, concert_id, category, scheduled_at, type, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            category = VALUES(category),
            scheduled_at = VALUES(scheduled_at),
            type = VALUES(type),
            updated_at = VALUES(updated_at)
        """

        db.cursor.execute(query, (
            schedule_id,
            int(concert_id),
            row.get('category', ''),
            scheduled_at_value,
            row.get('type', ''),
            row.get('updated_at')
        ))
    db.commit()
    print(f"✅ schedule 테이블 업데이트 완료")
    return True

def _upsert_concert_genres(db, df):
    """콘서트-장르 테이블 업서트"""
    skipped = 0
    for _, row in df.iterrows():
        concert_title = row.get('concert_title', '')
        genre_id = row.get('genre_id')

        try:
            genre_id = int(genre_id)
        except (ValueError, TypeError):
            print(f"⚠️ 잘못된 genre_id 형식, 스킵: {genre_id}")
            skipped += 1
            continue

        # concert_title로 현재 DB의 concert_id 조회 (DB마다 ID가 다를 수 있음)
        concert_id = _get_concert_id_by_title(db, concert_title)
        if not concert_id:
            print(f"⚠️ concerts 테이블에서 찾을 수 없음, 스킵: {concert_title}")
            skipped += 1
            continue

        query = """
        INSERT INTO concert_genres (concert_id, genre_id, concert_title, name)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        concert_title = VALUES(concert_title),
        name = VALUES(name)
        """
        db.cursor.execute(query, (concert_id, genre_id, concert_title, row.get('name', '')))

    db.commit()
    print(f"✅ concert_genres 테이블 업데이트 완료 (스킵: {skipped}개)")
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
            _parse_id(row.get('id', '')),
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
            _parse_id(row.get('id', '')),
            row.get('song_order', 0),
            row.get('title', ''),
            row.get('artist', '')
        ))
    
    db.commit()
    print(f"✅ setlists songs 테이블 업데이트 완료")
    return True

ALL_TABLES = [
    ("artists", "artists.csv"),
    ("concerts", "concerts.csv"),
    ("concert_genres", "concert_genres.csv"),
    ("schedule", "schedule.csv"),
    ("songs", "songs.csv"),
    ("setlists", "setlists.csv"),
]

def main():
    """전체 업서트 실행"""

    # 테이블 선택
    print("업서트할 테이블을 선택하세요.")
    for i, (table_name, _) in enumerate(ALL_TABLES, 1):
        print(f"  {i}. {table_name}")
    print(f"  {len(ALL_TABLES) + 1}. 전체")

    while True:
        table_input = input("선택 (번호, 쉼표로 여러 개 가능): ").strip()
        try:
            choices = [int(x.strip()) for x in table_input.split(',')]
            if all(1 <= c <= len(ALL_TABLES) + 1 for c in choices):
                break
        except ValueError:
            pass
        print(f"1~{len(ALL_TABLES) + 1} 사이 숫자를 입력해주세요.")

    if len(ALL_TABLES) + 1 in choices:
        tables = ALL_TABLES
    else:
        tables = [ALL_TABLES[c - 1] for c in choices]

    print("어느 DB에 업서트할까요?")
    print("  1. Dev DB")
    print("  2. Livith DB (프로덕션)")
    print("  3. Stage DB (livith_stage)")
    print("  4. 전체 (Dev + Stage + 프로덕션)")

    while True:
        choice = input("선택 (1/2/3/4): ").strip()
        if choice in ("1", "2", "3", "4"):
            break
        print("1, 2, 3, 4 중에 입력해주세요.")

    targets = []
    if choice in ("1", "4"):
        targets.append(("개발", get_dev_db_manager))
    if choice in ("3", "4"):
        targets.append(("스테이지", get_stage_db_manager))
    if choice in ("2", "4"):
        targets.append(("프로덕션", get_db_manager))

    for label, db_factory in targets:
        print(f"\n🚀 [{label}] CSV → MySQL 업서트 시작")
        for table_name, csv_file in tables:
            if not upsert_table(table_name, csv_file, db=db_factory()):
                print(f"❌ [{label}] {table_name} 업서트 실패")
                return False
        print(f"✅ [{label}] 모든 테이블 업서트 완료!")

    return True


if __name__ == "__main__":
    main()
