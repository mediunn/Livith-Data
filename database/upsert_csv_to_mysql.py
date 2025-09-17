#!/usr/bin/env python3
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import signal
import os
from datetime import datetime

class UpsertCSVToMySQL:
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None
        self.csv_base_path = '/Users/youz2me/Xcode/Livith-Data/output/main_output'

    def create_ssh_tunnel(self):
        """SSH 터널 생성"""
        try:
            print("🔧 SSH 터널 생성 중...")
            
            ssh_command = [
                'ssh',
                '-i', '/Users/youz2me/Downloads/livith-key.pem',
                '-L', '3307:livithdb.c142i2022qs5.ap-northeast-2.rds.amazonaws.com:3306',
                '-N',
                '-o', 'StrictHostKeyChecking=no',
                'ubuntu@43.203.48.65'
            ]
            
            self.ssh_process = subprocess.Popen(
                ssh_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid
            )
            
            time.sleep(3)
            
            if self.ssh_process.poll() is None:
                print("✅ SSH 터널 생성 완료!")
                return True
            else:
                return False
                
        except Exception as e:
            print(f"❌ SSH 터널 오류: {e}")
            return False

    def connect_mysql(self):
        """MySQL 연결"""
        try:
            print("🔌 MySQL 연결 중...")
            
            config = {
                'host': '127.0.0.1',
                'port': 3307,
                'user': 'root',
                'password': 'livith0407',
                'database': 'livith_v3',
                'charset': 'utf8mb4',
                'use_unicode': True
            }
            
            self.connection = mysql.connector.connect(**config)
            self.cursor = self.connection.cursor()
            
            print("✅ MySQL 연결 성공!")
            return True
            
        except Error as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False

    def upsert_artists(self):
        """artists.csv → artist 테이블 (UPSERT)"""
        try:
            print("\n📁 artists.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/artists.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ artists.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # 기존 아티스트 목록 가져오기
            self.cursor.execute("SELECT artist FROM artists")
            existing_artists = {artist[0] for artist in self.cursor.fetchall()}
            
            current_time = datetime.now()
            update_count = 0
            insert_count = 0
            
            for _, row in df.iterrows():
                artist_name = row['artist']
                
                if artist_name in existing_artists:
                    # 이미 존재하는 아티스트는 UPDATE (첫 번째 것만)
                    update_query = """
                        UPDATE artists 
                        SET debut_date = %s, category = %s, detail = %s,
                            instagram_url = %s, keywords = %s, img_url = %s, 
                            updated_at = %s
                        WHERE artist = %s AND id = (
                            SELECT * FROM (
                                SELECT MIN(id) FROM artists WHERE artist = %s
                            ) as temp
                        )
                    """
                    self.cursor.execute(update_query, (
                        row.get('debut_date', ''),
                        row.get('group_type', ''),
                        row.get('introduction', ''),
                        row.get('social_media', ''),
                        row.get('keywords', ''),
                        row.get('img_url', ''),
                        current_time,
                        artist_name,
                        artist_name
                    ))
                    if self.cursor.rowcount > 0:
                        update_count += 1
                else:
                    # 새로운 아티스트는 INSERT
                    insert_query = """
                        INSERT INTO artists (artist, debut_date, category, detail, 
                                           instagram_url, keywords, img_url, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        artist_name,
                        row.get('debut_date', ''),
                        row.get('group_type', ''),
                        row.get('introduction', ''),
                        row.get('social_media', ''),
                        row.get('keywords', ''),
                        row.get('img_url', ''),
                        current_time,
                        current_time
                    ))
                    existing_artists.add(artist_name)  # 목록에 추가
                    insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ artists 테이블에 {update_count}개 업데이트, {insert_count}개 삽입 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ artists UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concerts(self):
        """concerts.csv → concerts 테이블 (UPSERT)"""
        try:
            print("\n🎪 concerts.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/concerts.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ concerts.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # artist_id 매핑
            self.cursor.execute("SELECT id, artist FROM artists")
            artist_mapping = {artist: id for id, artist in self.cursor.fetchall()}
            
            # UPSERT 쿼리 (title이 같으면 UPDATE, 없으면 INSERT)
            upsert_query = """
                INSERT INTO concerts (
                    title, artist, artist_id, start_date, end_date, 
                    status, poster, code, ticket_site, 
                    ticket_url, venue, label, introduction, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    artist = VALUES(artist),
                    artist_id = VALUES(artist_id),
                    start_date = VALUES(start_date),
                    end_date = VALUES(end_date),
                    status = VALUES(status),
                    poster = VALUES(poster),
                    code = VALUES(code),
                    ticket_site = VALUES(ticket_site),
                    ticket_url = VALUES(ticket_url),
                    venue = VALUES(venue),
                    label = VALUES(label),
                    introduction = VALUES(introduction),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            unmapped_artists = []
            
            for _, row in df.iterrows():
                artist_name = row['artist']
                artist_id = artist_mapping.get(artist_name)
                
                if artist_id:
                    data_to_upsert.append((
                        row['title'],                    # title (UNIQUE KEY)
                        artist_name,                     # artist
                        artist_id,                       # artist_id
                        row['start_date'],              # start_date
                        row['end_date'],                # end_date
                        row['status'],                  # status
                        row.get('img_url', ''),         # poster (CSV의 img_url을 poster에)
                        row.get('code', ''),            # code
                        row.get('ticket_site', ''),     # ticket_site
                        row.get('ticket_url', ''),      # ticket_url
                        row.get('venue', ''),           # venue
                        row.get('label', ''),           # label
                        row.get('introduction', ''),    # introduction
                        current_time,                   # created_at
                        current_time                    # updated_at
                    ))
                else:
                    unmapped_artists.append(artist_name)
            
            if unmapped_artists:
                print(f"  ⚠️ 매핑되지 않은 아티스트 ({len(unmapped_artists)}개):")
                for artist in unmapped_artists[:3]:
                    print(f"     • {artist}")
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ concerts 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ concerts UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_songs(self):
        """songs.csv → songs 테이블 (UPSERT)"""
        try:
            print("\n🎵 songs.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/songs.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ songs.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            upserted_count = 0
            
            for _, row in df.iterrows():
                title = row['title']
                artist = row['artist']
                
                # 먼저 해당 곡이 존재하는지 확인 (title + artist 조합)
                self.cursor.execute("SELECT id FROM songs WHERE title = %s AND artist = %s", (title, artist))
                existing = self.cursor.fetchone()
                # 결과 버퍼 정리
                self.cursor.fetchall()
                
                if existing:
                    # 존재하면 UPDATE
                    update_query = """
                        UPDATE songs 
                        SET lyrics = %s, pronunciation = %s, translation = %s
                        WHERE title = %s AND artist = %s
                    """
                    self.cursor.execute(update_query, (
                        row.get('lyrics', ''),
                        row.get('pronunciation', ''),
                        row.get('translation', ''),
                        title,
                        artist
                    ))
                else:
                    # 존재하지 않으면 INSERT
                    insert_query = """
                        INSERT INTO songs (title, artist, lyrics, pronunciation, translation)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        title,
                        artist,
                        row.get('lyrics', ''),
                        row.get('pronunciation', ''),
                        row.get('translation', '')
                    ))
                
                upserted_count += 1
            
            if upserted_count > 0:
                self.connection.commit()
                print(f"  ✅ songs 테이블에 {upserted_count}개 UPSERT 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ songs UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_setlists(self):
        """setlists.csv → setlists 테이블 (UPSERT)"""
        try:
            print("\n🎤 setlists.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/setlists.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ setlists.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            current_time = datetime.now()
            upserted_count = 0
            
            for _, row in df.iterrows():
                title = row['title']
                artist = row['artist']
                
                # 먼저 해당 세트리스트가 존재하는지 확인 (title + artist 조합)
                self.cursor.execute("SELECT id FROM setlists WHERE title = %s AND artist = %s", (title, artist))
                existing = self.cursor.fetchone()
                # 결과 버퍼 정리
                self.cursor.fetchall()
                
                if existing:
                    # 존재하면 UPDATE
                    update_query = """
                        UPDATE setlists 
                        SET img_url = %s, end_date = %s, start_date = %s, venue = %s, updated_at = %s
                        WHERE title = %s AND artist = %s
                    """
                    self.cursor.execute(update_query, (
                        row.get('img_url', ''),
                        row.get('end_date', ''),
                        row.get('start_date', ''),
                        row.get('venue', ''),
                        current_time,
                        title,
                        artist
                    ))
                else:
                    # 존재하지 않으면 INSERT
                    insert_query = """
                        INSERT INTO setlists (title, artist, img_url, end_date, start_date, venue, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        title,
                        artist,
                        row.get('img_url', ''),
                        row.get('end_date', ''),
                        row.get('start_date', ''),
                        row.get('venue', ''),
                        current_time,
                        current_time
                    ))
                
                upserted_count += 1
            
            if upserted_count > 0:
                self.connection.commit()
                print(f"  ✅ setlists 테이블에 {upserted_count}개 UPSERT 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ setlists UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concert_info(self):
        """concert_info.csv → concert_info 테이블 (UPSERT)"""
        try:
            print("\n📋 concert_info.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/concert_info.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ concert_info.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # UPSERT 쿼리
            upsert_query = """
                INSERT INTO concert_info (concert_id, category, content, img_url, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    content = VALUES(content),
                    img_url = VALUES(img_url),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                if concert_id:
                    data_to_upsert.append((
                        concert_id,
                        row['category'],
                        row.get('content', ''),
                        row.get('img_url', ''),
                        current_time,
                        current_time
                    ))
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ concert_info 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ concert_info UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_cultures(self):
        """cultures.csv → cultures 테이블 (UPSERT)"""
        try:
            print("\n🎭 cultures.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/cultures.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ cultures.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # UPSERT 쿼리
            upsert_query = """
                INSERT INTO cultures (concert_id, title, content, img_url, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    content = VALUES(content),
                    img_url = VALUES(img_url),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                if concert_id:
                    data_to_upsert.append((
                        concert_id,
                        row['title'],
                        row.get('content', ''),
                        row.get('img_url', ''),
                        current_time,
                        current_time
                    ))
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ cultures 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ cultures UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_schedule(self):
        """schedule.csv → schedule 테이블 (UPSERT)"""
        try:
            print("\n📅 schedule.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/schedule.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ schedule.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # 중복 방지를 위한 세트
            processed_items = set()
            insert_count = 0
            duplicate_count = 0
            
            current_time = datetime.now()
            data_to_upsert = []
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                if not concert_id:
                    continue
                    
                category = row['category'][:50] if row['category'] else ''
                if not category:
                    continue
                    
                # scheduled_at 파싱
                scheduled_at_str = str(row.get('scheduled_at', ''))
                
                # 중복 체크 (concert_id + category + scheduled_at)
                item_key = (concert_id, category, scheduled_at_str)
                if item_key in processed_items:
                    duplicate_count += 1
                    continue
                processed_items.add(item_key)
                
                # DB 중복 체크
                self.cursor.execute(
                    "SELECT id FROM schedule WHERE concert_id = %s AND category = %s AND scheduled_at = %s",
                    (concert_id, category, scheduled_at_str)
                )
                existing = self.cursor.fetchone()
                
                if existing:
                    duplicate_count += 1
                    continue
                
                # INSERT
                insert_query = """
                    INSERT INTO schedule (concert_id, category, scheduled_at, type, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_query, (
                    concert_id,
                    category,
                    scheduled_at_str,
                    row.get('type', 'CONCERT'),
                    current_time,
                    current_time
                ))
                insert_count += 1
            
            if insert_count > 0:
                self.connection.commit()
            
            print(f"  ✅ schedule 테이블: {insert_count}개 삽입, {duplicate_count}개 중복 스킵")
            
            return True
            
        except Exception as e:
            print(f"  ❌ schedule UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_md(self):
        """md.csv → md 테이블 (UPSERT)"""
        try:
            print("\n🛍️ md.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/md.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ md.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # 중복 방지를 위한 세트
            processed_items = set()
            insert_count = 0
            duplicate_count = 0
            current_time = datetime.now()
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                if not concert_id:
                    continue
                    
                item_name = row['item_name'][:100] if row['item_name'] else ''
                if not item_name:
                    continue
                
                # 중복 체크 (concert_id + name)
                item_key = (concert_id, item_name)
                if item_key in processed_items:
                    duplicate_count += 1
                    continue
                processed_items.add(item_key)
                
                # DB 중복 체크
                self.cursor.execute(
                    "SELECT id FROM md WHERE concert_id = %s AND name = %s",
                    (concert_id, item_name)
                )
                existing = self.cursor.fetchone()
                
                if existing:
                    duplicate_count += 1
                    continue
                
                # INSERT
                insert_query = """
                    INSERT INTO md (concert_id, name, price, img_url, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_query, (
                    concert_id,
                    item_name,
                    row.get('price', '')[:30],
                    row.get('img_url', '')[:255],
                    current_time,
                    current_time
                ))
                insert_count += 1
            
            if insert_count > 0:
                self.connection.commit()
            
            print(f"  ✅ md 테이블: {insert_count}개 삽입, {duplicate_count}개 중복 스킵")
            
            return True
            
        except Exception as e:
            print(f"  ❌ md UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concert_genres(self):
        """concert_genres.csv → concert_genres 테이블 (UPSERT)"""
        try:
            print("\n🎭 concert_genres.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/concert_genres.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ concert_genres.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑 (concert_title -> concert.id)
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # UPSERT 쿼리
            upsert_query = """
                INSERT INTO concert_genres (concert_id, concert_title, genre_id, name)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    concert_title = VALUES(concert_title),
                    name = VALUES(name)
            """
            
            data_to_upsert = []
            unmapped_concerts = []
            
            for _, row in df.iterrows():
                concert_title = row['concert_title']
                concert_id = concert_mapping.get(concert_title)
                
                if concert_id:
                    data_to_upsert.append((
                        concert_id,
                        concert_title,
                        row.get('genre_id', 1),  # CSV의 genre_id
                        row.get('genre_name', '')  # CSV의 genre_name → MySQL의 name
                    ))
                else:
                    if concert_title not in unmapped_concerts:
                        unmapped_concerts.append(concert_title)
            
            if unmapped_concerts:
                print(f"  ⚠️ 매핑되지 않은 콘서트 ({len(unmapped_concerts)}개):")
                for concert in unmapped_concerts[:3]:
                    print(f"     • {concert}")
                if len(unmapped_concerts) > 3:
                    print(f"     • ... 외 {len(unmapped_concerts) - 3}개")
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ concert_genres 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ concert_genres UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concert_setlists(self):
        """concert_setlists.csv → concert_setlists 테이블 (UPSERT)"""
        try:
            print("\n🎸 concert_setlists.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/concert_setlists.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ concert_setlists.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # setlist_id 매핑
            self.cursor.execute("SELECT id, title FROM setlists")
            setlist_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # UPSERT 쿼리
            upsert_query = """
                INSERT INTO concert_setlists (concert_id, setlist_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            
            for _, row in df.iterrows():
                setlist_id = setlist_mapping.get(row['setlist_title'])
                if setlist_id:
                    data_to_upsert.append((
                        row['concert_id'],
                        setlist_id,
                        current_time,
                        current_time
                    ))
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ concert_setlists 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ concert_setlists UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_home_sections(self):
        """home_sections.csv → home_sections 테이블 (UPSERT)"""
        try:
            print("\n🏠 home_sections.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/home_sections.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ home_sections.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # UPSERT 쿼리
            upsert_query = """
                INSERT INTO home_sections (id, section_title, created_at, updated_at)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    section_title = VALUES(section_title),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            
            for _, row in df.iterrows():
                data_to_upsert.append((
                    row['id'],
                    row['section_title'],
                    row.get('created_at', current_time),
                    row.get('updated_at', current_time)
                ))
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ home_sections 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ home_sections UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_home_concert_sections(self):
        """home_concert_sections.csv → home_concert_sections 테이블 (UPSERT)"""
        try:
            print("\n🏠 home_concert_sections.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/home_concert_sections.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ home_concert_sections.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # UPSERT 쿼리
            upsert_query = """
                INSERT INTO home_concert_sections (home_section_id, concert_id, section_title, concert_title, sorted_index, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    section_title = VALUES(section_title),
                    concert_title = VALUES(concert_title),
                    sorted_index = VALUES(sorted_index),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            
            for _, row in df.iterrows():
                data_to_upsert.append((
                    row['home_section_id'],
                    row['concert_id'],
                    row['section_title'],
                    row['concert_title'],
                    row.get('sorted_index', 0),
                    current_time,
                    current_time
                ))
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ home_concert_sections 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ home_concert_sections UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_search_sections(self):
        """search_sections.csv → search_sections 테이블 (UPSERT)"""
        try:
            print("\n🔍 search_sections.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/search_sections.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ search_sections.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # UPSERT 쿼리
            upsert_query = """
                INSERT INTO search_sections (id, section_title, created_at, updated_at)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    section_title = VALUES(section_title),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            
            for _, row in df.iterrows():
                data_to_upsert.append((
                    row['id'],
                    row['section_title'],
                    row.get('created_at', current_time),
                    row.get('updated_at', current_time)
                ))
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ search_sections 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ search_sections UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_search_concert_sections(self):
        """search_concert_sections.csv → search_concert_sections 테이블 (UPSERT)"""
        try:
            print("\n🔍 search_concert_sections.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/search_concert_sections.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ search_concert_sections.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # UPSERT 쿼리
            upsert_query = """
                INSERT INTO search_concert_sections (search_section_id, concert_id, section_title, concert_title, sorted_index, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    section_title = VALUES(section_title),
                    concert_title = VALUES(concert_title),
                    sorted_index = VALUES(sorted_index),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            
            for _, row in df.iterrows():
                data_to_upsert.append((
                    row['search_section_id'],
                    row['concert_id'],
                    row['section_title'],
                    row['concert_title'],
                    row.get('sorted_index', 0),
                    current_time,
                    current_time
                ))
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ search_concert_sections 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ search_concert_sections UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_setlist_songs(self):
        """setlist_songs.csv → setlist_songs 테이블 (UPSERT)"""
        try:
            print("\n🎵 setlist_songs.csv UPSERT 중...")
            
            # CSV 파일 확인
            csv_path = f"{self.csv_base_path}/setlist_songs.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ setlist_songs.csv 파일이 없습니다.")
                return True
            
            # CSV 읽기
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # setlist_id와 song_id 매핑
            self.cursor.execute("SELECT id, title FROM setlists")
            setlist_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            self.cursor.execute("SELECT id, title FROM songs")
            song_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # UPSERT 쿼리
            upsert_query = """
                INSERT INTO setlist_songs (setlist_id, song_id, order_index, setlist_title, song_title, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    order_index = VALUES(order_index),
                    setlist_title = VALUES(setlist_title),
                    song_title = VALUES(song_title),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            
            # setlist_id로 그룹화된 데이터에서 setlist 정보 찾기
            setlist_groups = df.groupby('setlist_id')
            
            for setlist_csv_id, group in setlist_groups:
                # 첫 번째 행에서 아티스트 정보 가져와서 setlist 제목 생성
                first_row = group.iloc[0]
                artist = first_row['artist']
                # 아티스트 이름으로 setlist 찾기 (예상 셋리스트 형태)
                possible_setlist_titles = [
                    f"{title}" for title in setlist_mapping.keys() 
                    if artist in title or any(part in title for part in artist.split())
                ]
                
                if possible_setlist_titles:
                    setlist_title = possible_setlist_titles[0]  # 첫 번째 매치 사용
                    setlist_id = setlist_mapping[setlist_title]
                    
                    for _, row in group.iterrows():
                        song_id = song_mapping.get(row['title'])
                        if song_id:
                            data_to_upsert.append((
                                setlist_id,
                                song_id,
                                row.get('order', 1),
                                setlist_title,
                                row['title'],
                                current_time,
                                current_time
                            ))
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ setlist_songs 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            else:
                print("  ⚠️ 매핑된 데이터가 없습니다.")
            
            return True
            
        except Exception as e:
            print(f"  ❌ setlist_songs UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def verify_results(self):
        """업로드 결과 확인"""
        try:
            print("\n📊 업로드 결과 확인:")
            
            tables = ['artists', 'concert_genres', 'concerts', 'songs', 'setlists', 
                     'concert_setlists', 'concert_info', 'cultures', 'home_sections', 
                     'home_concert_sections', 'md', 'schedule', 'search_sections', 
                     'search_concert_sections', 'setlist_songs']
            
            for table in tables:
                try:
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = self.cursor.fetchone()[0]
                    print(f"  • {table}: {count:,}개 레코드")
                except:
                    print(f"  • {table}: 확인 불가")
                    
        except Exception as e:
            print(f"❌ 검증 실패: {e}")

    def close_connections(self):
        """연결 종료"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            if self.ssh_process:
                os.killpg(os.getpgid(self.ssh_process.pid), signal.SIGTERM)
            print("\n🔒 모든 연결 종료 완료")
        except Exception as e:
            print(f"⚠️ 연결 종료 중 오류: {e}")

    def run(self):
        """전체 UPSERT 프로세스 실행"""
        try:
            print("\n" + "="*60)
            print("🚀 CSV → MySQL UPSERT 시작")
            print("="*60)
            
            # SSH 터널 생성
            if not self.create_ssh_tunnel():
                print("❌ SSH 터널 생성 실패")
                return
            
            # MySQL 연결
            if not self.connect_mysql():
                print("❌ MySQL 연결 실패")
                return
            
            # 각 테이블 UPSERT (순서 중요)
            self.upsert_artists()
            self.upsert_concert_genres()
            self.upsert_concerts()
            self.upsert_songs()
            self.upsert_setlists()
            self.upsert_concert_setlists()
            self.upsert_concert_info()
            self.upsert_cultures()
            self.upsert_home_sections()
            self.upsert_home_concert_sections()
            self.upsert_md()
            self.upsert_schedule()
            self.upsert_search_sections()
            self.upsert_search_concert_sections()
            self.upsert_setlist_songs()
            
            # 결과 확인
            self.verify_results()
            
            print("\n" + "="*60)
            print("✨ 모든 데이터 UPSERT 완료!")
            print("="*60)
            
        except KeyboardInterrupt:
            print("\n\n⚠️ 사용자에 의해 중단됨")
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    upserter = UpsertCSVToMySQL()
    upserter.run()