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
        """artists.csv → artists 테이블 (UPSERT)"""
        try:
            print("\n📁 artists.csv UPSERT 중...")
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/artists.csv", encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # UPSERT 쿼리 (artist 이름이 같으면 UPDATE, 없으면 INSERT)
            upsert_query = """
                INSERT INTO artists (artist, debut_date, category, detail, 
                                   instagram_url, keywords, img_url, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    debut_date = VALUES(debut_date),
                    category = VALUES(category),
                    detail = VALUES(detail),
                    instagram_url = VALUES(instagram_url),
                    keywords = VALUES(keywords),
                    img_url = VALUES(img_url),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            
            for _, row in df.iterrows():
                data_to_upsert.append((
                    row['artist'],
                    row.get('debut_date', ''),
                    row.get('category', ''),
                    row.get('detail', ''),
                    row.get('instagram_url', ''),
                    row.get('keywords', ''),
                    row.get('img_url', ''),
                    current_time,
                    current_time
                ))
            
            self.cursor.executemany(upsert_query, data_to_upsert)
            self.connection.commit()
            
            print(f"  ✅ artists 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            return True
            
        except Exception as e:
            print(f"  ❌ artists UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concerts(self):
        """concerts.csv → concerts 테이블 (UPSERT)"""
        try:
            print("\n🎪 concerts.csv UPSERT 중...")
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/concerts.csv", encoding='utf-8')
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
                        row.get('poster', ''),          # poster
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
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/songs.csv", encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # 기존 레코드 확인을 위한 SELECT 쿼리
            select_query = "SELECT id FROM songs WHERE title = %s AND artist = %s LIMIT 1"
            
            # UPDATE 쿼리
            update_query = """
                UPDATE songs 
                SET lyrics = %s,
                    pronunciation = %s,
                    translation = %s,
                    youtube_id = %s,
                    updated_at = %s
                WHERE title = %s AND artist = %s
            """
            
            # INSERT 쿼리
            insert_query = """
                INSERT INTO songs (title, artist, lyrics, pronunciation, translation, youtube_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            current_time = datetime.now()
            update_count = 0
            insert_count = 0
            
            for _, row in df.iterrows():
                # 기존 레코드 확인
                self.cursor.execute(select_query, (row['title'], row['artist']))
                existing = self.cursor.fetchone()
                
                if existing:
                    # UPDATE
                    self.cursor.execute(update_query, (
                        row.get('lyrics', ''),
                        row.get('pronunciation', ''),
                        row.get('translation', ''),
                        row.get('youtube_id', ''),
                        current_time,
                        row['title'],
                        row['artist']
                    ))
                    update_count += 1
                else:
                    # INSERT
                    self.cursor.execute(insert_query, (
                        row['title'],
                        row['artist'],
                        row.get('lyrics', ''),
                        row.get('pronunciation', ''),
                        row.get('translation', ''),
                        row.get('youtube_id', ''),
                        current_time,
                        current_time
                    ))
                    insert_count += 1
            
            self.connection.commit()
            
            print(f"  ✅ songs 테이블 처리 완료")
            print(f"     • 업데이트: {update_count}개")
            print(f"     • 신규 추가: {insert_count}개")
            return True
            
        except Exception as e:
            print(f"  ❌ songs UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_setlists(self):
        """setlists.csv → setlists 테이블 (UPSERT)"""
        try:
            print("\n📋 setlists.csv UPSERT 중...")
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/setlists.csv", encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # UPSERT 쿼리 (title로 중복 체크)
            upsert_query = """
                INSERT INTO setlists (title, artist, start_date, end_date, venue, img_url, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    artist = VALUES(artist),
                    start_date = VALUES(start_date),
                    end_date = VALUES(end_date),
                    venue = VALUES(venue),
                    img_url = VALUES(img_url),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            
            for _, row in df.iterrows():
                data_to_upsert.append((
                    row['title'],
                    row['artist'],
                    row['start_date'],
                    row['end_date'],
                    row.get('venue', ''),
                    row.get('img_url', ''),
                    current_time,
                    current_time
                ))
            
            self.cursor.executemany(upsert_query, data_to_upsert)
            self.connection.commit()
            
            print(f"  ✅ setlists 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            return True
            
        except Exception as e:
            print(f"  ❌ setlists UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concert_setlists(self):
        """concert_setlists.csv → concert_setlists 테이블 (UPSERT)"""
        try:
            print("\n🎼 concert_setlists.csv UPSERT 중...")
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/concert_setlists.csv", encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑 (concert_title -> concert.id)
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # setlist_id 매핑 (setlist_title -> setlist.id)
            self.cursor.execute("SELECT id, title FROM setlists")
            setlist_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # UPSERT 쿼리
            upsert_query = """
                INSERT INTO concert_setlists (concert_id, setlist_id, type, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    type = VALUES(type),
                    status = VALUES(status),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            unmapped_items = []
            
            for _, row in df.iterrows():
                concert_title = row.get('concert_title', '')
                setlist_title = row.get('setlist_title', '')
                concert_id = concert_mapping.get(concert_title)
                setlist_id = setlist_mapping.get(setlist_title)
                
                if concert_id and setlist_id:
                    data_to_upsert.append((
                        concert_id,
                        setlist_id,
                        row.get('type', ''),
                        row.get('status', ''),
                        current_time,
                        current_time
                    ))
                else:
                    if not concert_id:
                        unmapped_items.append(f"콘서트 '{concert_title}'")
                    if not setlist_id:
                        unmapped_items.append(f"셋리스트 '{setlist_title}'")
            
            if unmapped_items:
                print(f"  ⚠️ 매핑되지 않은 항목 ({len(unmapped_items)}개):")
                for item in unmapped_items[:3]:  # 처음 3개만 표시
                    print(f"     • {item}")
                if len(unmapped_items) > 3:
                    print(f"     • ... 외 {len(unmapped_items) - 3}개")
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ concert_setlists 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            else:
                print("  ⚠️ 업로드할 데이터가 없습니다.")
            
            return True
            
        except Exception as e:
            print(f"  ❌ concert_setlists UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_setlist_songs(self):
        """setlist_songs.csv → setlist_songs 테이블 (UPSERT)"""
        try:
            print("\n🎶 setlist_songs.csv UPSERT 중...")
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/setlist_songs.csv", encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # setlist_id 매핑 (setlist_title -> setlist.id)
            self.cursor.execute("SELECT id, title FROM setlists")
            setlist_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # song_id 매핑 (song_title -> song.id)
            self.cursor.execute("SELECT id, title FROM songs")
            song_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # UPSERT 쿼리
            upsert_query = """
                INSERT INTO setlist_songs (setlist_id, song_id, song_title, setlist_date, order_index, fanchant, fanchant_point, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    song_title = VALUES(song_title),
                    setlist_date = VALUES(setlist_date),
                    order_index = VALUES(order_index),
                    fanchant = VALUES(fanchant),
                    fanchant_point = VALUES(fanchant_point),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            unmapped_items = []
            
            for _, row in df.iterrows():
                setlist_title = row.get('setlist_title', '')
                song_title = row.get('song_title', '')
                setlist_id = setlist_mapping.get(setlist_title)
                song_id = song_mapping.get(song_title)
                
                if setlist_id and song_id:
                    data_to_upsert.append((
                        setlist_id,
                        song_id,
                        song_title,
                        row.get('setlist_date', ''),
                        row.get('order_index', 0) if row.get('order_index', '') else 0,
                        row.get('fanchant', ''),
                        row.get('fanchant_point', ''),
                        current_time,
                        current_time
                    ))
                else:
                    if not setlist_id:
                        unmapped_items.append(f"셋리스트 '{setlist_title}'")
                    if not song_id:
                        unmapped_items.append(f"곡 '{song_title}'")
            
            if unmapped_items:
                print(f"  ⚠️ 매핑되지 않은 항목 ({len(unmapped_items)}개):")
                for item in unmapped_items[:3]:  # 처음 3개만 표시
                    print(f"     • {item}")
                if len(unmapped_items) > 3:
                    print(f"     • ... 외 {len(unmapped_items) - 3}개")
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ setlist_songs 테이블에 {len(data_to_upsert)}개 UPSERT 완료")
            else:
                print("  ⚠️ 업로드할 데이터가 없습니다.")
            
            return True
            
        except Exception as e:
            print(f"  ❌ setlist_songs UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concert_info(self):
        """concert_info.csv → concert_info 테이블 (UPSERT)"""
        try:
            print("\n📄 concert_info.csv UPSERT 중...")
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/concert_info.csv", encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑 (concert_title -> concert.id)
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # 기존 레코드 확인을 위한 SELECT 쿼리
            select_query = "SELECT id FROM concert_info WHERE concert_id = %s AND category = %s LIMIT 1"
            
            # UPDATE 쿼리
            update_query = """
                UPDATE concert_info 
                SET content = %s,
                    img_url = %s,
                    updated_at = %s
                WHERE concert_id = %s AND category = %s
            """
            
            # INSERT 쿼리
            insert_query = """
                INSERT INTO concert_info (concert_id, category, content, img_url, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            current_time = datetime.now()
            update_count = 0
            insert_count = 0
            unmapped_concerts = []
            
            for _, row in df.iterrows():
                concert_title = row.get('concert_title', '')
                concert_id = concert_mapping.get(concert_title)
                
                if not concert_id:
                    if concert_title not in unmapped_concerts:
                        unmapped_concerts.append(concert_title)
                    continue
                
                # 기존 레코드 확인
                self.cursor.execute(select_query, (concert_id, row.get('category', '')))
                existing = self.cursor.fetchone()
                
                if existing:
                    # UPDATE
                    self.cursor.execute(update_query, (
                        row.get('content', ''),
                        row.get('img_url', ''),
                        current_time,
                        concert_id,
                        row.get('category', '')
                    ))
                    update_count += 1
                else:
                    # INSERT
                    self.cursor.execute(insert_query, (
                        concert_id,
                        row.get('category', ''),
                        row.get('content', ''),
                        row.get('img_url', ''),
                        current_time,
                        current_time
                    ))
                    insert_count += 1
            
            if unmapped_concerts:
                print(f"  ⚠️ 매핑되지 않은 콘서트 ({len(unmapped_concerts)}개):")
                for concert in unmapped_concerts[:3]:
                    print(f"     • {concert}")
                if len(unmapped_concerts) > 3:
                    print(f"     • ... 외 {len(unmapped_concerts) - 3}개")
            
            self.connection.commit()
            
            print(f"  ✅ concert_info 테이블 처리 완료")
            print(f"     • 업데이트: {update_count}개")
            print(f"     • 신규 추가: {insert_count}개")
            
            return True
            
        except Exception as e:
            print(f"  ❌ concert_info UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_cultures(self):
        """cultures.csv → cultures 테이블 (UPSERT)"""
        try:
            print("\n🎭 cultures.csv UPSERT 중...")
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/cultures.csv", encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑 (concert_title -> concert.id)
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # 기존 레코드 확인을 위한 SELECT 쿼리
            select_query = "SELECT id FROM cultures WHERE concert_id = %s AND title = %s LIMIT 1"
            
            # UPDATE 쿼리
            update_query = """
                UPDATE cultures 
                SET content = %s,
                    img_url = %s,
                    updated_at = %s
                WHERE concert_id = %s AND title = %s
            """
            
            # INSERT 쿼리
            insert_query = """
                INSERT INTO cultures (concert_id, title, content, img_url, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            current_time = datetime.now()
            update_count = 0
            insert_count = 0
            unmapped_concerts = []
            
            for _, row in df.iterrows():
                concert_title = row.get('concert_title', '')
                concert_id = concert_mapping.get(concert_title)
                
                if not concert_id:
                    if concert_title not in unmapped_concerts:
                        unmapped_concerts.append(concert_title)
                    continue
                
                # 기존 레코드 확인
                self.cursor.execute(select_query, (concert_id, row.get('title', '')))
                existing = self.cursor.fetchone()
                
                if existing:
                    # UPDATE
                    self.cursor.execute(update_query, (
                        row.get('content', ''),
                        row.get('img_url', ''),
                        current_time,
                        concert_id,
                        row.get('title', '')
                    ))
                    update_count += 1
                else:
                    # INSERT
                    self.cursor.execute(insert_query, (
                        concert_id,
                        row.get('title', ''),
                        row.get('content', ''),
                        row.get('img_url', ''),
                        current_time,
                        current_time
                    ))
                    insert_count += 1
            
            if unmapped_concerts:
                print(f"  ⚠️ 매핑되지 않은 콘서트 ({len(unmapped_concerts)}개):")
                for concert in unmapped_concerts[:3]:
                    print(f"     • {concert}")
                if len(unmapped_concerts) > 3:
                    print(f"     • ... 외 {len(unmapped_concerts) - 3}개")
            
            self.connection.commit()
            
            print(f"  ✅ cultures 테이블 처리 완료")
            print(f"     • 업데이트: {update_count}개")
            print(f"     • 신규 추가: {insert_count}개")
            return True
            
        except Exception as e:
            print(f"  ❌ cultures UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_schedule(self):
        """schedule.csv → schedule 테이블 (UPSERT)"""
        try:
            print("\n📅 schedule.csv UPSERT 중...")
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/schedule.csv", encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑 (concert_title -> concert.id)
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # 기존 레코드 확인을 위한 SELECT 쿼리
            select_query = "SELECT id FROM schedule WHERE concert_id = %s AND category = %s LIMIT 1"
            
            # UPDATE 쿼리
            update_query = """
                UPDATE schedule 
                SET scheduled_at = %s,
                    updated_at = %s
                WHERE concert_id = %s AND category = %s
            """
            
            # INSERT 쿼리
            insert_query = """
                INSERT INTO schedule (concert_id, category, scheduled_at, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            current_time = datetime.now()
            update_count = 0
            insert_count = 0
            unmapped_concerts = []
            
            for _, row in df.iterrows():
                concert_title = row.get('concert_title', '')
                concert_id = concert_mapping.get(concert_title)
                
                if not concert_id:
                    if concert_title not in unmapped_concerts:
                        unmapped_concerts.append(concert_title)
                    continue
                
                # 기존 레코드 확인
                self.cursor.execute(select_query, (concert_id, row.get('category', '')))
                existing = self.cursor.fetchone()
                
                if existing:
                    # UPDATE
                    self.cursor.execute(update_query, (
                        row.get('scheduled_at', ''),
                        current_time,
                        concert_id,
                        row.get('category', '')
                    ))
                    update_count += 1
                else:
                    # INSERT
                    self.cursor.execute(insert_query, (
                        concert_id,
                        row.get('category', ''),
                        row.get('scheduled_at', ''),
                        current_time,
                        current_time
                    ))
                    insert_count += 1
            
            if unmapped_concerts:
                print(f"  ⚠️ 매핑되지 않은 콘서트 ({len(unmapped_concerts)}개):")
                for concert in unmapped_concerts[:3]:
                    print(f"     • {concert}")
                if len(unmapped_concerts) > 3:
                    print(f"     • ... 외 {len(unmapped_concerts) - 3}개")
            
            self.connection.commit()
            
            print(f"  ✅ schedule 테이블 처리 완료")
            print(f"     • 업데이트: {update_count}개")
            print(f"     • 신규 추가: {insert_count}개")
            return True
            
        except Exception as e:
            print(f"  ❌ schedule UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_md(self):
        """md.csv → md 테이블 (UPSERT)"""
        try:
            print("\n🛍️ md.csv UPSERT 중...")
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/md.csv", encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑 (concert_title -> concert.id)
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # 기존 레코드 확인을 위한 SELECT 쿼리
            select_query = "SELECT id FROM md WHERE concert_id = %s AND name = %s LIMIT 1"
            
            # UPDATE 쿼리
            update_query = """
                UPDATE md 
                SET price = %s,
                    img_url = %s,
                    updated_at = %s
                WHERE concert_id = %s AND name = %s
            """
            
            # INSERT 쿼리
            insert_query = """
                INSERT INTO md (concert_id, name, price, img_url, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            current_time = datetime.now()
            update_count = 0
            insert_count = 0
            unmapped_concerts = []
            
            for _, row in df.iterrows():
                concert_title = row.get('concert_title', '')
                concert_id = concert_mapping.get(concert_title)
                
                if not concert_id:
                    if concert_title not in unmapped_concerts:
                        unmapped_concerts.append(concert_title)
                    continue
                
                # 기존 레코드 확인
                self.cursor.execute(select_query, (concert_id, row.get('name', '')))
                existing = self.cursor.fetchone()
                
                if existing:
                    # UPDATE
                    self.cursor.execute(update_query, (
                        row.get('price', ''),
                        row.get('img_url', ''),
                        current_time,
                        concert_id,
                        row.get('name', '')
                    ))
                    update_count += 1
                else:
                    # INSERT
                    self.cursor.execute(insert_query, (
                        concert_id,
                        row.get('name', ''),
                        row.get('price', ''),
                        row.get('img_url', ''),
                        current_time,
                        current_time
                    ))
                    insert_count += 1
            
            if unmapped_concerts:
                print(f"  ⚠️ 매핑되지 않은 콘서트 ({len(unmapped_concerts)}개):")
                for concert in unmapped_concerts[:3]:
                    print(f"     • {concert}")
                if len(unmapped_concerts) > 3:
                    print(f"     • ... 외 {len(unmapped_concerts) - 3}개")
            
            self.connection.commit()
            
            print(f"  ✅ md 테이블 처리 완료")
            print(f"     • 업데이트: {update_count}개")
            print(f"     • 신규 추가: {insert_count}개")
            return True
            
        except Exception as e:
            print(f"  ❌ md UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concert_genres(self):
        """concert_genres.csv → concert_genres 테이블 (UPSERT)"""
        try:
            print("\n🎭 concert_genres.csv UPSERT 중...")
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/concert_genres.csv", encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑 (concert_title -> concert.id)
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # 기존 레코드 확인을 위한 SELECT 쿼리
            select_query = "SELECT id FROM concert_genres WHERE concert_id = %s AND genre_id = %s LIMIT 1"
            
            # UPDATE 쿼리
            update_query = """
                UPDATE concert_genres 
                SET name = %s,
                    updated_at = %s
                WHERE concert_id = %s AND genre_id = %s
            """
            
            # INSERT 쿼리
            insert_query = """
                INSERT INTO concert_genres (concert_id, genre_id, name, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            current_time = datetime.now()
            update_count = 0
            insert_count = 0
            unmapped_concerts = []
            
            for _, row in df.iterrows():
                concert_title = row.get('concert_title', '')
                concert_id = concert_mapping.get(concert_title)
                
                if not concert_id:
                    if concert_title not in unmapped_concerts:
                        unmapped_concerts.append(concert_title)
                    continue
                
                # 기존 레코드 확인
                genre_id = row.get('genre_id', '')
                if not genre_id:
                    continue
                    
                self.cursor.execute(select_query, (concert_id, genre_id))
                existing = self.cursor.fetchone()
                
                if existing:
                    # UPDATE
                    self.cursor.execute(update_query, (
                        row.get('name', ''),
                        current_time,
                        concert_id,
                        genre_id
                    ))
                    update_count += 1
                else:
                    # INSERT
                    self.cursor.execute(insert_query, (
                        concert_id,
                        genre_id,
                        row.get('name', ''),
                        current_time,
                        current_time
                    ))
                    insert_count += 1
            
            if unmapped_concerts:
                print(f"  ⚠️ 매핑되지 않은 콘서트 ({len(unmapped_concerts)}개):")
                for concert in unmapped_concerts[:3]:
                    print(f"     • {concert}")
                if len(unmapped_concerts) > 3:
                    print(f"     • ... 외 {len(unmapped_concerts) - 3}개")
            
            self.connection.commit()
            
            print(f"  ✅ concert_genres 테이블 처리 완료")
            print(f"     • 업데이트: {update_count}개")
            print(f"     • 신규 추가: {insert_count}개")
            return True
            
        except Exception as e:
            print(f"  ❌ concert_genres UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_all_tables(self):
        """모든 테이블 UPSERT"""
        try:
            print("🚀 모든 CSV 데이터 UPSERT 시작 (삭제 없이 업데이트)")
            print("="*80)
            
            # UPSERT 순서 (Foreign Key 의존성 고려)
            upsert_steps = [
                ("Artists", self.upsert_artists),
                ("Concerts", self.upsert_concerts),
                ("Songs", self.upsert_songs),
                ("Setlists", self.upsert_setlists),
                ("Setlist Songs", self.upsert_setlist_songs),
                ("Concert Setlists", self.upsert_concert_setlists),
                ("Concert Info", self.upsert_concert_info),
                ("Cultures", self.upsert_cultures),
                ("Schedule", self.upsert_schedule),
                ("Merchandise", self.upsert_md),
                ("Concert Genres", self.upsert_concert_genres),
                # TODO: 나머지 테이블들 추가
            ]
            
            for step_name, step_function in upsert_steps:
                print(f"\n🔄 {step_name} UPSERT...")
                if not step_function():
                    print(f"❌ {step_name} UPSERT 실패")
                    return False
            
            print("\n" + "="*80)
            print("🎉 모든 데이터 UPSERT 완료!")
            
            # 결과 확인
            self.verify_upsert_results()
            return True
            
        except Exception as e:
            print(f"❌ UPSERT 중 오류: {e}")
            return False

    def verify_upsert_results(self):
        """UPSERT 결과 확인"""
        try:
            print("\n📊 UPSERT 결과 확인:")
            
            tables = ['artists', 'concerts', 'songs', 'setlists', 'setlist_songs', 'concert_setlists', 'concert_info', 'cultures', 'schedule', 'merchandise', 'concert_genres']
            for table in tables:
                try:
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = self.cursor.fetchone()[0]
                    print(f"  • {table}: {count:,}개 레코드")
                except:
                    print(f"  • {table}: 확인 불가")
                    
        except Exception as e:
            print(f"❌ 검증 실패: {e}")

    def close(self):
        """연결 종료"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        if self.ssh_process:
            try:
                os.killpg(os.getpgid(self.ssh_process.pid), signal.SIGTERM)
                self.ssh_process.wait(timeout=5)
            except:
                try:
                    os.killpg(os.getpgid(self.ssh_process.pid), signal.SIGKILL)
                except:
                    pass
        print("🔌 연결 종료")

def main():
    """메인 실행"""
    upserter = UpsertCSVToMySQL()
    
    try:
        # SSH 터널 생성
        if not upserter.create_ssh_tunnel():
            return
        
        # MySQL 연결
        if not upserter.connect_mysql():
            return
        
        # 모든 데이터 UPSERT
        upserter.upsert_all_tables()
        
    except KeyboardInterrupt:
        print("\n⏹️ 사용자 중단")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        upserter.close()

if __name__ == "__main__":
    main()