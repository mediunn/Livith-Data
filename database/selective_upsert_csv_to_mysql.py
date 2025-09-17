#!/usr/bin/env python3
"""
선택적 CSV to MySQL UPSERT 스크립트 - 모든 테이블 지원
"""
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import signal
import os
from datetime import datetime

class SelectiveUpsertCSVToMySQL:
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
            print(f"❌ SSH 터널 생성 실패: {e}")
            return False

    def connect_mysql(self):
        """MySQL 연결"""
        try:
            print("🔌 MySQL 연결 중...")
            
            self.connection = mysql.connector.connect(
                host='127.0.0.1',
                port=3307,
                user='root',
                password='livith0407',
                database='livith_v3',
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci'
            )
            
            self.cursor = self.connection.cursor()
            self.cursor.execute("SET NAMES utf8mb4")
            self.cursor.execute("SET CHARACTER SET utf8mb4")
            self.cursor.execute("SET character_set_connection=utf8mb4")
            
            print("✅ MySQL 연결 성공!")
            return True
            
        except Error as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False

    def close_connections(self):
        """연결 종료"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        if self.ssh_process:
            os.killpg(os.getpgid(self.ssh_process.pid), signal.SIGTERM)
        print("\n👋 연결 종료 완료")

    def upsert_artists(self):
        """artists.csv → artists 테이블"""
        try:
            print("\n🎨 artists.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/artists.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ artists.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # 중복 방지를 위한 세트
            processed_items = set()
            insert_count = 0
            update_count = 0
            current_time = datetime.now()
            
            for _, row in df.iterrows():
                artist_name = row['artist']
                
                if not artist_name or artist_name in processed_items:
                    continue
                processed_items.add(artist_name)
                
                # DB에서 기존 아티스트 확인
                self.cursor.execute("SELECT id FROM artists WHERE artist = %s", (artist_name,))
                existing = self.cursor.fetchone()
                
                if not existing:
                    # 새 아티스트 삽입 - 모든 필드 포함
                    insert_query = """
                        INSERT INTO artists (
                            artist, category, detail, instagram_url, 
                            keywords, img_url, debut_date, created_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        artist_name,
                        row.get('group_type', ''),
                        row.get('introduction', ''),
                        row.get('social_media', ''),
                        row.get('keywords', ''),
                        row.get('img_url', ''),
                        row.get('debut_date', ''),
                        current_time,
                        current_time
                    ))
                    insert_count += 1
                else:
                    # 기존 아티스트 업데이트 - 모든 필드 업데이트
                    update_query = """
                        UPDATE artists 
                        SET category = %s, detail = %s, instagram_url = %s,
                            keywords = %s, img_url = %s, debut_date = %s, updated_at = %s
                        WHERE artist = %s
                    """
                    self.cursor.execute(update_query, (
                        row.get('group_type', ''),
                        row.get('introduction', ''),
                        row.get('social_media', ''),
                        row.get('keywords', ''),
                        row.get('img_url', ''),
                        row.get('debut_date', ''),
                        current_time,
                        artist_name
                    ))
                    update_count += 1
            
            if insert_count > 0 or update_count > 0:
                self.connection.commit()
            
            print(f"  ✅ artists: {insert_count}개 삽입, {update_count}개 업데이트")
            return True
            
        except Exception as e:
            print(f"  ❌ artists UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concerts(self):
        """concerts.csv → concerts 테이블"""
        try:
            print("\n🎪 concerts.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/concerts.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ concerts.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # artist_id 매핑
            self.cursor.execute("SELECT id, artist FROM artists")
            artist_mapping = {artist: id for id, artist in self.cursor.fetchall()}
            
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
            
            for _, row in df.iterrows():
                artist_name = row['artist']
                artist_id = artist_mapping.get(artist_name)
                
                if artist_id:
                    data_to_upsert.append((
                        row['title'],
                        artist_name,
                        artist_id,
                        row['start_date'],
                        row['end_date'],
                        row['status'],
                        row.get('img_url', ''),
                        row.get('code', ''),
                        row.get('ticket_site', ''),
                        row.get('ticket_url', ''),
                        row.get('venue', ''),
                        row.get('label', ''),
                        row.get('introduction', ''),
                        current_time,
                        current_time
                    ))
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ concerts: {len(data_to_upsert)}개 UPSERT 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ concerts UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_songs(self):
        """songs.csv → songs 테이블"""
        try:
            print("\n🎵 songs.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/songs.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ songs.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            processed_items = set()
            insert_count = 0
            update_count = 0
            
            for _, row in df.iterrows():
                title = row['title']
                artist = row['artist']
                
                if not title or not artist:
                    continue
                
                item_key = (title, artist)
                if item_key in processed_items:
                    continue
                processed_items.add(item_key)
                
                self.cursor.execute(
                    "SELECT id FROM songs WHERE title = %s AND artist = %s", 
                    (title, artist)
                )
                existing = self.cursor.fetchone()
                
                if existing:
                    update_query = """
                        UPDATE songs 
                        SET lyrics = %s, pronunciation = %s, translation = %s, updated_at = %s
                        WHERE title = %s AND artist = %s
                    """
                    self.cursor.execute(update_query, (
                        row.get('lyrics', ''),
                        row.get('pronunciation', ''),
                        row.get('translation', ''),
                        datetime.now(),
                        title,
                        artist
                    ))
                    update_count += 1
                else:
                    insert_query = """
                        INSERT INTO songs (title, artist, lyrics, pronunciation, translation, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    current_time = datetime.now()
                    self.cursor.execute(insert_query, (
                        title,
                        artist,
                        row.get('lyrics', ''),
                        row.get('pronunciation', ''),
                        row.get('translation', ''),
                        current_time,
                        current_time
                    ))
                    insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ songs: {insert_count}개 삽입, {update_count}개 업데이트")
            return True
            
        except Exception as e:
            print(f"  ❌ songs UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concert_info(self):
        """concert_info.csv → concert_info 테이블"""
        try:
            print("\n📋 concert_info.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/concert_info.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ concert_info.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            processed_items = set()
            insert_count = 0
            update_count = 0
            
            for _, row in df.iterrows():
                concert_title = row['concert_title']
                category = row['category']
                
                # 중복 체크
                item_key = (concert_title, category)
                if item_key in processed_items:
                    continue
                processed_items.add(item_key)
                
                concert_id = concert_mapping.get(concert_title)
                if not concert_id:
                    continue
                
                # 기존 레코드 확인
                self.cursor.execute(
                    "SELECT id FROM concert_info WHERE concert_id = %s AND category = %s",
                    (concert_id, category)
                )
                existing = self.cursor.fetchone()
                
                # content를 100자로 제한
                content = str(row.get('content', ''))[:100]
                
                if existing:
                    update_query = """
                        UPDATE concert_info 
                        SET content = %s, updated_at = %s
                        WHERE concert_id = %s AND category = %s
                    """
                    self.cursor.execute(update_query, (
                        content,
                        datetime.now(),
                        concert_id,
                        category
                    ))
                    update_count += 1
                else:
                    insert_query = """
                        INSERT INTO concert_info (concert_id, category, content, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    current_time = datetime.now()
                    self.cursor.execute(insert_query, (
                        concert_id,
                        category,
                        content,
                        current_time,
                        current_time
                    ))
                    insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ concert_info: {insert_count}개 삽입, {update_count}개 업데이트")
            return True
            
        except Exception as e:
            print(f"  ❌ concert_info UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_genres(self):
        """genres.csv → genres 테이블"""
        try:
            print("\n🎸 genres.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/genres.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ genres.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            upsert_query = """
                INSERT INTO genres (name, created_at, updated_at)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = [(row['name'], current_time, current_time) for _, row in df.iterrows()]
            
            self.cursor.executemany(upsert_query, data_to_upsert)
            self.connection.commit()
            
            print(f"  ✅ genres: {len(data_to_upsert)}개 UPSERT 완료")
            return True
            
        except Exception as e:
            print(f"  ❌ genres UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concert_genres(self):
        """concert_genres.csv → concert_genres 테이블"""
        try:
            print("\n🎼 concert_genres.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/concert_genres.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ concert_genres.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id와 genre_id 매핑
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            self.cursor.execute("SELECT id, name FROM genres")
            genre_mapping = {name: id for id, name in self.cursor.fetchall()}
            
            processed_items = set()
            insert_count = 0
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                genre_id = genre_mapping.get(row['genre_name'])
                
                if not concert_id or not genre_id:
                    continue
                
                item_key = (concert_id, genre_id)
                if item_key in processed_items:
                    continue
                processed_items.add(item_key)
                
                # 기존 레코드 확인
                self.cursor.execute(
                    "SELECT id FROM concert_genres WHERE concert_id = %s AND genre_id = %s",
                    (concert_id, genre_id)
                )
                if not self.cursor.fetchone():
                    insert_query = """
                        INSERT INTO concert_genres (concert_id, genre_id, created_at, updated_at)
                        VALUES (%s, %s, %s, %s)
                    """
                    current_time = datetime.now()
                    self.cursor.execute(insert_query, (concert_id, genre_id, current_time, current_time))
                    insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ concert_genres: {insert_count}개 삽입")
            return True
            
        except Exception as e:
            print(f"  ❌ concert_genres UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_setlists(self):
        """setlists.csv → setlists 테이블"""
        try:
            print("\n🎤 setlists.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/setlists.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ setlists.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            upsert_query = """
                INSERT INTO setlists (title, artist, img_url, end_date, start_date, venue, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = [
                (
                    row['title'], 
                    row.get('artist', ''), 
                    row.get('img_url', ''), 
                    row.get('end_date', ''), 
                    row.get('start_date', ''), 
                    row.get('venue', ''), 
                    current_time, 
                    current_time
                ) for _, row in df.iterrows()
            ]
            
            self.cursor.executemany(upsert_query, data_to_upsert)
            self.connection.commit()
            
            print(f"  ✅ setlists: {len(data_to_upsert)}개 UPSERT 완료")
            return True
            
        except Exception as e:
            print(f"  ❌ setlists UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concert_setlists(self):
        """concert_setlists.csv → concert_setlists 테이블"""
        try:
            print("\n🎶 concert_setlists.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/concert_setlists.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ concert_setlists.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id와 setlist_id 매핑
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            self.cursor.execute("SELECT id, name FROM setlists")
            setlist_mapping = {name: id for id, name in self.cursor.fetchall()}
            
            processed_items = set()
            insert_count = 0
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                setlist_id = setlist_mapping.get(row['setlist_name'])
                
                if not concert_id or not setlist_id:
                    continue
                
                item_key = (concert_id, setlist_id)
                if item_key in processed_items:
                    continue
                processed_items.add(item_key)
                
                # 기존 레코드 확인
                self.cursor.execute(
                    "SELECT id FROM concert_setlists WHERE concert_id = %s AND setlist_id = %s",
                    (concert_id, setlist_id)
                )
                if not self.cursor.fetchone():
                    insert_query = """
                        INSERT INTO concert_setlists (concert_id, setlist_id, created_at, updated_at)
                        VALUES (%s, %s, %s, %s)
                    """
                    current_time = datetime.now()
                    self.cursor.execute(insert_query, (concert_id, setlist_id, current_time, current_time))
                    insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ concert_setlists: {insert_count}개 삽입")
            return True
            
        except Exception as e:
            print(f"  ❌ concert_setlists UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_setlist_songs(self):
        """setlist_songs.csv → setlist_songs 테이블"""
        try:
            print("\n🎹 setlist_songs.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/setlist_songs.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ setlist_songs.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            processed_items = set()
            insert_count = 0
            
            for _, row in df.iterrows():
                setlist_id = row['setlist_id']
                song_id = row['song_id']
                order_index = row.get('order_index', 0)
                
                if not setlist_id or not song_id:
                    continue
                
                item_key = (setlist_id, song_id, order_index)
                if item_key in processed_items:
                    continue
                processed_items.add(item_key)
                
                # 기존 레코드 확인
                self.cursor.execute(
                    "SELECT id FROM setlist_songs WHERE setlist_id = %s AND song_id = %s AND order_index = %s",
                    (setlist_id, song_id, order_index)
                )
                if not self.cursor.fetchone():
                    insert_query = """
                        INSERT INTO setlist_songs (
                            setlist_id, song_id, order_index, fanchant, 
                            setlist_date, setlist_title, song_title, 
                            fanchant_point, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        setlist_id,
                        song_id,
                        order_index,
                        row.get('fanchant', ''),
                        row.get('setlist_date', ''),
                        row.get('setlist_title', ''),
                        row.get('song_title', ''),
                        row.get('fanchant_point', ''),
                        row.get('created_at', datetime.now()),
                        row.get('updated_at', datetime.now())
                    ))
                    insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ setlist_songs: {insert_count}개 삽입")
            return True
            
        except Exception as e:
            print(f"  ❌ setlist_songs UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_schedule(self):
        """schedule.csv → schedule 테이블"""
        try:
            print("\n📅 schedule.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/schedule.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ schedule.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            processed_items = set()
            insert_count = 0
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                if not concert_id:
                    continue
                
                item_key = (concert_id, row['scheduled_at'], row['category'])
                if item_key in processed_items:
                    continue
                processed_items.add(item_key)
                
                # 기존 레코드 확인
                self.cursor.execute(
                    "SELECT id FROM schedule WHERE concert_id = %s AND scheduled_at = %s AND category = %s",
                    (concert_id, row['scheduled_at'], row['category'])
                )
                if not self.cursor.fetchone():
                    insert_query = """
                        INSERT INTO schedule (concert_id, scheduled_at, category, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    current_time = datetime.now()
                    self.cursor.execute(insert_query, (
                        concert_id,
                        row['scheduled_at'],
                        row['category'],
                        current_time,
                        current_time
                    ))
                    insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ schedule: {insert_count}개 삽입")
            return True
            
        except Exception as e:
            print(f"  ❌ schedule UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_md(self):
        """md.csv → md 테이블"""
        try:
            print("\n🛍️ md.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/md.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ md.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            processed_items = set()
            insert_count = 0
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                if not concert_id:
                    continue
                
                item_key = (concert_id, row['name'])
                if item_key in processed_items:
                    continue
                processed_items.add(item_key)
                
                # 기존 레코드 확인
                self.cursor.execute(
                    "SELECT id FROM md WHERE concert_id = %s AND name = %s",
                    (concert_id, row['name'])
                )
                if not self.cursor.fetchone():
                    insert_query = """
                        INSERT INTO md (concert_id, name, price, img_url, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    current_time = datetime.now()
                    self.cursor.execute(insert_query, (
                        concert_id,
                        row['name'],
                        row.get('price', ''),
                        row.get('img_url', ''),
                        current_time,
                        current_time
                    ))
                    insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ md: {insert_count}개 삽입")
            return True
            
        except Exception as e:
            print(f"  ❌ md UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_cultures(self):
        """cultures.csv → cultures 테이블"""
        try:
            print("\n🎭 cultures.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/cultures.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ cultures.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            upsert_query = """
                INSERT INTO cultures (artist, category, description, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    description = VALUES(description),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            
            for _, row in df.iterrows():
                data_to_upsert.append((
                    row['artist'],
                    row['category'],
                    row.get('description', ''),
                    current_time,
                    current_time
                ))
            
            self.cursor.executemany(upsert_query, data_to_upsert)
            self.connection.commit()
            
            print(f"  ✅ cultures: {len(data_to_upsert)}개 UPSERT 완료")
            return True
            
        except Exception as e:
            print(f"  ❌ cultures UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def show_menu(self):
        """메뉴 표시"""
        csv_files = [f for f in os.listdir(self.csv_base_path) if f.endswith('.csv')]
        csv_files.sort()
        
        print("\n" + "="*60)
        print("📁 선택적 CSV 업로드")
        print("="*60)
        print("사용 가능한 CSV 파일:")
        
        methods = {
            'artists.csv': 'upsert_artists',
            'concerts.csv': 'upsert_concerts',
            'songs.csv': 'upsert_songs',
            'concert_info.csv': 'upsert_concert_info',
            'genres.csv': 'upsert_genres',
            'concert_genres.csv': 'upsert_concert_genres',
            'setlists.csv': 'upsert_setlists',
            'concert_setlists.csv': 'upsert_concert_setlists',
            'setlist_songs.csv': 'upsert_setlist_songs',
            'schedule.csv': 'upsert_schedule',
            'md.csv': 'upsert_md',
            'cultures.csv': 'upsert_cultures'
        }
        
        available_methods = []
        for i, file in enumerate(csv_files, 1):
            if file.startswith('songs_backup') or file.startswith('concert_info_backup'):
                continue
            method = methods.get(file, 'not_implemented')
            status = "✅" if method != 'not_implemented' else "❌"
            print(f"  {i:2d}. {file} {status}")
            available_methods.append((file, method))
        
        print(f"\n  {len(available_methods) + 1:2d}. 전체 업로드")
        print(f"  {len(available_methods) + 2:2d}. 종료")
        
        return available_methods

    def run_selective(self):
        """선택적 업로드 실행"""
        try:
            # SSH 터널 및 MySQL 연결
            if not self.create_ssh_tunnel():
                print("❌ SSH 터널 생성 실패")
                return
            
            if not self.connect_mysql():
                print("❌ MySQL 연결 실패")
                return
            
            while True:
                available_methods = self.show_menu()
                
                try:
                    choice = input("\n업로드할 파일 번호를 선택하세요: ")
                    choice_num = int(choice)
                    
                    if choice_num == len(available_methods) + 2:  # 종료
                        break
                    elif choice_num == len(available_methods) + 1:  # 전체 업로드
                        print("\n🚀 전체 파일 업로드 시작...")
                        # 순서대로 업로드
                        order = ['artists', 'genres', 'concerts', 'songs', 'concert_info', 
                                'concert_genres', 'setlists', 'concert_setlists', 
                                'setlist_songs', 'schedule', 'md', 'cultures']
                        for table in order:
                            method_name = f'upsert_{table}'
                            if hasattr(self, method_name):
                                getattr(self, method_name)()
                        break
                    elif 1 <= choice_num <= len(available_methods):
                        file, method = available_methods[choice_num - 1]
                        if method == 'not_implemented':
                            print(f"❌ {file}에 대한 업로드 메서드가 구현되지 않았습니다.")
                        else:
                            print(f"\n🚀 {file} 업로드 시작...")
                            getattr(self, method)()
                    else:
                        print("❌ 잘못된 선택입니다.")
                        
                except ValueError:
                    print("❌ 숫자를 입력해주세요.")
                except KeyboardInterrupt:
                    print("\n\n⚠️ 사용자에 의해 중단됨")
                    break
                    
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    upserter = SelectiveUpsertCSVToMySQL()
    upserter.run_selective()