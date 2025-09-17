#!/usr/bin/env python3
"""
개선된 CSV to MySQL UPSERT 스크립트 - 중복 방지 로직 포함
"""
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import signal
import os
from datetime import datetime

class ImprovedUpsertCSVToMySQL:
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

    def add_unique_constraints(self):
        """테이블에 UNIQUE 제약조건 추가"""
        try:
            print("\n🔧 UNIQUE 제약조건 추가 중...")
            
            constraints = [
                # artists - artist 컬럼에 UNIQUE
                ("ALTER TABLE artists ADD UNIQUE KEY uk_artist (artist)", "artists.artist"),
                
                # concerts - title에 UNIQUE (code는 이미 UNIQUE)
                ("ALTER TABLE concerts ADD UNIQUE KEY uk_title (title)", "concerts.title"),
                
                # songs - title + artist 조합에 UNIQUE
                ("ALTER TABLE songs ADD UNIQUE KEY uk_title_artist (title, artist)", "songs.(title,artist)"),
                
                # setlists - title + artist 조합에 UNIQUE
                ("ALTER TABLE setlists ADD UNIQUE KEY uk_title_artist (title, artist)", "setlists.(title,artist)"),
                
                # concert_info - concert_id + category 조합에 UNIQUE
                ("ALTER TABLE concert_info ADD UNIQUE KEY uk_concert_category (concert_id, category)", "concert_info.(concert_id,category)"),
                
                # cultures - concert_id + title 조합에 UNIQUE (title이 TEXT라서 길이 제한 필요)
                ("ALTER TABLE cultures ADD UNIQUE KEY uk_concert_title (concert_id, title(255))", "cultures.(concert_id,title)"),
                
                # md - concert_id + name 조합에 UNIQUE
                ("ALTER TABLE md ADD UNIQUE KEY uk_concert_name (concert_id, name)", "md.(concert_id,name)"),
                
                # schedule - concert_id + scheduled_at + type 조합에 UNIQUE
                ("ALTER TABLE schedule ADD UNIQUE KEY uk_concert_schedule (concert_id, scheduled_at, type)", "schedule.(concert_id,scheduled_at,type)"),
                
                # setlist_songs - setlist_id + order_index는 이미 UNIQUE
            ]
            
            for query, desc in constraints:
                try:
                    self.cursor.execute(query)
                    print(f"  ✅ {desc} UNIQUE 제약조건 추가")
                except Error as e:
                    if "Duplicate key name" in str(e):
                        print(f"  ℹ️ {desc} UNIQUE 제약조건 이미 존재")
                    elif "Duplicate entry" in str(e):
                        print(f"  ⚠️ {desc} 중복 데이터 존재 - 정리 필요")
                    else:
                        print(f"  ❌ {desc} 추가 실패: {e}")
            
            self.connection.commit()
            
        except Exception as e:
            print(f"❌ UNIQUE 제약조건 추가 실패: {e}")
            self.connection.rollback()

    def upsert_artists(self):
        """artists.csv → artists 테이블 (중복 방지 UPSERT)"""
        try:
            print("\n📁 artists.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/artists.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ artists.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # INSERT ... ON DUPLICATE KEY UPDATE 사용
            upsert_query = """
                INSERT INTO artists (
                    artist, debut_date, category, detail, 
                    instagram_url, keywords, img_url, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    row.get('group_type', ''),
                    row.get('introduction', ''),
                    row.get('social_media', ''),
                    row.get('keywords', ''),
                    row.get('img_url', ''),
                    current_time,
                    current_time
                ))
            
            self.cursor.executemany(upsert_query, data_to_upsert)
            self.connection.commit()
            
            print(f"  ✅ artists 테이블 UPSERT 완료 ({len(data_to_upsert)}개)")
            return True
            
        except Exception as e:
            print(f"  ❌ artists UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concerts(self):
        """concerts.csv → concerts 테이블 (중복 방지 UPSERT)"""
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
            
            # title을 기준으로 UPSERT
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
                    code = COALESCE(code, VALUES(code)),
                    ticket_site = VALUES(ticket_site),
                    ticket_url = VALUES(ticket_url),
                    venue = VALUES(venue),
                    label = VALUES(label),
                    introduction = VALUES(introduction),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            unmapped_artists = set()
            
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
                else:
                    unmapped_artists.add(artist_name)
            
            if unmapped_artists:
                print(f"  ⚠️ 매핑되지 않은 아티스트: {', '.join(list(unmapped_artists)[:3])}")
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ concerts 테이블 UPSERT 완료 ({len(data_to_upsert)}개)")
            
            return True
            
        except Exception as e:
            print(f"  ❌ concerts UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_songs(self):
        """songs.csv → songs 테이블 (중복 방지 UPSERT)"""
        try:
            print("\n🎵 songs.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/songs.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ songs.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # title + artist 조합으로 UPSERT
            upsert_query = """
                INSERT INTO songs (
                    title, artist, lyrics, pronunciation, translation,
                    img_url, youtube_id, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    lyrics = VALUES(lyrics),
                    pronunciation = VALUES(pronunciation),
                    translation = VALUES(translation),
                    img_url = VALUES(img_url),
                    youtube_id = VALUES(youtube_id),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            
            for _, row in df.iterrows():
                data_to_upsert.append((
                    row['title'][:50],  # VARCHAR(50) 제한
                    row['artist'][:50],  # VARCHAR(50) 제한
                    row.get('lyrics', ''),
                    row.get('pronunciation', ''),
                    row.get('translation', ''),
                    row.get('img_url', ''),
                    row.get('youtube_id', ''),
                    current_time,
                    current_time
                ))
            
            self.cursor.executemany(upsert_query, data_to_upsert)
            self.connection.commit()
            
            print(f"  ✅ songs 테이블 UPSERT 완료 ({len(data_to_upsert)}개)")
            return True
            
        except Exception as e:
            print(f"  ❌ songs UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_setlists(self):
        """setlists.csv → setlists 테이블 (중복 방지 UPSERT)"""
        try:
            print("\n🎤 setlists.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/setlists.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ setlists.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # title + artist 조합으로 UPSERT
            upsert_query = """
                INSERT INTO setlists (
                    title, artist, img_url, start_date, end_date, 
                    venue, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    img_url = VALUES(img_url),
                    start_date = VALUES(start_date),
                    end_date = VALUES(end_date),
                    venue = VALUES(venue),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            
            for _, row in df.iterrows():
                data_to_upsert.append((
                    row['title'],
                    row['artist'],
                    row.get('img_url', ''),
                    row.get('start_date', ''),
                    row.get('end_date', ''),
                    row.get('venue', ''),
                    current_time,
                    current_time
                ))
            
            self.cursor.executemany(upsert_query, data_to_upsert)
            self.connection.commit()
            
            print(f"  ✅ setlists 테이블 UPSERT 완료 ({len(data_to_upsert)}개)")
            return True
            
        except Exception as e:
            print(f"  ❌ setlists UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concert_info(self):
        """concert_info.csv → concert_info 테이블 (중복 방지 UPSERT)"""
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
            
            # concert_id + category 조합으로 UPSERT
            upsert_query = """
                INSERT INTO concert_info (
                    concert_id, category, content, img_url, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    content = VALUES(content),
                    img_url = VALUES(img_url),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            unmapped_concerts = set()
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                if concert_id:
                    data_to_upsert.append((
                        concert_id,
                        row['category'][:30],  # VARCHAR(30) 제한
                        row.get('content', '')[:100],  # VARCHAR(100) 제한
                        row.get('img_url', ''),
                        current_time,
                        current_time
                    ))
                else:
                    unmapped_concerts.add(row['concert_title'])
            
            if unmapped_concerts:
                print(f"  ⚠️ 매핑되지 않은 콘서트: {', '.join(list(unmapped_concerts)[:3])}")
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ concert_info 테이블 UPSERT 완료 ({len(data_to_upsert)}개)")
            
            return True
            
        except Exception as e:
            print(f"  ❌ concert_info UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_cultures(self):
        """cultures.csv → cultures 테이블 (중복 방지 UPSERT)"""
        try:
            print("\n🎭 cultures.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/cultures.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ cultures.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # concert_id + title 조합으로 UPSERT
            upsert_query = """
                INSERT INTO cultures (
                    concert_id, title, content, img_url, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    content = VALUES(content),
                    img_url = VALUES(img_url),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            unmapped_concerts = set()
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                if concert_id:
                    data_to_upsert.append((
                        concert_id,
                        row['title'],  # TEXT 타입
                        row.get('content', ''),  # TEXT 타입
                        row.get('img_url', ''),
                        current_time,
                        current_time
                    ))
                else:
                    unmapped_concerts.add(row['concert_title'])
            
            if unmapped_concerts:
                print(f"  ⚠️ 매핑되지 않은 콘서트: {', '.join(list(unmapped_concerts)[:3])}")
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ cultures 테이블 UPSERT 완료 ({len(data_to_upsert)}개)")
            
            return True
            
        except Exception as e:
            print(f"  ❌ cultures UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_md(self):
        """md.csv → md 테이블 (중복 방지 UPSERT)"""
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
            
            # concert_id + name 조합으로 중복 제거 후 INSERT
            current_time = datetime.now()
            processed_items = set()
            insert_count = 0
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                if concert_id:
                    item_name = row['item_name'][:100]  # VARCHAR(100) 제한
                    item_key = (concert_id, item_name)
                    
                    # 중복 체크 (이미 처리된 항목 스킵)
                    if item_key in processed_items:
                        continue
                    processed_items.add(item_key)
                    
                    # DB에서 중복 체크
                    self.cursor.execute(
                        "SELECT id FROM md WHERE concert_id = %s AND name = %s",
                        (concert_id, item_name)
                    )
                    existing = self.cursor.fetchone()
                    # 결과 버퍼 정리
                    self.cursor.fetchall()
                    
                    if existing:
                        continue  # 이미 존재하는 경우 스킵
                    
                    # INSERT
                    insert_query = """
                        INSERT INTO md (concert_id, name, price, img_url, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    
                    self.cursor.execute(insert_query, (
                        concert_id,
                        item_name,
                        row.get('price', '')[:30],  # VARCHAR(30) 제한
                        row.get('img_url', ''),
                        current_time,
                        current_time
                    ))
                    insert_count += 1
            
            if insert_count > 0:
                self.connection.commit()
                print(f"  ✅ md 테이블에 {insert_count}개 삽입 완료")
            else:
                print(f"  ℹ️ md 테이블에 새로운 데이터 없음")
            
            return True
            
        except Exception as e:
            print(f"  ❌ md UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_schedule(self):
        """schedule.csv → schedule 테이블 (중복 방지 UPSERT)"""
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
            
            # scheduled_at 컬럼 처리
            def parse_scheduled_at(date_str):
                if not date_str:
                    return datetime.now()
                try:
                    # 다양한 날짜 형식 처리
                    for fmt in ['%Y-%m-%d %H:%M:%S', '%Y.%m.%d %H:%M', '%Y-%m-%d', '%Y.%m.%d']:
                        try:
                            return datetime.strptime(date_str, fmt)
                        except:
                            continue
                    return datetime.now()
                except:
                    return datetime.now()
            
            # concert_id + scheduled_at + type 조합으로 UPSERT
            upsert_query = """
                INSERT INTO schedule (
                    concert_id, category, scheduled_at, type, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    category = VALUES(category),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_upsert = []
            unmapped_concerts = set()
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                if concert_id:
                    scheduled_at = parse_scheduled_at(row.get('scheduled_at', ''))
                    schedule_type = row.get('type', 'CONCERT')
                    if schedule_type not in ['CONCERT', 'TICKETING']:
                        schedule_type = 'CONCERT'
                    
                    data_to_upsert.append((
                        concert_id,
                        row['category'][:50],  # VARCHAR(50) 제한
                        scheduled_at,
                        schedule_type,
                        current_time,
                        current_time
                    ))
                else:
                    unmapped_concerts.add(row['concert_title'])
            
            if unmapped_concerts:
                print(f"  ⚠️ 매핑되지 않은 콘서트: {', '.join(list(unmapped_concerts)[:3])}")
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ schedule 테이블 UPSERT 완료 ({len(data_to_upsert)}개)")
            
            return True
            
        except Exception as e:
            print(f"  ❌ schedule UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concert_genres(self):
        """concert_genres.csv → concert_genres 테이블 (중복 방지 UPSERT)"""
        try:
            print("\n🎸 concert_genres.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/concert_genres.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ concert_genres.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # genre 매핑
            genre_mapping = {
                'JPOP': 1, 'ROCK_METAL': 2, 'RAP_HIPHOP': 3,
                'CLASSIC_JAZZ': 4, 'ACOUSTIC': 5, 'ELECTRONIC': 6
            }
            
            # concert_id + genre_id 조합으로 UPSERT (이미 UNIQUE KEY 존재)
            upsert_query = """
                INSERT INTO concert_genres (
                    concert_id, concert_title, genre_id, name
                ) VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    concert_title = VALUES(concert_title),
                    name = VALUES(name)
            """
            
            data_to_upsert = []
            unmapped_concerts = set()
            
            for _, row in df.iterrows():
                concert_title = row['concert_title']
                concert_id = concert_mapping.get(concert_title)
                genre_name = row.get('name', 'JPOP')
                genre_id = genre_mapping.get(genre_name, 1)
                
                if concert_id:
                    data_to_upsert.append((
                        concert_id,
                        concert_title,
                        genre_id,
                        genre_name
                    ))
                else:
                    unmapped_concerts.add(concert_title)
            
            if unmapped_concerts:
                print(f"  ⚠️ 매핑되지 않은 콘서트: {', '.join(list(unmapped_concerts)[:3])}")
            
            if data_to_upsert:
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ concert_genres 테이블 UPSERT 완료 ({len(data_to_upsert)}개)")
            
            return True
            
        except Exception as e:
            print(f"  ❌ concert_genres UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_concert_setlists(self):
        """concert_setlists.csv → concert_setlists 테이블 (중복 방지 UPSERT)"""
        try:
            print("\n🎼 concert_setlists.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/concert_setlists.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ concert_setlists.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # CSV에 이미 ID가 있고 concert_id, setlist_id가 있으므로 직접 사용
            current_time = datetime.now()
            insert_count = 0
            duplicate_count = 0
            
            for _, row in df.iterrows():
                concert_id = int(row['concert_id']) if row['concert_id'] else None
                setlist_id = int(row['setlist_id']) if row['setlist_id'] else None
                
                if concert_id and setlist_id:
                    # 중복 체크
                    self.cursor.execute(
                        "SELECT id FROM concert_setlists WHERE concert_id = %s AND setlist_id = %s",
                        (concert_id, setlist_id)
                    )
                    existing = self.cursor.fetchone()
                    # 결과 버퍼 정리
                    self.cursor.fetchall()
                    
                    if existing:
                        duplicate_count += 1
                        continue
                    
                    # INSERT
                    insert_query = """
                        INSERT INTO concert_setlists (
                            concert_id, setlist_id, type, status,
                            concert_title, setlist_title, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    self.cursor.execute(insert_query, (
                        concert_id,
                        setlist_id,
                        row.get('type', 'ONGOING'),
                        row.get('status', ''),
                        row.get('concert_title', ''),
                        row.get('setlist_title', ''),
                        current_time,
                        current_time
                    ))
                    insert_count += 1
            
            if insert_count > 0:
                self.connection.commit()
            
            print(f"  ✅ concert_setlists 테이블: {insert_count}개 삽입, {duplicate_count}개 중복 스킵")
            return True
            
        except Exception as e:
            print(f"  ❌ concert_setlists UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_setlist_songs(self):
        """setlist_songs.csv → setlist_songs 테이블 (중복 방지 UPSERT, setlist별 order_index)"""
        try:
            print("\n🎶 setlist_songs.csv UPSERT 중...")
            
            csv_path = f"{self.csv_base_path}/setlist_songs.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ setlist_songs.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # song_id 매핑 (title + artist)
            self.cursor.execute("SELECT id, title, artist FROM songs")
            song_mapping = {f"{title}_{artist}": id for id, title, artist in self.cursor.fetchall()}
            
            # setlist별로 order_index 재할당하여 처리
            current_time = datetime.now()
            data_to_upsert = []
            unmapped_items = []
            
            # setlist별로 그룹화하고 각각 처리
            for setlist_id in df['setlist_id'].unique():
                setlist_data = df[df['setlist_id'] == setlist_id].copy()
                
                # CSV의 'order' 컬럼 기준으로 정렬 (없으면 기존 순서 유지)
                if 'order' in setlist_data.columns:
                    setlist_data = setlist_data.sort_values('order')
                else:
                    setlist_data = setlist_data.sort_values('order_index')
                
                # 각 setlist 내에서 order_index를 1부터 순차 할당
                for idx, (_, row) in enumerate(setlist_data.iterrows(), 1):
                    song_key = f"{row['title']}_{row['artist']}" if 'title' in row and 'artist' in row else f"{row['song_title']}_{row.get('song_artist', '')}"
                    song_id = song_mapping.get(song_key)
                    
                    # song이 없으면 먼저 songs 테이블에 INSERT
                    if not song_id:
                        song_title = row.get('title', row.get('song_title', ''))
                        song_artist = row.get('artist', row.get('song_artist', ''))
                        
                        if song_title and song_artist:
                            # songs 테이블에 INSERT
                            song_insert = """
                                INSERT IGNORE INTO songs (title, artist, created_at, updated_at)
                                VALUES (%s, %s, %s, %s)
                            """
                            self.cursor.execute(song_insert, (
                                song_title[:50],
                                song_artist[:50],
                                current_time,
                                current_time
                            ))
                            
                            # 새로 삽입된 song_id 가져오기
                            self.cursor.execute(
                                "SELECT id FROM songs WHERE title = %s AND artist = %s",
                                (song_title[:50], song_artist[:50])
                            )
                            result = self.cursor.fetchone()
                            if result:
                                song_id = result[0]
                    
                    if setlist_id and song_id:
                        song_title = row.get('title', row.get('song_title', ''))
                        setlist_title = row.get('setlist_title', f"setlist_{setlist_id}")
                        
                        data_to_upsert.append((
                            int(setlist_id),
                            song_id,
                            idx,  # setlist별로 1부터 시작하는 order_index
                            row.get('fanchant', row.get('lyrics', '')),  # fanchant 또는 lyrics
                            row.get('fanchant_point', row.get('pronunciation', '')),  # fanchant_point 또는 pronunciation
                            row.get('setlist_date', ''),
                            setlist_title,
                            song_title[:50],  # VARCHAR(50) 제한
                            current_time,
                            current_time
                        ))
                    else:
                        if not setlist_id:
                            unmapped_items.append(f"setlist_id: {setlist_id}")
                        if not song_id:
                            unmapped_items.append(f"song: {song_key}")
            
            if unmapped_items:
                print(f"  ⚠️ 매핑되지 않은 항목: {', '.join(unmapped_items[:5])}")
            
            if data_to_upsert:
                # setlist_id + order_index 조합으로 UPSERT (UNIQUE KEY 존재)
                upsert_query = """
                    INSERT INTO setlist_songs (
                        setlist_id, song_id, order_index, fanchant, fanchant_point,
                        setlist_date, setlist_title, song_title, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        song_id = VALUES(song_id),
                        fanchant = VALUES(fanchant),
                        fanchant_point = VALUES(fanchant_point),
                        song_title = VALUES(song_title),
                        setlist_title = VALUES(setlist_title),
                        updated_at = VALUES(updated_at)
                """
                
                self.cursor.executemany(upsert_query, data_to_upsert)
                self.connection.commit()
                print(f"  ✅ setlist_songs 테이블 UPSERT 완료 ({len(data_to_upsert)}개)")
            
            return True
            
        except Exception as e:
            print(f"  ❌ setlist_songs UPSERT 실패: {e}")
            self.connection.rollback()
            return False

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
            print("🚀 개선된 CSV to MySQL UPSERT (중복 방지)")
            print("="*60)
            
            # SSH 터널 생성
            if not self.create_ssh_tunnel():
                print("❌ SSH 터널 생성 실패")
                return
            
            # MySQL 연결
            if not self.connect_mysql():
                print("❌ MySQL 연결 실패")
                return
            
            # UNIQUE 제약조건 추가
            self.add_unique_constraints()
            
            # 각 테이블 UPSERT (순서 중요!)
            print("\n" + "="*40)
            print("📊 데이터 UPSERT 시작")
            print("="*40)
            
            # 1. 기본 테이블
            self.upsert_artists()
            self.upsert_concerts()
            self.upsert_songs()
            self.upsert_setlists()
            
            # 2. 연관 테이블
            self.upsert_concert_info()
            self.upsert_cultures()
            self.upsert_md()
            self.upsert_schedule()
            
            # 3. 관계 테이블
            self.upsert_concert_genres()
            self.upsert_concert_setlists()
            self.upsert_setlist_songs()
            
            print("\n" + "="*60)
            print("✅ 모든 UPSERT 작업 완료!")
            print("="*60)
            
        except KeyboardInterrupt:
            print("\n\n⚠️ 사용자에 의해 중단됨")
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    upserter = ImprovedUpsertCSVToMySQL()
    upserter.run()