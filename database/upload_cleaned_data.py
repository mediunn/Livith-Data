#!/usr/bin/env python3
"""
cleaned_data 폴더의 데이터를 중복 없이 MySQL에 안전하게 업로드하는 스크립트
"""
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import signal
import os
from datetime import datetime

class SafeCleanedDataUploader:
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None
        self.cleaned_data_path = '/Users/youz2me/Xcode/Livith-Data/output/cleaned_data'

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

    def clear_result_buffer(self):
        """결과 버퍼 정리"""
        try:
            self.cursor.fetchall()
        except:
            pass

    def safe_upsert_artists(self):
        """artists.csv → artists 테이블 (안전한 UPSERT)"""
        try:
            print("\n👥 artists.csv 안전 업로드 중...")
            
            csv_path = f"{self.cleaned_data_path}/artists.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ artists.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # 중복 방지를 위한 세트
            processed_artists = set()
            insert_count = 0
            update_count = 0
            duplicate_count = 0
            
            current_time = datetime.now()
            
            for _, row in df.iterrows():
                artist_name = row['artist']
                
                if not artist_name or artist_name in processed_artists:
                    duplicate_count += 1
                    continue
                    
                processed_artists.add(artist_name)
                
                # DB에서 기존 아티스트 확인
                self.cursor.execute("SELECT id FROM artists WHERE artist = %s", (artist_name,))
                existing = self.cursor.fetchone()
                self.clear_result_buffer()
                
                if existing:
                    # UPDATE
                    update_query = """
                        UPDATE artists 
                        SET debut_date = %s, category = %s, detail = %s,
                            instagram_url = %s, keywords = %s, img_url = %s, 
                            updated_at = %s
                        WHERE artist = %s
                    """
                    self.cursor.execute(update_query, (
                        row.get('debut_date', ''),
                        row.get('category', ''),
                        row.get('detail', ''),
                        row.get('instagram_url', ''),
                        row.get('keywords', ''),
                        row.get('img_url', ''),
                        current_time,
                        artist_name
                    ))
                    update_count += 1
                else:
                    # INSERT
                    insert_query = """
                        INSERT INTO artists (artist, debut_date, category, detail, 
                                           instagram_url, keywords, img_url, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        artist_name,
                        row.get('debut_date', ''),
                        row.get('category', ''),
                        row.get('detail', ''),
                        row.get('instagram_url', ''),
                        row.get('keywords', ''),
                        row.get('img_url', ''),
                        current_time,
                        current_time
                    ))
                    insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ artists: {insert_count}개 삽입, {update_count}개 업데이트, {duplicate_count}개 중복 스킵")
            return True
            
        except Exception as e:
            print(f"  ❌ artists 업로드 실패: {e}")
            self.connection.rollback()
            return False

    def safe_upsert_concerts(self):
        """concerts.csv → concerts 테이블 (안전한 UPSERT)"""
        try:
            print("\n🎪 concerts.csv 안전 업로드 중...")
            
            csv_path = f"{self.cleaned_data_path}/concerts.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ concerts.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # artist_id 매핑
            self.cursor.execute("SELECT id, artist FROM artists")
            artist_mapping = {artist: id for id, artist in self.cursor.fetchall()}
            self.clear_result_buffer()
            
            processed_concerts = set()
            insert_count = 0
            update_count = 0
            duplicate_count = 0
            unmapped_artists = set()
            
            current_time = datetime.now()
            
            for _, row in df.iterrows():
                title = row['title']
                artist_name = row['artist']
                
                if not title or title in processed_concerts:
                    duplicate_count += 1
                    continue
                    
                processed_concerts.add(title)
                artist_id = artist_mapping.get(artist_name)
                
                if not artist_id:
                    unmapped_artists.add(artist_name)
                    continue
                
                # DB에서 기존 콘서트 확인
                self.cursor.execute("SELECT id FROM concerts WHERE title = %s", (title,))
                existing = self.cursor.fetchone()
                self.clear_result_buffer()
                
                if existing:
                    # UPDATE
                    update_query = """
                        UPDATE concerts 
                        SET artist = %s, artist_id = %s, start_date = %s, end_date = %s,
                            status = %s, poster = %s, code = %s, ticket_site = %s,
                            ticket_url = %s, venue = %s, label = %s, introduction = %s,
                            updated_at = %s
                        WHERE title = %s
                    """
                    self.cursor.execute(update_query, (
                        artist_name, artist_id, row['start_date'], row['end_date'],
                        row['status'], row.get('poster', ''), row.get('code', ''),
                        row.get('ticket_site', ''), row.get('ticket_url', ''),
                        row.get('venue', ''), row.get('label', ''), row.get('introduction', ''),
                        current_time, title
                    ))
                    update_count += 1
                else:
                    # INSERT
                    insert_query = """
                        INSERT INTO concerts (
                            title, artist, artist_id, start_date, end_date, 
                            status, poster, code, ticket_site, 
                            ticket_url, venue, label, introduction, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        title, artist_name, artist_id, row['start_date'], row['end_date'],
                        row['status'], row.get('poster', ''), row.get('code', ''),
                        row.get('ticket_site', ''), row.get('ticket_url', ''),
                        row.get('venue', ''), row.get('label', ''), row.get('introduction', ''),
                        current_time, current_time
                    ))
                    insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ concerts: {insert_count}개 삽입, {update_count}개 업데이트, {duplicate_count}개 중복 스킵")
            
            if unmapped_artists:
                print(f"  ⚠️ 매핑되지 않은 아티스트: {', '.join(list(unmapped_artists)[:3])}")
            
            return True
            
        except Exception as e:
            print(f"  ❌ concerts 업로드 실패: {e}")
            self.connection.rollback()
            return False

    def safe_upsert_songs(self):
        """songs.csv → songs 테이블 (안전한 UPSERT)"""
        try:
            print("\n🎵 songs.csv 안전 업로드 중...")
            
            csv_path = f"{self.cleaned_data_path}/songs.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ songs.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            processed_songs = set()
            insert_count = 0
            update_count = 0
            duplicate_count = 0
            
            current_time = datetime.now()
            
            for _, row in df.iterrows():
                title = row['title'][:50] if row['title'] else ''
                artist = row['artist'][:50] if row['artist'] else ''
                
                if not title or not artist:
                    continue
                    
                song_key = (title, artist)
                
                if song_key in processed_songs:
                    duplicate_count += 1
                    continue
                    
                processed_songs.add(song_key)
                
                # DB에서 기존 곡 확인
                self.cursor.execute("SELECT id FROM songs WHERE title = %s AND artist = %s", (title, artist))
                existing = self.cursor.fetchone()
                self.clear_result_buffer()
                
                if existing:
                    # UPDATE
                    update_query = """
                        UPDATE songs 
                        SET lyrics = %s, pronunciation = %s, translation = %s,
                            img_url = %s, youtube_id = %s, updated_at = %s
                        WHERE title = %s AND artist = %s
                    """
                    self.cursor.execute(update_query, (
                        row.get('lyrics', ''), row.get('pronunciation', ''), row.get('translation', ''),
                        row.get('img_url', ''), row.get('youtube_id', ''), current_time,
                        title, artist
                    ))
                    update_count += 1
                else:
                    # INSERT
                    insert_query = """
                        INSERT INTO songs (title, artist, lyrics, pronunciation, translation,
                                         img_url, youtube_id, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        title, artist, row.get('lyrics', ''), row.get('pronunciation', ''),
                        row.get('translation', ''), row.get('img_url', ''), row.get('youtube_id', ''),
                        current_time, current_time
                    ))
                    insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ songs: {insert_count}개 삽입, {update_count}개 업데이트, {duplicate_count}개 중복 스킵")
            return True
            
        except Exception as e:
            print(f"  ❌ songs 업로드 실패: {e}")
            self.connection.rollback()
            return False

    def safe_upsert_setlists(self):
        """setlists.csv → setlists 테이블 (안전한 UPSERT)"""
        try:
            print("\n🎤 setlists.csv 안전 업로드 중...")
            
            csv_path = f"{self.cleaned_data_path}/setlists.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ setlists.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            processed_setlists = set()
            insert_count = 0
            update_count = 0
            duplicate_count = 0
            
            current_time = datetime.now()
            
            for _, row in df.iterrows():
                title = row['title']
                artist = row.get('artist', '')
                
                if not title:
                    continue
                    
                setlist_key = (title, artist)
                
                if setlist_key in processed_setlists:
                    duplicate_count += 1
                    continue
                    
                processed_setlists.add(setlist_key)
                
                # DB에서 기존 세트리스트 확인
                self.cursor.execute("SELECT id FROM setlists WHERE title = %s AND artist = %s", (title, artist))
                existing = self.cursor.fetchone()
                self.clear_result_buffer()
                
                if existing:
                    # UPDATE
                    update_query = """
                        UPDATE setlists 
                        SET img_url = %s, start_date = %s, end_date = %s, 
                            venue = %s, updated_at = %s
                        WHERE title = %s AND artist = %s
                    """
                    self.cursor.execute(update_query, (
                        row.get('img_url', ''), row.get('start_date', ''), row.get('end_date', ''),
                        row.get('venue', ''), current_time, title, artist
                    ))
                    update_count += 1
                else:
                    # INSERT
                    insert_query = """
                        INSERT INTO setlists (title, artist, img_url, start_date, end_date, 
                                            venue, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        title, artist, row.get('img_url', ''), row.get('start_date', ''),
                        row.get('end_date', ''), row.get('venue', ''), current_time, current_time
                    ))
                    insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ setlists: {insert_count}개 삽입, {update_count}개 업데이트, {duplicate_count}개 중복 스킵")
            return True
            
        except Exception as e:
            print(f"  ❌ setlists 업로드 실패: {e}")
            self.connection.rollback()
            return False

    def safe_upsert_setlist_songs(self):
        """setlist_songs.csv → setlist_songs 테이블 (안전한 UPSERT)"""
        try:
            print("\n🎶 setlist_songs.csv 안전 업로드 중...")
            
            csv_path = f"{self.cleaned_data_path}/setlist_songs.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ setlist_songs.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            processed_positions = set()
            insert_count = 0
            update_count = 0
            duplicate_count = 0
            
            current_time = datetime.now()
            
            for _, row in df.iterrows():
                setlist_id = int(row['setlist_id']) if row['setlist_id'] else None
                song_id = int(row['song_id']) if row['song_id'] else None
                order_index = int(row['order_index']) if row['order_index'] else None
                
                if not setlist_id or not song_id or order_index is None:
                    continue
                
                position_key = (setlist_id, order_index)
                
                if position_key in processed_positions:
                    duplicate_count += 1
                    continue
                    
                processed_positions.add(position_key)
                
                # DB에서 기존 위치 확인
                self.cursor.execute(
                    "SELECT id FROM setlist_songs WHERE setlist_id = %s AND order_index = %s", 
                    (setlist_id, order_index)
                )
                existing = self.cursor.fetchone()
                self.clear_result_buffer()
                
                if existing:
                    # UPDATE
                    update_query = """
                        UPDATE setlist_songs 
                        SET song_id = %s, fanchant = %s, fanchant_point = %s,
                            setlist_date = %s, setlist_title = %s, song_title = %s,
                            updated_at = %s
                        WHERE setlist_id = %s AND order_index = %s
                    """
                    self.cursor.execute(update_query, (
                        song_id, row.get('fanchant', ''), row.get('fanchant_point', ''),
                        row.get('setlist_date', ''), row.get('setlist_title', ''),
                        row.get('song_title', '')[:50], current_time,
                        setlist_id, order_index
                    ))
                    update_count += 1
                else:
                    # INSERT
                    insert_query = """
                        INSERT INTO setlist_songs (
                            setlist_id, song_id, order_index, fanchant, fanchant_point,
                            setlist_date, setlist_title, song_title, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        setlist_id, song_id, order_index, row.get('fanchant', ''),
                        row.get('fanchant_point', ''), row.get('setlist_date', ''),
                        row.get('setlist_title', ''), row.get('song_title', '')[:50],
                        current_time, current_time
                    ))
                    insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ setlist_songs: {insert_count}개 삽입, {update_count}개 업데이트, {duplicate_count}개 중복 스킵")
            return True
            
        except Exception as e:
            print(f"  ❌ setlist_songs 업로드 실패: {e}")
            self.connection.rollback()
            return False

    def safe_upsert_remaining_tables(self):
        """나머지 테이블들 안전 업로드"""
        try:
            print("\n📊 나머지 테이블들 안전 업로드 중...")
            
            # 이미 중복이 제거된 테이블들이므로 기존 로직 재사용
            from improved_upsert_csv_to_mysql import ImprovedUpsertCSVToMySQL
            
            # 임시로 기존 스크립트의 일부 메소드 호출
            temp_upserter = ImprovedUpsertCSVToMySQL()
            temp_upserter.connection = self.connection
            temp_upserter.cursor = self.cursor
            temp_upserter.csv_base_path = self.cleaned_data_path
            
            # 나머지 테이블들 처리
            success_count = 0
            
            # concert_info는 이미 cleaned_data에서 중복 제거됨
            if temp_upserter.upsert_concert_info():
                success_count += 1
                
            # cultures도 이미 cleaned_data에서 중복 제거됨  
            if temp_upserter.upsert_cultures():
                success_count += 1
            
            print(f"  ✅ 추가 테이블 {success_count}개 완료")
            return True
            
        except Exception as e:
            print(f"  ❌ 나머지 테이블 업로드 실패: {e}")
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
        """전체 안전 업로드 실행"""
        try:
            print("\n" + "="*70)
            print("🚀 CLEANED DATA 안전 업로드 (중복 절대 방지)")
            print("="*70)
            
            # SSH 터널 생성
            if not self.create_ssh_tunnel():
                print("❌ SSH 터널 생성 실패")
                return
            
            # MySQL 연결
            if not self.connect_mysql():
                print("❌ MySQL 연결 실패")
                return
            
            print("\n🛡️ 중복 방지 업로드 시작...")
            
            success_count = 0
            total_count = 5
            
            # 순서가 중요함: 종속성 고려
            if self.safe_upsert_artists():
                success_count += 1
            
            if self.safe_upsert_concerts():
                success_count += 1
            
            if self.safe_upsert_songs():
                success_count += 1
            
            if self.safe_upsert_setlists():
                success_count += 1
            
            if self.safe_upsert_setlist_songs():
                success_count += 1
            
            print("\n" + "="*70)
            print(f"✅ CLEANED DATA 안전 업로드 완료! ({success_count}/{total_count})")
            print("🛡️ 중복 생성 위험 ZERO!")
            print("="*70)
            
        except KeyboardInterrupt:
            print("\n\n⚠️ 사용자에 의해 중단됨")
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    uploader = SafeCleanedDataUploader()
    uploader.run()