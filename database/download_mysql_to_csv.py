#!/usr/bin/env python3
"""
MySQL 데이터베이스에서 CSV 파일로 데이터를 다운로드하는 스크립트
기존 CSV 파일을 백업하고 DB 데이터로 덮어씁니다.
"""
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import signal
import os
from datetime import datetime
import shutil

class DownloadMySQLToCSV:
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None
        self.csv_base_path = '/Users/youz2me/Xcode/Livith-Data/output/main_output'
        self.backup_path = '/Users/youz2me/Xcode/Livith-Data/output/backups'
        
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
            self.cursor = self.connection.cursor(dictionary=True)
            
            print("✅ MySQL 연결 성공!")
            return True
            
        except Error as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False
    
    def backup_csv_files(self):
        """기존 CSV 파일들을 백업"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_dir = f"{self.backup_path}/mysql_download_{timestamp}"
            
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)
            
            print(f"\n📂 백업 디렉토리: {backup_dir}")
            
            # 백업할 CSV 파일들
            csv_files = [
                'artists.csv',
                'concert_genres.csv',
                'concert_info.csv',
                'concert_setlists.csv',
                'concerts.csv',
                'cultures.csv',
                'home_concert_sections.csv',
                'home_sections.csv',
                'md.csv',
                'schedule.csv',
                'search_concert_sections.csv',
                'search_sections.csv',
                'setlists.csv',
                'songs.csv'
            ]
            
            for csv_file in csv_files:
                src = f"{self.csv_base_path}/{csv_file}"
                if os.path.exists(src):
                    dst = f"{backup_dir}/{csv_file}"
                    shutil.copy2(src, dst)
                    print(f"  • {csv_file} 백업 완료")
            
            return True
            
        except Exception as e:
            print(f"❌ 백업 실패: {e}")
            return False
    
    def download_artists(self):
        """artists 테이블 → artists.csv"""
        try:
            print("\n📥 artists 테이블 다운로드 중...")
            
            query = """
                SELECT id, artist, debut_date, category, detail, 
                       instagram_url, keywords, img_url, created_at, updated_at
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY artist ORDER BY id ASC) as rn
                    FROM artists
                ) ranked
                WHERE rn = 1
                ORDER BY id
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                
                # 컬럼명 매핑 (DB 컬럼명과 CSV 컬럼명이 다른 경우)
                df = df.rename(columns={
                    'debut_date': 'debut_date',
                    'category': 'group_type',
                    'detail': 'introduction',
                    'instagram_url': 'social_media'
                })
                
                # CSV 파일에 있지만 DB에 없는 컬럼 추가
                df['birth_date'] = ''
                df['nationality'] = ''
                
                # 컬럼 순서 정렬
                column_order = ['id', 'artist', 'birth_date', 'debut_date', 'nationality', 
                               'group_type', 'introduction', 'social_media', 'keywords', 'img_url', 
                               'created_at', 'updated_at']
                df = df.reindex(columns=column_order, fill_value='')
                
                # CSV 저장
                df.to_csv(f"{self.csv_base_path}/artists.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_concerts(self):
        """concerts 테이블 → concerts.csv"""
        try:
            print("\n📥 concerts 테이블 다운로드 중...")
            
            query = """
                SELECT id, artist, title, start_date, end_date, status, 
                       label, introduction, poster, code, ticket_site, 
                       ticket_url, venue, created_at, updated_at
                FROM concerts
                ORDER BY start_date DESC, artist, title
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                
                # CSV 저장
                # img_url 컬럼 추가 (poster를 img_url로 매핑)
                df = df.rename(columns={'poster': 'img_url'})
                
                df.to_csv(f"{self.csv_base_path}/concerts.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_setlists(self):
        """setlists 테이블 → setlists.csv"""
        try:
            print("\n📥 setlists 테이블 다운로드 중...")
            
            query = """
                SELECT id, title, artist, created_at, updated_at, img_url, 
                       end_date, start_date, venue
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY title, artist ORDER BY id ASC) as rn
                    FROM setlists
                ) ranked
                WHERE rn = 1
                ORDER BY id
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                
                # CSV 저장
                df.to_csv(f"{self.csv_base_path}/setlists.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_concert_genres(self):
        """concert_genres 테이블 → concert_genres.csv"""
        try:
            print("\n📥 concert_genres 테이블 다운로드 중...")
            
            query = """
                SELECT concert_id, concert_title, genre_id, name as genre_name
                FROM concert_genres
                ORDER BY concert_id, concert_title, genre_id
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                df.to_csv(f"{self.csv_base_path}/concert_genres.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_concert_setlists(self):
        """concert_setlists 테이블 → concert_setlists.csv"""
        try:
            print("\n📥 concert_setlists 테이블 다운로드 중...")
            
            query = """
                WITH setlist_with_songs AS (
                    SELECT cs.id, cs.concert_id, cs.setlist_id, cs.type, cs.status, 
                           cs.created_at, cs.updated_at, 
                           c.title as concert_title, s.title as setlist_title,
                           (SELECT COUNT(*) FROM setlist_songs ss WHERE ss.setlist_id = cs.setlist_id) as song_count,
                           ROW_NUMBER() OVER (PARTITION BY cs.concert_id ORDER BY 
                                             (SELECT COUNT(*) FROM setlist_songs ss WHERE ss.setlist_id = cs.setlist_id) DESC,
                                             cs.created_at ASC) as rn
                    FROM concert_setlists cs
                    LEFT JOIN concerts c ON cs.concert_id = c.id
                    LEFT JOIN setlists s ON cs.setlist_id = s.id
                )
                SELECT id, concert_id, setlist_id, type, status, created_at, updated_at,
                       concert_title, setlist_title
                FROM setlist_with_songs 
                WHERE rn = 1
                ORDER BY concert_id, setlist_id
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                df.to_csv(f"{self.csv_base_path}/concert_setlists.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_home_sections(self):
        """home_sections 테이블 → home_sections.csv"""
        try:
            print("\n📥 home_sections 테이블 다운로드 중...")
            
            query = """
                SELECT id, section_title as title
                FROM home_sections
                ORDER BY id
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                
                # 누락된 컬럼들을 기본값으로 추가
                df['is_artist_section'] = 0
                df['is_date_included'] = 0
                df['sub_heading'] = ''
                df['section_code'] = ''
                df['endpoint'] = ''
                df['order'] = 0
                
                df.to_csv(f"{self.csv_base_path}/home_sections.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                df.to_csv(f"{self.csv_base_path}/home_sections.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_home_concert_sections(self):
        """home_concert_sections 테이블 → home_concert_sections.csv"""
        try:
            print("\n📥 home_concert_sections 테이블 다운로드 중...")
            
            query = """
                SELECT id, home_section_id, concert_id, section_title, 
                       concert_title, sorted_index, created_at, updated_at
                FROM home_concert_sections
                ORDER BY home_section_id, sorted_index
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                df.to_csv(f"{self.csv_base_path}/home_concert_sections.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_search_sections(self):
        """search_sections 테이블 → search_sections.csv"""
        try:
            print("\n📥 search_sections 테이블 다운로드 중...")
            
            query = """
                SELECT id, section_title as title
                FROM search_sections
                ORDER BY id
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                
                # 누락된 컬럼들을 기본값으로 추가
                df['is_artist_section'] = 0
                df['is_date_included'] = 0
                df['sub_heading'] = ''
                df['section_code'] = ''
                df['endpoint'] = ''
                df['order'] = 0
                
                df.to_csv(f"{self.csv_base_path}/search_sections.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                df.to_csv(f"{self.csv_base_path}/search_sections.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_search_concert_sections(self):
        """search_concert_sections 테이블 → search_concert_sections.csv"""
        try:
            print("\n📥 search_concert_sections 테이블 다운로드 중...")
            
            query = """
                SELECT id, search_section_id, concert_id, section_title, 
                       concert_title, sorted_index, created_at, updated_at
                FROM search_concert_sections
                ORDER BY search_section_id, sorted_index
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                df.to_csv(f"{self.csv_base_path}/search_concert_sections.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_songs(self):
        """songs 테이블 → songs.csv"""
        try:
            print("\n📥 songs 테이블 다운로드 중...")
            
            query = """
                SELECT DISTINCT title, artist, lyrics, pronunciation, translation
                FROM songs
                ORDER BY artist, title
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                
                # musixmatch_url 컬럼 추가 (빈 값)
                df['musixmatch_url'] = ''
                
                df.to_csv(f"{self.csv_base_path}/songs.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_setlist_songs(self):
        """setlist_songs 테이블 → setlist_songs.csv"""
        try:
            print("\n📥 setlist_songs 테이블 다운로드 중...")
            
            query = """
                SELECT ss.song_title as title, 
                       (SELECT s.artist FROM songs s WHERE s.title = ss.song_title LIMIT 1) as artist,
                       ss.setlist_id, ss.order_index as `order`,
                       ss.fanchant as lyrics, '' as pronunciation, '' as translation
                FROM setlist_songs ss
                ORDER BY ss.setlist_id, ss.order_index
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                
                # musixmatch_url 컬럼 추가 (빈 값)
                df['musixmatch_url'] = ''
                
                df.to_csv(f"{self.csv_base_path}/setlist_songs.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                
                # CSV 저장
                df.to_csv(f"{self.csv_base_path}/songs.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_cultures(self):
        """cultures 테이블 → cultures.csv"""
        try:
            print("\n📥 cultures 테이블 다운로드 중...")
            
            query = """
                SELECT c.artist as artist_name, c.title as concert_title, 
                       cu.title, cu.content
                FROM cultures cu
                LEFT JOIN concerts c ON cu.concert_id = c.id
                ORDER BY artist_name, concert_title, cu.title
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                
                # CSV 저장
                df.to_csv(f"{self.csv_base_path}/cultures.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_schedule(self):
        """schedule 테이블 → schedule.csv"""
        try:
            print("\n📥 schedule 테이블 다운로드 중...")
            
            query = """
                SELECT c.title as concert_title, s.category, s.scheduled_at
                FROM schedule s
                LEFT JOIN concerts c ON s.concert_id = c.id
                ORDER BY s.scheduled_at, c.title
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                
                # datetime을 문자열로 변환
                df['scheduled_at'] = df['scheduled_at'].astype(str)
                
                # CSV 저장
                df.to_csv(f"{self.csv_base_path}/schedule.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_md(self):
        """md 테이블 → md.csv"""
        try:
            print("\n📥 md (굿즈) 테이블 다운로드 중...")
            
            query = """
                SELECT c.artist as artist_name, c.title as concert_title, 
                       m.name as item_name, m.price, m.img_url
                FROM md m
                LEFT JOIN concerts c ON m.concert_id = c.id
                ORDER BY artist_name, concert_title, item_name
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                
                # CSV 저장
                df.to_csv(f"{self.csv_base_path}/md.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
    def download_concert_info(self):
        """concert_info 테이블 → concert_info.csv"""
        try:
            print("\n📥 concert_info 테이블 다운로드 중...")
            
            query = """
                SELECT DISTINCT c.artist as artist_name, c.title as concert_title, 
                       ci.category, ci.content
                FROM concert_info ci
                LEFT JOIN concerts c ON ci.concert_id = c.id
                ORDER BY artist_name, concert_title, category
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows)
                
                # CSV 저장
                df.to_csv(f"{self.csv_base_path}/concert_info.csv", index=False, encoding='utf-8')
                print(f"  ✅ {len(df)}개 레코드 저장 완료")
            else:
                print("  ⚠️ 데이터가 없습니다")
                
        except Exception as e:
            print(f"  ❌ 다운로드 실패: {e}")
    
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
        """전체 다운로드 프로세스 실행"""
        try:
            print("\n" + "="*60)
            print("🚀 MySQL → CSV 다운로드 시작")
            print("="*60)
            
            # SSH 터널 생성
            if not self.create_ssh_tunnel():
                print("❌ SSH 터널 생성 실패")
                return
            
            # MySQL 연결
            if not self.connect_mysql():
                print("❌ MySQL 연결 실패")
                return
            
            # 기존 CSV 파일 백업
            if not self.backup_csv_files():
                print("⚠️ 백업 실패했지만 계속 진행합니다...")
            
            # 각 테이블 다운로드
            self.download_artists()
            self.download_concert_genres()
            self.download_concert_info()
            self.download_concert_setlists()
            self.download_concerts()
            self.download_cultures()
            self.download_home_concert_sections()
            self.download_home_sections()
            self.download_md()
            self.download_schedule()
            self.download_search_concert_sections()
            self.download_search_sections()
            self.download_setlists()
            self.download_setlist_songs()
            self.download_songs()
            
            print("\n" + "="*60)
            print("✨ 모든 데이터 다운로드 완료!")
            print("="*60)
            
        except KeyboardInterrupt:
            print("\n\n⚠️ 사용자에 의해 중단됨")
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    downloader = DownloadMySQLToCSV()
    downloader.run()