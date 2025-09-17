#!/usr/bin/env python3
"""
데이터베이스 중복 제거 및 다운로드 스크립트
"""
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import signal
import os
from datetime import datetime

class FixDuplicatesAndDownload:
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None
        self.output_path = '/Users/youz2me/Xcode/Livith-Data/output/cleaned_data'
        
        # 출력 디렉토리 생성
        os.makedirs(self.output_path, exist_ok=True)

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

    def check_duplicates(self, table_name, unique_columns):
        """테이블의 중복 확인"""
        try:
            print(f"\n🔍 {table_name} 테이블 중복 확인...")
            
            # 중복 레코드 찾기
            columns_str = ', '.join(unique_columns)
            having_clause = ' AND '.join([f'COUNT(DISTINCT {col}) = 1' for col in unique_columns])
            
            query = f"""
                SELECT {columns_str}, COUNT(*) as cnt
                FROM {table_name}
                GROUP BY {columns_str}
                HAVING COUNT(*) > 1
                ORDER BY cnt DESC
                LIMIT 10
            """
            
            self.cursor.execute(query)
            duplicates = self.cursor.fetchall()
            
            if duplicates:
                print(f"  ⚠️ 중복 발견! (상위 10개)")
                for dup in duplicates:
                    print(f"    • {dup}")
                return True
            else:
                print(f"  ✅ 중복 없음")
                return False
                
        except Exception as e:
            print(f"  ❌ 중복 확인 실패: {e}")
            return False

    def remove_duplicates_artists(self):
        """artists 테이블 중복 제거"""
        try:
            print("\n🧹 artists 테이블 중복 제거 중...")
            
            # 중복된 artist 찾기
            self.cursor.execute("""
                SELECT artist, COUNT(*) as cnt
                FROM artists
                GROUP BY artist
                HAVING COUNT(*) > 1
            """)
            duplicate_artists = self.cursor.fetchall()
            
            if duplicate_artists:
                print(f"  • 중복된 아티스트: {len(duplicate_artists)}개")
                
                for artist, count in duplicate_artists:
                    # 가장 최근 업데이트된 것만 남기고 나머지 삭제
                    self.cursor.execute("""
                        DELETE FROM artists 
                        WHERE artist = %s 
                        AND id NOT IN (
                            SELECT * FROM (
                                SELECT id FROM artists 
                                WHERE artist = %s 
                                ORDER BY updated_at DESC, id DESC 
                                LIMIT 1
                            ) as temp
                        )
                    """, (artist, artist))
                
                self.connection.commit()
                print(f"  ✅ 중복 제거 완료")
            else:
                print(f"  ✅ 중복 없음")
                
        except Exception as e:
            print(f"  ❌ 중복 제거 실패: {e}")
            self.connection.rollback()

    def remove_duplicates_songs(self):
        """songs 테이블 중복 제거"""
        try:
            print("\n🧹 songs 테이블 중복 제거 중...")
            
            # 중복된 song 찾기 (title + artist 조합)
            self.cursor.execute("""
                SELECT title, artist, COUNT(*) as cnt
                FROM songs
                GROUP BY title, artist
                HAVING COUNT(*) > 1
            """)
            duplicate_songs = self.cursor.fetchall()
            
            if duplicate_songs:
                print(f"  • 중복된 노래: {len(duplicate_songs)}개")
                
                for title, artist, count in duplicate_songs:
                    # 가장 최근 것만 남기고 나머지 삭제
                    self.cursor.execute("""
                        DELETE FROM songs 
                        WHERE title = %s AND artist = %s
                        AND id NOT IN (
                            SELECT * FROM (
                                SELECT id FROM songs 
                                WHERE title = %s AND artist = %s
                                ORDER BY id DESC 
                                LIMIT 1
                            ) as temp
                        )
                    """, (title, artist, title, artist))
                
                self.connection.commit()
                print(f"  ✅ 중복 제거 완료")
            else:
                print(f"  ✅ 중복 없음")
                
        except Exception as e:
            print(f"  ❌ 중복 제거 실패: {e}")
            self.connection.rollback()

    def remove_duplicates_setlists(self):
        """setlists 테이블 중복 제거"""
        try:
            print("\n🧹 setlists 테이블 중복 제거 중...")
            
            # 중복된 setlist 찾기 (title + artist 조합)
            self.cursor.execute("""
                SELECT title, artist, COUNT(*) as cnt
                FROM setlists
                GROUP BY title, artist
                HAVING COUNT(*) > 1
            """)
            duplicate_setlists = self.cursor.fetchall()
            
            if duplicate_setlists:
                print(f"  • 중복된 세트리스트: {len(duplicate_setlists)}개")
                
                for title, artist, count in duplicate_setlists:
                    # 가장 최근 업데이트된 것만 남기고 나머지 삭제
                    self.cursor.execute("""
                        DELETE FROM setlists 
                        WHERE title = %s AND artist = %s
                        AND id NOT IN (
                            SELECT * FROM (
                                SELECT id FROM setlists 
                                WHERE title = %s AND artist = %s
                                ORDER BY updated_at DESC, id DESC 
                                LIMIT 1
                            ) as temp
                        )
                    """, (title, artist, title, artist))
                
                self.connection.commit()
                print(f"  ✅ 중복 제거 완료")
            else:
                print(f"  ✅ 중복 없음")
                
        except Exception as e:
            print(f"  ❌ 중복 제거 실패: {e}")
            self.connection.rollback()

    def remove_duplicates_concert_info(self):
        """concert_info 테이블 중복 제거"""
        try:
            print("\n🧹 concert_info 테이블 중복 제거 중...")
            
            # 중복된 concert_info 찾기 (concert_id + category 조합)
            self.cursor.execute("""
                SELECT concert_id, category, COUNT(*) as cnt
                FROM concert_info
                GROUP BY concert_id, category
                HAVING COUNT(*) > 1
            """)
            duplicate_info = self.cursor.fetchall()
            
            if duplicate_info:
                print(f"  • 중복된 정보: {len(duplicate_info)}개")
                
                for concert_id, category, count in duplicate_info:
                    # 가장 최근 업데이트된 것만 남기고 나머지 삭제
                    self.cursor.execute("""
                        DELETE FROM concert_info 
                        WHERE concert_id = %s AND category = %s
                        AND id NOT IN (
                            SELECT * FROM (
                                SELECT id FROM concert_info 
                                WHERE concert_id = %s AND category = %s
                                ORDER BY updated_at DESC, id DESC 
                                LIMIT 1
                            ) as temp
                        )
                    """, (concert_id, category, concert_id, category))
                
                self.connection.commit()
                print(f"  ✅ 중복 제거 완료")
            else:
                print(f"  ✅ 중복 없음")
                
        except Exception as e:
            print(f"  ❌ 중복 제거 실패: {e}")
            self.connection.rollback()

    def remove_duplicates_cultures(self):
        """cultures 테이블 중복 제거"""
        try:
            print("\n🧹 cultures 테이블 중복 제거 중...")
            
            # 중복된 culture 찾기 (concert_id + title 조합)
            self.cursor.execute("""
                SELECT concert_id, title, COUNT(*) as cnt
                FROM cultures
                GROUP BY concert_id, title
                HAVING COUNT(*) > 1
            """)
            duplicate_cultures = self.cursor.fetchall()
            
            if duplicate_cultures:
                print(f"  • 중복된 문화 정보: {len(duplicate_cultures)}개")
                
                for concert_id, title, count in duplicate_cultures:
                    # 가장 최근 업데이트된 것만 남기고 나머지 삭제
                    self.cursor.execute("""
                        DELETE FROM cultures 
                        WHERE concert_id = %s AND title = %s
                        AND id NOT IN (
                            SELECT * FROM (
                                SELECT id FROM cultures 
                                WHERE concert_id = %s AND title = %s
                                ORDER BY updated_at DESC, id DESC 
                                LIMIT 1
                            ) as temp
                        )
                    """, (concert_id, title, concert_id, title))
                
                self.connection.commit()
                print(f"  ✅ 중복 제거 완료")
            else:
                print(f"  ✅ 중복 없음")
                
        except Exception as e:
            print(f"  ❌ 중복 제거 실패: {e}")
            self.connection.rollback()

    def download_table(self, table_name):
        """테이블 데이터를 CSV로 다운로드"""
        try:
            print(f"\n📥 {table_name} 테이블 다운로드 중...")
            
            # 전체 데이터 조회
            self.cursor.execute(f"SELECT * FROM {table_name}")
            rows = self.cursor.fetchall()
            
            # 컬럼명 가져오기
            self.cursor.execute(f"DESCRIBE {table_name}")
            columns = [col[0] for col in self.cursor.fetchall()]
            
            # DataFrame 생성
            df = pd.DataFrame(rows, columns=columns)
            
            # CSV 저장
            csv_path = f"{self.output_path}/{table_name}.csv"
            df.to_csv(csv_path, index=False, encoding='utf-8')
            
            print(f"  ✅ {len(df)}개 레코드 저장 완료: {csv_path}")
            return True
            
        except Exception as e:
            print(f"  ❌ {table_name} 다운로드 실패: {e}")
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
        """전체 프로세스 실행"""
        try:
            print("\n" + "="*60)
            print("🔧 데이터베이스 중복 제거 및 다운로드")
            print("="*60)
            
            # SSH 터널 생성
            if not self.create_ssh_tunnel():
                print("❌ SSH 터널 생성 실패")
                return
            
            # MySQL 연결
            if not self.connect_mysql():
                print("❌ MySQL 연결 실패")
                return
            
            # 1. 중복 제거
            print("\n" + "="*40)
            print("📊 STEP 1: 중복 제거")
            print("="*40)
            
            self.remove_duplicates_artists()
            self.remove_duplicates_songs()
            self.remove_duplicates_setlists()
            self.remove_duplicates_concert_info()
            self.remove_duplicates_cultures()
            
            # 2. 중복 확인
            print("\n" + "="*40)
            print("📊 STEP 2: 중복 제거 후 확인")
            print("="*40)
            
            self.check_duplicates('artists', ['artist'])
            self.check_duplicates('songs', ['title', 'artist'])
            self.check_duplicates('setlists', ['title', 'artist'])
            self.check_duplicates('concert_info', ['concert_id', 'category'])
            self.check_duplicates('cultures', ['concert_id', 'title'])
            
            # 3. 데이터 다운로드
            print("\n" + "="*40)
            print("📊 STEP 3: 정리된 데이터 다운로드")
            print("="*40)
            
            tables_to_download = [
                'artists', 'concerts', 'songs', 'setlists', 
                'concert_info', 'cultures', 'md', 'schedule',
                'concert_genres', 'concert_setlists', 'setlist_songs',
                'home_sections', 'home_concert_sections',
                'search_sections', 'search_concert_sections'
            ]
            
            for table in tables_to_download:
                self.download_table(table)
            
            print("\n" + "="*60)
            print("✅ 모든 작업 완료!")
            print(f"📁 다운로드 경로: {self.output_path}")
            print("="*60)
            
        except KeyboardInterrupt:
            print("\n\n⚠️ 사용자에 의해 중단됨")
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    fixer = FixDuplicatesAndDownload()
    fixer.run()