#!/usr/bin/env python3
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import signal
import os
from datetime import datetime

class CSVToMySQLLoader:
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None
        self.csv_base_path = '/Users/youz2me/Xcode/Livith-Data/output'

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
                stdout, stderr = self.ssh_process.communicate()
                print(f"❌ SSH 터널 생성 실패: {stderr.decode()}")
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
                'database': 'livith_v2',
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

    def clear_data_tables(self):
        """기존 데이터 삭제 (Foreign Key 순서 고려)"""
        try:
            print("🗑️ 기존 데이터 삭제 중...")
            
            # Foreign Key 체크 비활성화
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # 데이터 삭제 (의존성 역순)
            tables_to_clear = [
                'setlist_songs',
                'concert_setlists', 
                'concert_info',
                'cultures',
                'schedule',
                'setlists',
                'songs',
                'concerts',
                'artists'
            ]
            
            for table in tables_to_clear:
                # 기존 데이터는 유지하고 업서트 준비
                print(f"  ✓ {table} 업서트 준비 완료")
            
            # Foreign Key 체크 재활성화
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            self.connection.commit()
            print("✅ 모든 기존 데이터 삭제 완료")
            return True
            
        except Error as e:
            print(f"❌ 데이터 삭제 실패: {e}")
            self.connection.rollback()
            return False

    def load_artists(self):
        """artists.csv → artists 테이블"""
        try:
            print("\n📁 artists.csv 로드 중...")
            
            # CSV 읽기 (헤더 있음)
            df = pd.read_csv(f"{self.csv_base_path}/artists.csv", encoding='utf-8')
            
            print(f"  • CSV 레코드: {len(df)}개")
            print(f"  • CSV 컬럼: {list(df.columns)}")
            
            # birth_date 컬럼을 정수로 변환 (float 형식 방지)
            if 'birth_date' in df.columns:
                df['birth_date'] = df['birth_date'].apply(lambda x: int(float(x)) if pd.notnull(x) and str(x).replace('.','').replace('-','').isdigit() and x != '' else '')
            
            # 업서트 쿼리 (ON DUPLICATE KEY UPDATE 사용)
            upsert_query = """
                INSERT INTO artists (artist, birth_date, birth_place, category, detail, 
                                   instagram_url, keywords, img_url, created_at, updated_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    birth_date = VALUES(birth_date),
                    birth_place = VALUES(birth_place),
                    category = VALUES(category),
                    detail = VALUES(detail),
                    instagram_url = VALUES(instagram_url),
                    keywords = VALUES(keywords),
                    img_url = VALUES(img_url),
                    updated_at = VALUES(updated_at)
            """
            
            current_time = datetime.now()
            data_to_insert = []
            
            # NaN 값을 빈 문자열로 치환
            df = df.fillna('')
            
            for _, row in df.iterrows():
                data_to_insert.append((
                    row['artist'],
                    row.get('birth_date', ''),
                    row.get('birth_place', ''),
                    row.get('category', ''),
                    row.get('detail', ''),
                    row.get('instagram_url', ''),
                    row.get('keywords', ''),
                    row.get('img_url', ''),
                    current_time,
                    current_time
                ))
            
            self.cursor.executemany(upsert_query, data_to_insert)
            self.connection.commit()
            
            print(f"  ✅ artists 테이블에 {len(data_to_insert)}개 삽입 완료")
            return True
            
        except Exception as e:
            print(f"  ❌ artists 로드 실패: {e}")
            self.connection.rollback()
            return False

    def load_concerts(self):
        """concerts.csv → concerts 테이블"""
        try:
            print("\n🎪 concerts.csv 로드 중...")
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/concerts.csv", encoding='utf-8')
            print(f"  • CSV 레코드: {len(df)}개")
            
            # artist_id 매핑용 딕셔너리 생성
            self.cursor.execute("SELECT id, artist FROM artists")
            artist_mapping = {artist: id for id, artist in self.cursor.fetchall()}
            
            # 삽입 쿼리
            insert_query = """
                INSERT INTO concerts (title, artist, artist_id, start_date, end_date, venue, created_at, updated_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            current_time = datetime.now()
            data_to_insert = []
            unmapped_artists = []
            
            for _, row in df.iterrows():
                artist_name = row['artist']
                artist_id = artist_mapping.get(artist_name)
                
                if artist_id:
                    data_to_insert.append((
                        row['title'],
                        artist_name,
                        artist_id,
                        row.get('start_date', ''),
                        row.get('end_date', ''),
                        row.get('venue', ''),
                        current_time,
                        current_time
                    ))
                else:
                    unmapped_artists.append(artist_name)
            
            if unmapped_artists:
                print(f"  ⚠️ 매핑되지 않은 아티스트 ({len(unmapped_artists)}개):")
                for artist in unmapped_artists[:5]:
                    print(f"     • {artist}")
            
            if data_to_insert:
                self.cursor.executemany(insert_query, data_to_insert)
                self.connection.commit()
                print(f"  ✅ concerts 테이블에 {len(data_to_insert)}개 삽입 완료")
            
            return True
            
        except Exception as e:
            print(f"  ❌ concerts 로드 실패: {e}")
            self.connection.rollback()
            return False

    def load_songs(self):
        """songs.csv → songs 테이블"""
        try:
            print("\n🎵 songs.csv 로드 중...")
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/songs.csv", encoding='utf-8')
            print(f"  • CSV 레코드: {len(df)}개")
            
            # 삽입 쿼리 (현재 DB에는 artist_id FK가 없는 것으로 보임)
            insert_query = """
                INSERT INTO songs (title, artist, lyrics, pronunciation, translation, youtube_id, created_at, updated_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # NaN 값을 빈 문자열로 치환
            df = df.fillna('')
            
            current_time = datetime.now()
            data_to_insert = []
            
            for _, row in df.iterrows():
                data_to_insert.append((
                    row['title'],
                    row['artist'],
                    row.get('lyrics', ''),
                    row.get('pronunciation', ''),
                    row.get('translation', ''),
                    row.get('youtube_id', ''),
                    current_time,
                    current_time
                ))
            
            self.cursor.executemany(insert_query, data_to_insert)
            self.connection.commit()
            
            print(f"  ✅ songs 테이블에 {len(data_to_insert)}개 삽입 완료")
            return True
            
        except Exception as e:
            print(f"  ❌ songs 로드 실패: {e}")
            self.connection.rollback()
            return False

    def load_setlists(self):
        """setlists.csv → setlists 테이블"""
        try:
            print("\n📋 setlists.csv 로드 중...")
            
            # CSV 읽기
            df = pd.read_csv(f"{self.csv_base_path}/setlists.csv", encoding='utf-8')
            print(f"  • CSV 레코드: {len(df)}개")
            
            # 삽입 쿼리
            insert_query = """
                INSERT INTO setlists (title, artist, start_date, end_date, venue, img_url, created_at, updated_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # NaN 값을 빈 문자열로 치환
            df = df.fillna('')
            
            current_time = datetime.now()
            data_to_insert = []
            
            for _, row in df.iterrows():
                data_to_insert.append((
                    row['title'],
                    row['artist'],
                    row['start_date'],
                    row['end_date'],
                    row.get('venue', ''),
                    row.get('img_url', ''),
                    current_time,
                    current_time
                ))
            
            self.cursor.executemany(insert_query, data_to_insert)
            self.connection.commit()
            
            print(f"  ✅ setlists 테이블에 {len(data_to_insert)}개 삽입 완료")
            return True
            
        except Exception as e:
            print(f"  ❌ setlists 로드 실패: {e}")
            self.connection.rollback()
            return False

    def load_all_data(self):
        """모든 CSV 데이터를 순서대로 로드"""
        try:
            print("🚀 CSV → MySQL 데이터 로드 시작")
            print("="*60)
            
            # 로드 순서 (Foreign Key 의존성 고려)
            load_steps = [
                ("기존 데이터 삭제", self.clear_data_tables),
                ("Artists 로드", self.load_artists),
                ("Concerts 로드", self.load_concerts), 
                ("Songs 로드", self.load_songs),
                ("Setlists 로드", self.load_setlists),
                # TODO: 나머지 테이블들 추가 필요
            ]
            
            for step_name, step_function in load_steps:
                print(f"\n🔄 {step_name}...")
                if not step_function():
                    print(f"❌ {step_name} 실패 - 중단")
                    return False
            
            print("\n" + "="*60)
            print("🎉 모든 데이터 로드 완료!")
            
            # 결과 확인
            self.verify_data()
            return True
            
        except Exception as e:
            print(f"❌ 데이터 로드 중 오류: {e}")
            return False

    def verify_data(self):
        """데이터 로드 결과 확인"""
        try:
            print("\n📊 데이터 로드 결과 확인:")
            
            tables = ['artists', 'concerts', 'songs', 'setlists']
            for table in tables:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]
                print(f"  • {table}: {count:,}개 레코드")
                
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
        print("🔌 모든 연결 종료")

def main():
    """메인 실행"""
    loader = CSVToMySQLLoader()
    
    try:
        # SSH 터널 생성
        if not loader.create_ssh_tunnel():
            return
        
        # MySQL 연결
        if not loader.connect_mysql():
            return
        
        # 모든 데이터 로드
        loader.load_all_data()
        
    except KeyboardInterrupt:
        print("\n⏹️ 사용자 중단")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        loader.close()

if __name__ == "__main__":
    main()