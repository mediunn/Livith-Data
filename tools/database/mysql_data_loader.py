#!/usr/bin/env python3
"""
MySQL 데이터베이스에서 데이터를 로드하고 처리하는 유틸리티
"""
import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime
import sys

class MySQLDataLoader:
    def __init__(self, host, port, user, password, database):
        """
        MySQL 데이터 로더 초기화
        
        Args:
            host: MySQL 서버 호스트
            port: MySQL 서버 포트
            user: MySQL 사용자명
            password: MySQL 비밀번호
            database: 데이터베이스 이름
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None
        
        # 로깅 설정
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('mysql_data_load.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """MySQL 데이터베이스에 연결"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                use_unicode=True
            )
            self.cursor = self.connection.cursor()
            self.logger.info(f"MySQL 데이터베이스 '{self.database}' 연결 성공")
            return True
        except Error as e:
            self.logger.error(f"MySQL 연결 실패: {e}")
            return False

    def disconnect(self):
        """MySQL 데이터베이스 연결 종료"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.logger.info("MySQL 연결 종료")

    def get_table_structure(self, table_name):
        """테이블 구조 조회"""
        try:
            self.cursor.execute(f"DESCRIBE {table_name}")
            columns = self.cursor.fetchall()
            self.logger.info(f"테이블 '{table_name}' 구조:")
            for col in columns:
                self.logger.info(f"  {col[0]} - {col[1]} - {col[2]} - {col[3]} - {col[4]} - {col[5]}")
            return columns
        except Error as e:
            self.logger.error(f"테이블 구조 조회 실패 ({table_name}): {e}")
            return None

    def clear_table(self, table_name):
        """테이블 데이터 삭제 (Foreign Key 제약조건 고려)"""
        try:
            # Foreign Key 체크 비활성화
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # 기존 데이터 유지 (업서트 모드)
            # self.cursor.execute(f"TRUNCATE TABLE {table_name}")
            # self.connection.commit()
            
            # Foreign Key 체크 재활성화
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            self.logger.info(f"테이블 '{table_name}' 데이터 삭제 완료")
            return True
        except Error as e:
            self.logger.error(f"테이블 데이터 삭제 실패 ({table_name}): {e}")
            self.connection.rollback()
            return False

    def load_artists(self, csv_path):
        """artists 테이블에 데이터 로드"""
        try:
            df = pd.read_csv(csv_path, encoding='utf-8')
            self.logger.info(f"artists.csv 로드: {len(df)} 행")
            
            # 테이블 구조에 맞게 조정 (예시)
            insert_query = """
                INSERT INTO artists (name, created_at, updated_at) 
                VALUES (%s, %s, %s)
            """
            
            current_time = datetime.now()
            data_to_insert = []
            
            # CSV가 헤더가 없는 경우 (첫 번째 컬럼이 아티스트명)
            if 'artist' in df.columns:
                artist_names = df['artist'].tolist()
            else:
                artist_names = df.iloc[:, 0].tolist()
            
            for artist_name in artist_names:
                data_to_insert.append((artist_name, current_time, current_time))
            
            self.cursor.executemany(upsert_query, data_to_insert)
            self.connection.commit()
            
            self.logger.info(f"artists 테이블에 {len(data_to_insert)}개 데이터 삽입 완료")
            return True
            
        except Error as e:
            self.logger.error(f"artists 데이터 로드 실패: {e}")
            self.connection.rollback()
            return False

    def load_concerts(self, csv_path):
        """concerts 테이블에 데이터 로드"""
        try:
            df = pd.read_csv(csv_path, encoding='utf-8')
            self.logger.info(f"concerts.csv 로드: {len(df)} 행")
            
            # 예시 쿼리 (실제 테이블 구조에 맞게 수정 필요)
            insert_query = """
                INSERT INTO concerts (title, artist_id, venue, date, created_at, updated_at) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            current_time = datetime.now()
            data_to_insert = []
            
            for _, row in df.iterrows():
                # artist_id를 artists 테이블에서 조회
                artist_query = "SELECT id FROM artists WHERE name = %s"
                self.cursor.execute(artist_query, (row['artist'],))
                artist_result = self.cursor.fetchone()
                
                if artist_result:
                    artist_id = artist_result[0]
                    data_to_insert.append((
                        row['title'],
                        artist_id,
                        row.get('venue', ''),
                        row.get('date', None),
                        current_time,
                        current_time
                    ))
                else:
                    self.logger.warning(f"아티스트를 찾을 수 없음: {row['artist']}")
            
            self.cursor.executemany(insert_query, data_to_insert)
            self.connection.commit()
            
            self.logger.info(f"concerts 테이블에 {len(data_to_insert)}개 데이터 삽입 완료")
            return True
            
        except Error as e:
            self.logger.error(f"concerts 데이터 로드 실패: {e}")
            self.connection.rollback()
            return False

    def load_songs(self, csv_path):
        """songs 테이블에 데이터 로드"""
        try:
            df = pd.read_csv(csv_path, encoding='utf-8')
            self.logger.info(f"songs.csv 로드: {len(df)} 행")
            
            insert_query = """
                INSERT INTO songs (title, artist_id, lyrics, pronunciation, translation, 
                                 youtube_id, musixmatch_url, created_at, updated_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            current_time = datetime.now()
            data_to_insert = []
            
            for _, row in df.iterrows():
                # artist_id를 artists 테이블에서 조회
                artist_query = "SELECT id FROM artists WHERE name = %s"
                self.cursor.execute(artist_query, (row['artist'],))
                artist_result = self.cursor.fetchone()
                
                if artist_result:
                    artist_id = artist_result[0]
                    data_to_insert.append((
                        row['title'],
                        artist_id,
                        row.get('lyrics', ''),
                        row.get('pronunciation', ''),
                        row.get('translation', ''),
                        row.get('youtube_id', ''),
                        row.get('musixmatch_url', ''),
                        current_time,
                        current_time
                    ))
                else:
                    self.logger.warning(f"아티스트를 찾을 수 없음: {row['artist']}")
            
            self.cursor.executemany(insert_query, data_to_insert)
            self.connection.commit()
            
            self.logger.info(f"songs 테이블에 {len(data_to_insert)}개 데이터 삽입 완료")
            return True
            
        except Error as e:
            self.logger.error(f"songs 데이터 로드 실패: {e}")
            self.connection.rollback()
            return False

    def load_all_data(self, csv_base_path):
        """모든 CSV 데이터를 MySQL에 로드"""
        try:
            # Foreign Key 관계를 고려한 삽입 순서
            load_sequence = [
                ('artists', 'artists.csv', self.load_artists),
                # ('concerts', 'concerts.csv', self.load_concerts),
                # ('songs', 'songs.csv', self.load_songs),
                # 추가 테이블들...
            ]
            
            self.logger.info("=== 데이터 로드 시작 ===")
            
            for table_name, csv_file, load_function in load_sequence:
                self.logger.info(f"\n{table_name} 테이블 로드 시작...")
                
                # 테이블 구조 확인
                self.get_table_structure(table_name)
                
                # 기존 데이터 삭제 (선택사항)
                # self.clear_table(table_name)
                
                # 데이터 로드
                csv_path = f"{csv_base_path}/{csv_file}"
                success = load_function(csv_path)
                
                if not success:
                    self.logger.error(f"{table_name} 로드 실패")
                    return False
                    
                self.logger.info(f"{table_name} 로드 완료")
            
            self.logger.info("=== 모든 데이터 로드 완료 ===")
            return True
            
        except Exception as e:
            self.logger.error(f"데이터 로드 중 오류: {e}")
            return False

def main():
    """메인 실행 함수"""
    # 실제 연결 정보
    config = {
        'host': 'localhost',       # MySQL 서버 호스트
        'port': 3307,             # MySQL 포트
        'user': 'root',           # MySQL 사용자명
        'password': 'livith0407', # MySQL 비밀번호
        'database': 'livith_v2'   # 데이터베이스 이름
    }
    
    # CSV 파일들이 있는 경로
    csv_base_path = '/Users/youz2me/Xcode/Livith-Data/output'
    
    # 데이터 로더 생성
    loader = MySQLDataLoader(**config)
    
    try:
        # 데이터베이스 연결
        if not loader.connect():
            print("데이터베이스 연결 실패")
            return
        
        # 모든 테이블 구조 확인
        tables = ['artists', 'concerts', 'songs', 'setlists', 'concert_info', 
                 'concert_setlists', 'cultures', 'schedule', 'setlist_songs']
        
        print("=== 테이블 구조 확인 ===")
        for table in tables:
            print(f"\n{table} 테이블:")
            loader.get_table_structure(table)
        
        # 실제 데이터 로드는 테이블 구조 확인 후 주석 해제
        # loader.load_all_data(csv_base_path)
        
    except Exception as e:
        print(f"오류 발생: {e}")
    finally:
        loader.disconnect()

if __name__ == "__main__":
    main()