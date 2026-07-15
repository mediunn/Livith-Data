#!/usr/bin/env python3
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import signal
import os
from datetime import datetime
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from lib.config import Config
from lib.db_utils import get_db_manager

class ConcertDataFixer:
    def __init__(self):
        self.db = None
        self.connection = None
        self.cursor = None

    def connect(self):
        self.db = get_db_manager()
        if self.db.connect_with_ssh():
            self.cursor = self.db.cursor
            self.connection = self.db.connection
            return True
        return False

    def fix_concerts_data(self):
        """concerts 테이블 데이터 수정"""
        try:
            print("\n" + "="*60)
            print("🔧 CONCERTS 테이블 데이터 수정")
            print("="*60)
            
            # 기존 concerts 데이터 확인
            print("📈 기존 concerts 데이터 확인...")
            # 삭제하지 않고 업서트 모드로 진행
            # self.cursor.execute("DELETE FROM concerts")
            # self.connection.commit()
            
            # CSV 파일 읽기
            print("📁 concerts.csv 로드 중...")
            csv_path = str(Config.OUTPUT_DIR / 'concerts.csv')
            df = pd.read_csv(csv_path, encoding='utf-8')
            
            # NaN 값을 빈 문자열로 치환
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            print(f"  • CSV 컬럼: {list(df.columns)}")
            
            # artist_id 매핑용 딕셔너리 생성
            self.cursor.execute("SELECT id, artist FROM artists")
            artist_mapping = {artist: id for id, artist in self.cursor.fetchall()}
            print(f"  • 아티스트 매핑: {len(artist_mapping)}개")
            
            # 수정된 삽입 쿼리 (모든 컬럼 포함)
            insert_query = """
                INSERT INTO concerts (
                    title, artist, artist_id, start_date, end_date, 
                    status, poster, code, sorted_index, ticket_site, 
                    ticket_url, venue, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            current_time = datetime.now()
            data_to_insert = []
            unmapped_artists = []
            
            for _, row in df.iterrows():
                artist_name = row['artist']
                artist_id = artist_mapping.get(artist_name)
                
                if artist_id:
                    # status 값 매핑 (CSV의 UPCOMING을 적절히 변환)
                    csv_status = row.get('status', 'ONGOING')
                    if csv_status == 'UPCOMING':
                        db_status = 'UPCOMING'
                    elif csv_status == 'COMPLETED':
                        db_status = 'COMPLETED'
                    else:
                        db_status = 'ONGOING'
                    
                    # sorted_index 처리 (중복 방지를 위해 각기 다른 값 할당)
                    sorted_index = row.get('sorted_index', '')
                    if sorted_index == '' or pd.isna(sorted_index) or sorted_index == 0:
                        # 0이나 빈 값인 경우 row 번호를 기반으로 고유값 생성
                        sorted_index = len(data_to_insert) + 1  # 1부터 시작하는 고유 번호
                    else:
                        try:
                            sorted_index = int(sorted_index)
                        except:
                            sorted_index = len(data_to_insert) + 1
                    
                    data_to_insert.append((
                        row['title'],                    # title
                        artist_name,                     # artist  
                        artist_id,                       # artist_id
                        row['start_date'],              # start_date
                        row['end_date'],                # end_date
                        db_status,                      # status
                        row.get('poster', ''),          # poster
                        row.get('code', ''),            # code
                        sorted_index,                   # sorted_index
                        row.get('ticket_site', ''),     # ticket_site
                        row.get('ticket_url', ''),      # ticket_url
                        row.get('venue', ''),           # venue
                        current_time,                   # created_at
                        current_time                    # updated_at
                    ))
                else:
                    unmapped_artists.append(artist_name)
            
            if unmapped_artists:
                print(f"  ⚠️ 매핑되지 않은 아티스트 ({len(unmapped_artists)}개):")
                for artist in unmapped_artists[:5]:
                    print(f"     • {artist}")
            
            if data_to_insert:
                print(f"🔄 데이터 업서트 중... ({len(data_to_insert)}개)")
                self.cursor.executemany(upsert_query, data_to_insert)
                self.connection.commit()
                print(f"  ✅ concerts 테이블에 {len(data_to_insert)}개 업서트 완료")
            
            # 결과 확인
            self.verify_concerts_data()
            
            return True
            
        except Exception as e:
            print(f"❌ concerts 데이터 수정 실패: {e}")
            self.connection.rollback()
            return False

    def verify_concerts_data(self):
        """수정된 데이터 확인"""
        try:
            print(f"\n📊 수정 결과 확인:")
            
            # 전체 레코드 수
            self.cursor.execute("SELECT COUNT(*) FROM concerts")
            total_count = self.cursor.fetchone()[0]
            print(f"  • 총 레코드 수: {total_count}개")
            
            # NULL 값 개수 확인
            fields_to_check = [
                'code', 'poster', 'sorted_index', 'ticket_site', 'ticket_url'
            ]
            
            for field in fields_to_check:
                self.cursor.execute(f"SELECT COUNT(*) FROM concerts WHERE {field} IS NOT NULL AND {field} != ''")
                non_null_count = self.cursor.fetchone()[0]
                print(f"  • {field}: {non_null_count}/{total_count}개 데이터 있음")
            
            # status 값 분포 확인
            self.cursor.execute("SELECT status, COUNT(*) FROM concerts GROUP BY status")
            status_counts = self.cursor.fetchall()
            print(f"  • status 분포:")
            for status, count in status_counts:
                print(f"    - {status}: {count}개")
            
            # 샘플 데이터 확인
            print(f"\n📋 샘플 데이터 (처음 2개):")
            self.cursor.execute("""
                SELECT title, code, poster, sorted_index, ticket_site, status 
                FROM concerts 
                LIMIT 2
            """)
            samples = self.cursor.fetchall()
            
            for i, sample in enumerate(samples, 1):
                title, code, poster, sorted_index, ticket_site, status = sample
                print(f"  {i}. {title[:30]}...")
                print(f"     code: {code}")
                print(f"     poster: {'있음' if poster else '없음'}")
                print(f"     sorted_index: {sorted_index}")
                print(f"     ticket_site: {ticket_site}")
                print(f"     status: {status}")
                
        except Exception as e:
            print(f"❌ 검증 실패: {e}")

    def close(self):
        """연결 종료"""
        if self.db:
            self.db.disconnect()
        print("🔌 연결 종료")

def main():
    """메인 실행"""
    fixer = ConcertDataFixer()

    try:
        if not fixer.connect():
            return

        fixer.fix_concerts_data()
        
    except KeyboardInterrupt:
        print("\n⏹️ 사용자 중단")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        fixer.close()

if __name__ == "__main__":
    main()