#!/usr/bin/env python3
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import signal
import os

class ConcertDataChecker:
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None

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

    def check_concert_data(self):
        """concerts 테이블 데이터 확인"""
        try:
            print("\n" + "="*80)
            print("🎪 CONCERTS 테이블 데이터 확인")
            print("="*80)
            
            # 테이블 구조 재확인
            print("\n📊 테이블 구조:")
            self.cursor.execute("DESCRIBE concerts")
            columns = self.cursor.fetchall()
            
            db_columns = []
            for col in columns:
                field, type_info, null_val, key, default, extra = col
                db_columns.append(field)
                print(f"  {field:<20} {type_info:<20} {null_val:<6} {key:<6}")
            
            # 현재 DB 데이터 샘플 확인
            print(f"\n📋 현재 DB 데이터 (처음 5개):")
            self.cursor.execute("SELECT * FROM concerts LIMIT 5")
            db_data = self.cursor.fetchall()
            
            if db_data:
                print(f"  컬럼 순서: {db_columns}")
                for i, row in enumerate(db_data, 1):
                    print(f"\n  {i}번째 레코드:")
                    for j, value in enumerate(row):
                        print(f"    {db_columns[j]}: {value}")
            else:
                print("  ⚠️ 데이터가 없습니다.")
                return
            
            # CSV 파일 데이터와 비교
            print(f"\n📁 CSV 파일 데이터:")
            csv_path = '/Users/youz2me/Xcode/Livith-Data/output/concerts.csv'
            df = pd.read_csv(csv_path, encoding='utf-8')
            
            print(f"  • CSV 레코드 수: {len(df)}개")
            print(f"  • CSV 컬럼: {list(df.columns)}")
            
            # CSV 샘플 데이터 출력
            print(f"\n📄 CSV 데이터 샘플 (처음 3개):")
            for i, (_, row) in enumerate(df.head(3).iterrows(), 1):
                print(f"\n  {i}번째 CSV 레코드:")
                for col in df.columns:
                    value = row[col] if pd.notna(row[col]) else "NULL"
                    print(f"    {col}: {value}")
            
            # 데이터 불일치 확인
            print(f"\n🔍 데이터 비교 분석:")
            
            # DB에서 전체 데이터 가져오기
            self.cursor.execute("SELECT title, artist, start_date, end_date, venue FROM concerts")
            all_db_data = self.cursor.fetchall()
            
            print(f"  • DB 레코드 수: {len(all_db_data)}개")
            print(f"  • CSV 레코드 수: {len(df)}개")
            
            if len(all_db_data) != len(df):
                print(f"  ⚠️ 레코드 수 불일치!")
            
            # 특정 필드들 비교
            print(f"\n🔎 필드별 데이터 확인:")
            
            # NULL 값들 확인
            null_checks = [
                ('start_date', 'SELECT COUNT(*) FROM concerts WHERE start_date IS NULL OR start_date = ""'),
                ('end_date', 'SELECT COUNT(*) FROM concerts WHERE end_date IS NULL OR end_date = ""'),
                ('venue', 'SELECT COUNT(*) FROM concerts WHERE venue IS NULL OR venue = ""'),
                ('poster', 'SELECT COUNT(*) FROM concerts WHERE poster IS NULL OR poster = ""'),
            ]
            
            for field_name, query in null_checks:
                self.cursor.execute(query)
                null_count = self.cursor.fetchone()[0]
                print(f"  • {field_name}: {null_count}개 NULL/빈값")
            
            # 데이터 타입별 문제 확인
            print(f"\n⚠️ 잠재적 문제 확인:")
            
            # 날짜 형식 확인
            self.cursor.execute("SELECT DISTINCT start_date FROM concerts WHERE start_date != '' LIMIT 10")
            dates = self.cursor.fetchall()
            print(f"  • start_date 샘플: {[d[0] for d in dates[:5]]}")
            
            return True
            
        except Exception as e:
            print(f"❌ 확인 실패: {e}")
            return False

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
    checker = ConcertDataChecker()
    
    try:
        if not checker.create_ssh_tunnel():
            return
        
        if not checker.connect_mysql():
            return
        
        checker.check_concert_data()
        
    except KeyboardInterrupt:
        print("\n⏹️ 사용자 중단")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        checker.close()

if __name__ == "__main__":
    main()