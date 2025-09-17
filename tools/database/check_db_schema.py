#!/usr/bin/env python3
"""
데이터베이스 테이블 구조를 확인하는 스크립트
"""
import subprocess
import mysql.connector
from mysql.connector import Error
import time
import signal
import os
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from lib.config import Config

class DatabaseSchemaChecker:
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
                '-i', Config.get_ssh_key_path(),
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

    def show_all_tables(self):
        """모든 테이블 목록 조회"""
        try:
            print("\n📋 모든 테이블 목록:")
            self.cursor.execute("SHOW TABLES")
            tables = [table[0] for table in self.cursor.fetchall()]
            
            for i, table in enumerate(tables, 1):
                print(f"  {i:2d}. {table}")
            
            return tables
            
        except Exception as e:
            print(f"❌ 테이블 목록 조회 실패: {e}")
            return []

    def describe_table(self, table_name):
        """테이블 구조 조회"""
        try:
            print(f"\n🔍 {table_name} 테이블 구조:")
            self.cursor.execute(f"DESCRIBE {table_name}")
            columns = self.cursor.fetchall()
            
            print("  컬럼명              | 타입                | Null | Key | Default | Extra")
            print("  " + "-" * 70)
            for col in columns:
                field, type_, null, key, default, extra = col
                print(f"  {field:<18} | {type_:<18} | {null:<4} | {key:<3} | {str(default):<7} | {extra}")
            
            return columns
            
        except Exception as e:
            print(f"❌ {table_name} 구조 조회 실패: {e}")
            return []

    def get_sample_data(self, table_name, limit=3):
        """샘플 데이터 조회"""
        try:
            print(f"\n📊 {table_name} 샘플 데이터 ({limit}개):")
            self.cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
            rows = self.cursor.fetchall()
            
            if rows:
                # 컬럼명 가져오기
                self.cursor.execute(f"DESCRIBE {table_name}")
                columns = [col[0] for col in self.cursor.fetchall()]
                
                # 헤더 출력
                header = " | ".join([col[:15] for col in columns])
                print(f"  {header}")
                print("  " + "-" * len(header))
                
                # 데이터 출력
                for row in rows:
                    row_str = " | ".join([str(val)[:15] if val is not None else "NULL" for val in row])
                    print(f"  {row_str}")
            else:
                print("  (데이터 없음)")
                
        except Exception as e:
            print(f"❌ {table_name} 데이터 조회 실패: {e}")

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
        """전체 스키마 확인 프로세스 실행"""
        try:
            print("\n" + "="*60)
            print("🔍 데이터베이스 스키마 확인")
            print("="*60)
            
            # SSH 터널 생성
            if not self.create_ssh_tunnel():
                print("❌ SSH 터널 생성 실패")
                return
            
            # MySQL 연결
            if not self.connect_mysql():
                print("❌ MySQL 연결 실패")
                return
            
            # 모든 테이블 목록 조회
            tables = self.show_all_tables()
            
            # 관심 있는 테이블들 상세 조회
            important_tables = [
                'artist', 'artists', 'concert_genres', 'concert_info', 
                'concert_setlists', 'concerts', 'cultures', 
                'home_concert_sections', 'home_sections', 
                'md', 'schedule', 'search_concert_sections', 
                'search_sections', 'setlists', 'songs'
            ]
            
            existing_tables = [table for table in important_tables if table in tables]
            
            print(f"\n📋 중요 테이블 중 존재하는 테이블 ({len(existing_tables)}개):")
            for table in existing_tables:
                print(f"  • {table}")
            
            # 각 테이블 상세 정보
            for table in existing_tables[:10]:  # 처음 10개만
                self.describe_table(table)
                self.get_sample_data(table)
                print()
            
        except KeyboardInterrupt:
            print("\n\n⚠️ 사용자에 의해 중단됨")
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    checker = DatabaseSchemaChecker()
    checker.run()