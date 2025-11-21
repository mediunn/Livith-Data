#!/usr/bin/env python3
"""
테이블 구조 분석 및 UNIQUE 제약조건 확인 스크립트
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

class TableConstraintAnalyzer:
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
                'database': 'livith_service',
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

    def analyze_table_indexes(self, table_name):
        """테이블의 인덱스 분석"""
        try:
            print(f"\n📊 {table_name} 테이블 인덱스 분석:")
            
            # 인덱스 정보 조회
            self.cursor.execute(f"SHOW INDEX FROM {table_name}")
            indexes = self.cursor.fetchall()
            
            if indexes:
                unique_indexes = {}
                for idx in indexes:
                    key_name = idx[2]
                    column_name = idx[4]
                    non_unique = idx[1]
                    
                    if key_name not in unique_indexes:
                        unique_indexes[key_name] = {
                            'columns': [],
                            'unique': non_unique == 0
                        }
                    unique_indexes[key_name]['columns'].append(column_name)
                
                for key_name, info in unique_indexes.items():
                    unique_str = "UNIQUE" if info['unique'] else "INDEX"
                    columns_str = ", ".join(info['columns'])
                    print(f"  • {key_name}: {unique_str} ({columns_str})")
            else:
                print("  • 인덱스 없음")
            
            return indexes
            
        except Exception as e:
            print(f"  ❌ 인덱스 분석 실패: {e}")
            return []

    def analyze_all_tables(self):
        """모든 주요 테이블 분석"""
        tables = [
            'artists', 'concerts', 'songs', 'setlists',
            'concert_info', 'cultures', 'md', 'schedule',
            'concert_genres', 'concert_setlists', 'setlist_songs'
        ]
        
        table_info = {}
        
        for table in tables:
            print(f"\n{'='*50}")
            print(f"🔍 {table} 테이블 분석")
            print('='*50)
            
            # 테이블 구조
            self.cursor.execute(f"DESCRIBE {table}")
            columns = self.cursor.fetchall()
            
            print("\n📋 컬럼 구조:")
            for col in columns:
                field, type_, null, key, default, extra = col
                print(f"  • {field:<20} {type_:<20} {key:<5} {null:<5}")
            
            # 인덱스 분석
            self.analyze_table_indexes(table)
            
            # 현재 레코드 수
            self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = self.cursor.fetchone()[0]
            print(f"\n📊 현재 레코드 수: {count}개")
            
            table_info[table] = {
                'columns': columns,
                'count': count
            }
        
        return table_info

    def suggest_unique_constraints(self):
        """각 테이블별 UNIQUE 제약조건 제안"""
        print("\n" + "="*60)
        print("💡 권장 UNIQUE 제약조건")
        print("="*60)
        
        suggestions = {
            'artists': ['artist'],  # 아티스트명은 고유해야 함
            'concerts': ['title', 'code'],  # 콘서트 제목 또는 코드는 고유
            'songs': ['(title, artist)'],  # 제목+아티스트 조합은 고유
            'setlists': ['(title, artist)'],  # 세트리스트 제목+아티스트 조합은 고유
            'concert_info': ['(concert_id, category)'],  # 콘서트별 카테고리는 고유
            'cultures': ['(concert_id, title)'],  # 콘서트별 문화 제목은 고유
            'md': ['(concert_id, name)'],  # 콘서트별 MD명은 고유
            'schedule': ['(concert_id, scheduled_at, type)'],  # 콘서트별 일정은 고유
            'concert_genres': ['(concert_id, genre_id)'],  # 콘서트별 장르는 고유
            'concert_setlists': ['(concert_id, setlist_id)'],  # 콘서트-세트리스트 연결은 고유
            'setlist_songs': ['(setlist_id, song_id, sorted_index)']  # 세트리스트별 곡 순서는 고유
        }
        
        for table, constraints in suggestions.items():
            print(f"\n📌 {table}:")
            for constraint in constraints:
                print(f"  • UNIQUE KEY: {constraint}")

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
        """전체 분석 프로세스 실행"""
        try:
            print("\n" + "="*60)
            print("🔍 데이터베이스 테이블 제약조건 분석")
            print("="*60)
            
            # SSH 터널 생성
            if not self.create_ssh_tunnel():
                print("❌ SSH 터널 생성 실패")
                return
            
            # MySQL 연결
            if not self.connect_mysql():
                print("❌ MySQL 연결 실패")
                return
            
            # 모든 테이블 분석
            table_info = self.analyze_all_tables()
            
            # UNIQUE 제약조건 제안
            self.suggest_unique_constraints()
            
        except KeyboardInterrupt:
            print("\n\n⚠️ 사용자에 의해 중단됨")
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    analyzer = TableConstraintAnalyzer()
    analyzer.run()