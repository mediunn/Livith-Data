#!/usr/bin/env python3
import subprocess
import mysql.connector
from mysql.connector import Error
import time
import signal
import os

class SimpleSSHMySQL:
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None

    def create_ssh_tunnel(self):
        """SSH 터널 생성"""
        try:
            print("🔧 SSH 터널 생성 중...")
            
            # SSH 터널 명령어
            # -L : 로컬 포트 포워딩
            # 3307:RDS엔드포인트:3306 : 로컬3307 -> RDS3306으로 포워딩
            # -N : 명령어 실행하지 않고 터널만 유지
            # -f : 백그라운드 실행
            ssh_command = [
                'ssh',
                '-i', '/Users/youz2me/Downloads/livith-key.pem',
                '-L', '3307:livithdb.c142i2022qs5.ap-northeast-2.rds.amazonaws.com:3306',
                '-N',
                '-o', 'StrictHostKeyChecking=no',
                'ubuntu@43.203.48.65'
            ]
            
            print(f"실행 명령어: {' '.join(ssh_command)}")
            
            # SSH 터널 실행
            self.ssh_process = subprocess.Popen(
                ssh_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid
            )
            
            # 터널이 생성될 시간 대기
            time.sleep(3)
            
            # 프로세스 상태 확인
            if self.ssh_process.poll() is None:
                print("✅ SSH 터널 생성 완료!")
                return True
            else:
                stdout, stderr = self.ssh_process.communicate()
                print(f"❌ SSH 터널 생성 실패:")
                print(f"STDOUT: {stdout.decode()}")
                print(f"STDERR: {stderr.decode()}")
                return False
                
        except Exception as e:
            print(f"❌ SSH 터널 오류: {e}")
            return False

    def connect_mysql(self):
        """MySQL 연결"""
        try:
            print("🔌 MySQL 연결 중...")
            
            config = {
                'host': '127.0.0.1',      # SSH 터널을 통해 로컬 연결
                'port': 3307,             # SSH 터널 로컬 포트
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

    def test_connection(self):
        """연결 테스트"""
        try:
            # MySQL 버전 확인
            self.cursor.execute("SELECT VERSION()")
            version = self.cursor.fetchone()
            print(f"📊 MySQL 버전: {version[0]}")
            
            # 현재 데이터베이스 확인
            self.cursor.execute("SELECT DATABASE()")
            current_db = self.cursor.fetchone()
            print(f"📁 현재 데이터베이스: {current_db[0]}")
            
            # 테이블 목록 확인
            self.cursor.execute("SHOW TABLES")
            tables = self.cursor.fetchall()
            
            print(f"\n📋 테이블 목록 ({len(tables)}개):")
            table_names = []
            for i, table in enumerate(tables, 1):
                table_name = table[0]
                table_names.append(table_name)
                print(f"  {i:2d}. {table_name}")
            
            return table_names
            
        except Exception as e:
            print(f"❌ 테스트 실패: {e}")
            return []

    def get_table_structure(self, table_name):
        """테이블 구조 확인"""
        try:
            print(f"\n{'='*60}")
            print(f"📊 {table_name} 테이블 구조")
            print('='*60)
            
            self.cursor.execute(f"DESCRIBE {table_name}")
            columns = self.cursor.fetchall()
            
            print(f"{'컬럼명':<25} {'타입':<20} {'NULL':<6} {'KEY':<6} {'기본값':<15} {'Extra'}")
            print('-' * 90)
            
            for col in columns:
                field, type_info, null_val, key, default, extra = col
                default_str = str(default) if default is not None else 'NULL'
                print(f"{field:<25} {type_info:<20} {null_val:<6} {key:<6} {default_str:<15} {extra}")
            
            # 테이블 데이터 개수 확인
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = self.cursor.fetchone()
            print(f"\n📊 총 레코드 수: {count[0]:,}개")
            
            return columns
            
        except Exception as e:
            print(f"❌ 테이블 구조 조회 실패 ({table_name}): {e}")
            return []

    def close(self):
        """연결 종료"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        if self.ssh_process:
            try:
                # SSH 프로세스 그룹 전체 종료
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
    ssh_mysql = SimpleSSHMySQL()
    
    try:
        # SSH 터널 생성
        if not ssh_mysql.create_ssh_tunnel():
            return
        
        # MySQL 연결
        if not ssh_mysql.connect_mysql():
            return
        
        # 연결 테스트
        table_names = ssh_mysql.test_connection()
        
        if table_names:
            print(f"\n🎯 모든 테이블 구조 확인:")
            for table_name in table_names:
                ssh_mysql.get_table_structure(table_name)
                
        print(f"\n🎉 데이터베이스 연결 및 분석 완료!")
        print(f"📋 총 {len(table_names)}개 테이블 확인됨")
        
    except KeyboardInterrupt:
        print("\n⏹️ 사용자 중단")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        ssh_mysql.close()

if __name__ == "__main__":
    main()