#!/usr/bin/env python3
"""
SSH 터널을 통해 MySQL 데이터베이스에 안전하게 연결하는 클래스
"""
import mysql.connector
from mysql.connector import Error
from sshtunnel import SSHTunnelForwarder
import logging
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from lib.config import Config

class SSHMySQLConnection:
    def __init__(self, ssh_config, mysql_config):
        """
        SSH 터널을 통한 MySQL 연결
        
        Args:
            ssh_config: SSH 연결 설정
            mysql_config: MySQL 연결 설정
        """
        self.ssh_config = ssh_config
        self.mysql_config = mysql_config
        self.tunnel = None
        self.connection = None
        self.cursor = None
        
        # 로깅 설정
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """SSH 터널 생성 및 MySQL 연결"""
        try:
            # SSH 터널 생성
            self.logger.info("SSH 터널 생성 중...")
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_config['host'], self.ssh_config['port']),
                ssh_username=self.ssh_config['username'],
                ssh_pkey=self.ssh_config['private_key_path'],
                remote_bind_address=(self.mysql_config['host'], self.mysql_config['port']),
                local_bind_address=('127.0.0.1', 0)  # 자동 포트 할당
            )
            
            # SSH 터널 시작
            self.tunnel.start()
            self.logger.info(f"SSH 터널 생성 완료: localhost:{self.tunnel.local_bind_port}")
            
            # MySQL 연결
            self.logger.info("MySQL 연결 중...")
            mysql_config = {
                'host': '127.0.0.1',  # SSH 터널을 통해 로컬로 연결
                'port': self.tunnel.local_bind_port,
                'user': self.mysql_config['user'],
                'password': self.mysql_config['password'],
                'database': self.mysql_config['database'],
                'charset': self.mysql_config.get('charset', 'utf8mb4'),
                'use_unicode': True
            }
            
            self.connection = mysql.connector.connect(**mysql_config)
            self.cursor = self.connection.cursor()
            
            self.logger.info("✅ SSH + MySQL 연결 성공!")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 연결 실패: {e}")
            self.disconnect()
            return False

    def disconnect(self):
        """연결 종료"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        if self.tunnel:
            self.tunnel.stop()
        self.logger.info("🔌 연결 종료")

    def get_table_structure(self, table_name):
        """테이블 구조 조회"""
        try:
            self.cursor.execute(f"DESCRIBE {table_name}")
            columns = self.cursor.fetchall()
            return columns
        except Error as e:
            self.logger.error(f"테이블 구조 조회 실패 ({table_name}): {e}")
            return None

    def test_connection(self):
        """연결 테스트 및 기본 정보 확인"""
        try:
            # MySQL 버전 확인
            self.cursor.execute("SELECT VERSION()")
            version = self.cursor.fetchone()
            self.logger.info(f"📊 MySQL 버전: {version[0]}")
            
            # 데이터베이스 목록 확인
            self.cursor.execute("SHOW DATABASES")
            databases = self.cursor.fetchall()
            self.logger.info("📁 사용 가능한 데이터베이스:")
            for db in databases:
                mark = "👉" if db[0] == self.mysql_config['database'] else "   "
                self.logger.info(f"      {mark} {db[0]}")
            
            # 테이블 목록 확인
            self.cursor.execute(f"USE {self.mysql_config['database']}")
            self.cursor.execute("SHOW TABLES")
            tables = self.cursor.fetchall()
            
            self.logger.info(f"📋 {self.mysql_config['database']} 데이터베이스의 테이블 목록:")
            table_names = []
            for i, table in enumerate(tables, 1):
                table_name = table[0]
                table_names.append(table_name)
                self.logger.info(f"  {i}. {table_name}")
            
            # 각 테이블 구조 확인
            self.logger.info(f"\n🏗️  테이블 구조:")
            for table_name in table_names:
                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"📊 {table_name} 테이블 구조:")
                self.logger.info('='*60)
                
                columns = self.get_table_structure(table_name)
                if columns:
                    self.logger.info(f"{'컬럼명':<20} {'타입':<20} {'NULL':<8} {'KEY':<8} {'기본값':<15} {'Extra'}")
                    self.logger.info('-' * 80)
                    
                    for col in columns:
                        field, type_info, null, key, default, extra = col
                        default_str = str(default) if default is not None else 'NULL'
                        self.logger.info(f"{field:<20} {type_info:<20} {null:<8} {key:<8} {default_str:<15} {extra}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 테스트 실패: {e}")
            return False
    def commit(self):
        """트랜잭션 커밋"""
        if self.connection:
            self.connection.commit()

    def rollback(self):
        """트랜잭션 롤백"""
        if self.connection:
            self.connection.rollback()
            
def main():
    """메인 실행 함수"""

    # SSH 설정
    ssh_config = {
        'host': Config.DB_SSH_HOST,
        'port': Config.DB_SSH_PORT,
        'username': Config.DB_SSH_USER,
        'private_key_path': Config.get_ssh_key_path()
    }

    # MySQL 설정
    mysql_config = {
        'host': Config.DB_HOST,
        'port': Config.DB_PORT,
        'user': Config.DB_USER,
        'password': Config.DB_PASSWORD,
        'database': Config.DB_NAME,
        'charset': 'utf8mb4'
    }
    
    # 연결 테스트
    ssh_mysql = SSHMySQLConnection(ssh_config, mysql_config)
    
    try:
        if ssh_mysql.connect():
            print("🎉 연결 성공!")
            ssh_mysql.test_connection()
        else:
            print("❌ 연결 실패")
            
    except Exception as e:
        print(f"오류 발생: {e}")
    finally:
        ssh_mysql.disconnect()

if __name__ == "__main__":
    main()