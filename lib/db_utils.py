#!/usr/bin/env python3
"""
데이터베이스 관련 공통 유틸리티
"""
import os
import sys
import time
import mysql.connector
from mysql.connector import Error
from pathlib import Path
from lib.platform_utils import create_cross_platform_subprocess
from lib.config import Config


class DatabaseManager:
    """MySQL 데이터베이스 연결 및 SSH 터널 관리"""
    
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None
        self.project_root = Path(__file__).parent.parent
        
    def create_ssh_tunnel(self, ssh_config=None):
        """SSH 터널 생성"""
        if ssh_config is None:
            ssh_config = {
                'key_path': Config.get_ssh_key_path(),
                'host': f"{Config.DB_SSH_USER}@{Config.DB_SSH_HOST}",
                'remote_host': Config.DB_HOST,
                'remote_port': Config.DB_PORT,
                'local_port': 3307
            }
        
        try:
            print("🔧 SSH 터널 생성 중...")
            
            ssh_command = [
                'ssh',
                '-i', ssh_config['key_path'],
                '-L', f"{ssh_config['local_port']}:{ssh_config['remote_host']}:{ssh_config['remote_port']}",
                '-N',
                '-o', 'StrictHostKeyChecking=no',
                ssh_config['host']
            ]
            
            self.ssh_process = create_cross_platform_subprocess(
                ssh_command,
                stdout=None,
                stderr=None
            )
            
            time.sleep(3)
            
            if self.ssh_process.poll() is None:
                print("✅ SSH 터널 생성 완료!")
                return True
            else:
                print("❌ SSH 터널 생성 실패")
                return False
                
        except Exception as e:
            print(f"❌ SSH 터널 오류: {e}")
            return False

    def connect_mysql(self, config=None):
        """MySQL 연결"""
        if config is None:
            config = {
                'host': '127.0.0.1',
                'port': 3307,
                'user': Config.DB_USER,
                'password': Config.DB_PASSWORD,
                'database': Config.DB_NAME,
                'charset': 'utf8mb4',
                'use_unicode': True
            }
        
        try:
            print("🔌 MySQL 연결 중...")
            
            self.connection = mysql.connector.connect(**config)
            self.cursor = self.connection.cursor()
            
            print("✅ MySQL 연결 성공!")
            return True
            
        except Error as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False

    def connect_with_ssh(self, ssh_config=None, mysql_config=None):
        """SSH 터널 + MySQL 연결 한번에"""
        if not self.create_ssh_tunnel(ssh_config):
            return False
        
        return self.connect_mysql(mysql_config)

    def disconnect(self):
        """모든 연결 종료"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        if self.ssh_process:
            self.ssh_process.terminate()
            print("🔌 연결 종료")

    def get_data_path(self, filename=""):
        """프로젝트 내 데이터 경로 반환"""
        data_path = self.project_root / "data" / "main_output"
        if filename:
            return str(data_path / filename)
        return str(data_path)

    def get_backup_path(self, filename=""):
        """백업 디렉토리 경로 반환"""
        backup_path = self.project_root / "data" / "backups"
        backup_path.mkdir(parents=True, exist_ok=True)
        if filename:
            return str(backup_path / filename)
        return str(backup_path)

    def execute_query(self, query, params=None):
        """쿼리 실행"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except Error as e:
            print(f"❌ 쿼리 실행 실패: {e}")
            return None

    def commit(self):
        """트랜잭션 커밋"""
        if self.connection:
            self.connection.commit()


def get_db_manager():
    """DatabaseManager 인스턴스 반환"""
    return DatabaseManager()