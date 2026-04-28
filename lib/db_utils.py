#!/usr/bin/env python3
"""
데이터베이스 연결 유틸리티
SSH 터널을 열고 MySQL에 연결하는 모든 과정을 담당
"""
import time
import mysql.connector
from mysql.connector import Error
from pathlib import Path
from lib.platform_utils import create_cross_platform_subprocess
from lib.config import Config


class DatabaseManager:
    #SSH 터널 생성 → MySQL 연결 → 쿼리 실행 → 연결 종료

    def __init__(self, db_name: str = None, local_port: int = 3307):
        #DatabaseManager 초기화
        self.ssh_process = None
        self.connection = None
        self.cursor = None
        self.project_root = Path(__file__).parent.parent
        self.db_name = db_name or Config.DB_NAME
        self.local_port = local_port

    def create_ssh_tunnel(self, ssh_config=None):
        #SSH 터널 생성 (DB 접근용 중간 다리)
        if ssh_config is None:
            ssh_config = {
                'key_path': Config.get_ssh_key_path(),  # SSH 키 파일 경로 (없으면 에러)
                'host': f"{Config.DB_SSH_USER}@{Config.DB_SSH_HOST}",
                'remote_host': Config.DB_HOST,
                'remote_port': Config.DB_PORT,
                'local_port': self.local_port
            }

        try:
            print(" SSH 터널 생성 중...")

            ssh_command = [
                'ssh',
                '-i', ssh_config['key_path'],
                '-L', f"{ssh_config['local_port']}:{ssh_config['remote_host']}:{ssh_config['remote_port']}",
                '-N',
                '-o', 'StrictHostKeyChecking=no',
                ssh_config['host']
            ]

            # platform_utils로 OS에 맞는 방식으로 subprocess 생성
            self.ssh_process = create_cross_platform_subprocess(
                ssh_command,
                stdout=None,
                stderr=None
            )

            time.sleep(3)  # 터널 연결 대기

            if self.ssh_process.poll() is None:
                print(" SSH 터널 연결 성공")
                return True
            else:
                print(" SSH 터널 생성 실패")
                return False

        except Exception as e:
            print(f" SSH 터널 오류: {e}")
            return False

    def connect_mysql(self, config=None):
        #SSH 터널을 통해 MySQL 연결
        if config is None:
            config = {
                'host': '127.0.0.1',
                'port': self.local_port,
                'user': Config.DB_USER,
                'password': Config.DB_PASSWORD,
                'database': self.db_name,
                'charset': 'utf8mb4',
                'collation': 'utf8mb4_unicode_ci',
                'use_unicode': True
            }

        try:
            print(" MySQL 연결 중...")

            self.connection = mysql.connector.connect(**config)
            self.cursor = self.connection.cursor()

            # 한글 깨짐 방지를 위한 문자셋 설정
            self.cursor.execute("SET NAMES utf8mb4")
            self.cursor.execute("SET CHARACTER SET utf8mb4")
            self.cursor.execute("SET character_set_connection=utf8mb4")

            print(" MySQL 연결 성공!")
            return True

        except Error as e:
            print(f" MySQL 연결 실패: {e}")
            return False

    def connect_with_ssh(self, ssh_config=None, mysql_config=None):
        #SSH 터널 생성 + MySQL 연결 한번에
        if not self.create_ssh_tunnel(ssh_config):
            return False

        return self.connect_mysql(mysql_config)

    def disconnect(self):
        #커서, DB 연결, SSH 터널 순으로 종료
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        if self.ssh_process:
            self.ssh_process.terminate()
            print(" 연결 종료")

    def get_data_path(self, filename=""):
        #main_output 디렉토리 경로 반환
        data_path = self.project_root / "data" / "main_output"
        if filename:
            return str(data_path / filename)
        return str(data_path)

    def get_backup_path(self, filename=""):
        #backups 디렉토리 경로 반환
        backup_path = self.project_root / "data" / "backups"
        backup_path.mkdir(parents=True, exist_ok=True)
        if filename:
            return str(backup_path / filename)
        return str(backup_path)

    def execute_query(self, query, params=None):
        #SQL 쿼리 실행 후 결과 반환
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except Error as e:
            print(f" 쿼리 실행 실패: {e}")
            return None

    def get_all_concert_codes_from_db(self):
        #DB에서 모든 콘서트 코드 조회 (중복 수집 방지용)
        try:
            self.cursor.execute("SELECT code FROM concerts")
            results = self.cursor.fetchall()
            return {row[0] for row in results}
        except Error as e:
            print(f" DB에서 콘서트 코드 조회 실패: {e}")
            return set()

    def commit(self):
        #트랜잭션 커밋 (변경사항 저장)
        if self.connection:
            self.connection.commit()

    def rollback(self):
        #트랜잭션 롤백 (변경사항 취소)
        if self.connection:
            self.connection.rollback()


def get_db_manager():
    #프로덕션 DB 매니저 반환
    return DatabaseManager()

def get_dev_db_manager():
    #개발 DB 매니저 반환 (DB 이름, 포트만 다름)
    return DatabaseManager(db_name=Config.DEV_DB_NAME, local_port=3308)

def get_stage_db_manager():
    #스테이지 DB 매니저 반환
    return DatabaseManager(db_name=Config.STAGE_DB_NAME, local_port=3308)
