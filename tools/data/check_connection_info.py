#!/usr/bin/env python3
"""
네트워크 연결 상태 및 MySQL 데이터베이스 연결 정보를 확인하는 스크립트
"""
import socket
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from lib.config import Config
from lib.db_utils import get_db_manager

def check_connection_details():
    """SSH 터널을 통한 연결 정보 단계별 확인"""
    local_port = 3307

    print("🔍 SSH 터널을 통한 MySQL 연결 확인")
    print("="*50)

    # 1. SSH 터널 확인
    print(f"\n1️⃣ SSH 터널 상태 확인 (localhost:{local_port})")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', local_port))
        sock.close()

        if result == 0:
            print(f"✅ SSH 터널 활성화 확인 - 포트 {local_port}이 열려있음")
        else:
            print("❌ SSH 터널이 실행되지 않음")
            print(f"\n🚨 SSH 터널을 먼저 실행하세요:")
            print(f"   ssh -i {Config.LIVITH_SSH_KEY_PATH} \\")
            print(f"       -L {local_port}:{Config.DB_HOST}:{Config.DB_PORT} \\")
            print(f"       {Config.DB_SSH_USER}@{Config.DB_SSH_HOST} -N &")
            return False

    except Exception as e:
        print(f"❌ 네트워크 오류: {e}")
        return False

    # 2. MySQL 연결 테스트
    print(f"\n2️⃣ MySQL 연결 테스트 (SSH 터널 경유)")
    db = get_db_manager()
    if not db.connect_with_ssh():
        print("❌ MySQL 연결 실패")
        return False

    try:
        db.cursor.execute("SELECT VERSION()")
        version = db.cursor.fetchone()
        print(f"   ✅ MySQL 연결 성공!")
        print(f"   📊 MySQL 버전: {version[0]}")

        db.cursor.execute("SHOW DATABASES")
        databases = db.cursor.fetchall()
        print(f"   📁 사용 가능한 데이터베이스:")
        for row in databases:
            mark = "👉" if row[0] == Config.DB_NAME else "   "
            print(f"      {mark} {row[0]}")

        db.cursor.execute("SHOW TABLES")
        tables = db.cursor.fetchall()
        print(f"\n   📋 {Config.DB_NAME} 테이블 ({len(tables)}개):")
        for table in tables[:5]:
            print(f"      - {table[0]}")
        if len(tables) > 5:
            print(f"      ... 외 {len(tables)-5}개")

        print("\n✅ 모든 연결 테스트 성공!")
        return True

    except Exception as e:
        print(f"   ❌ 오류: {e}")
        return False
    finally:
        db.disconnect()


if __name__ == "__main__":
    check_connection_details()
