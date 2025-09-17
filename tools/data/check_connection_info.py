#!/usr/bin/env python3
"""
네트워크 연결 상태 및 MySQL 데이터베이스 연결 정보를 확인하는 스크립트
"""
import socket
import mysql.connector
from mysql.connector import Error

def check_connection_details():
    """SSH 터널을 통한 연결 정보 단계별 확인"""
    
    # SSH 터널을 통한 로컬 연결 설정
    host = 'localhost'  # SSH 터널을 통해 연결
    port = 3307  # 로컬 포워딩 포트
    
    print("🔍 SSH 터널을 통한 MySQL 연결 확인")
    print("="*50)
    
    # 1. SSH 터널 확인 (localhost:3307)
    print(f"\n1️⃣ SSH 터널 상태 확인 ({host}:{port})")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print("✅ SSH 터널 활성화 확인 - 포트 3307이 열려있음")
        else:
            print("❌ SSH 터널이 실행되지 않음")
            print("\n🚨 SSH 터널을 먼저 실행하세요:")
            print("   ssh -i /path/to/livith-key.pem \\")
            print("       -L 3307:livithdb.c142i2022qs5.ap-northeast-2.rds.amazonaws.com:3306 \\")
            print("       ubuntu@43.203.48.65 -N &")
            print("\n   또는 별도 터미널에서 실행 (백그라운드 & 없이):")
            print("   ssh -i /path/to/livith-key.pem \\")
            print("       -L 3307:livithdb.c142i2022qs5.ap-northeast-2.rds.amazonaws.com:3306 \\")
            print("       ubuntu@43.203.48.65 -N")
            return False
            
    except Exception as e:
        print(f"❌ 네트워크 오류: {e}")
        return False
    
    # 2. MySQL 연결 테스트 (SSH 터널을 통해)
    print(f"\n2️⃣ MySQL 연결 테스트 (SSH 터널 경유)")
    config = {
        'host': 'localhost',  # SSH 터널을 통해 연결
        'port': 3307,         # 로컬 포워딩 포트
        'user': 'root',
        'password': 'livith0407',
        'database': 'livith_v3',
        'charset': 'utf8mb4'
    }
    
    print(f"   연결 정보: {config['host']}:{config['port']} (SSH 터널 → RDS)")
    try:
        connection = mysql.connector.connect(**config)
        print("   ✅ MySQL 연결 성공!")
        
        cursor = connection.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()
        print(f"   📊 MySQL 버전: {version[0]}")
        
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        print(f"   📁 사용 가능한 데이터베이스:")
        for db in databases:
            mark = "👉" if db[0] == 'livith_v2' else "   "
            print(f"      {mark} {db[0]}")
        
        # 테이블 확인
        cursor.execute("USE livith_v2")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"\n   📋 livith_v2 데이터베이스의 테이블 ({len(tables)}개):")
        for table in tables[:5]:  # 처음 5개만 표시
            print(f"      - {table[0]}")
        if len(tables) > 5:
            print(f"      ... 외 {len(tables)-5}개")
        
        cursor.close()
        connection.close()
        
        print("\n✅ 모든 연결 테스트 성공!")
        print("\n📌 Python 코드에서 사용할 연결 정보:")
        print("   connection = mysql.connector.connect(")
        print("       host='localhost',")
        print("       port=3307,")
        print("       user='root',")
        print("       password='livith0407',")
        print("       database='livith_v3',")
        print("       charset='utf8mb4'")
        print("   )")
        return True
        
    except Error as e:
        print(f"   ❌ MySQL 연결 실패: {e}")
        
        if "Access denied" in str(e):
            print("   🔑 인증 실패 - 사용자명이나 비밀번호 확인")
        elif "Unknown database" in str(e):
            print("   📁 데이터베이스가 존재하지 않음")
        elif "Can't connect" in str(e):
            print("   🌐 SSH 터널은 열려있지만 MySQL 연결 실패")
            print("      - EC2에서 RDS 접근 권한 확인 필요")
            print("      - RDS 보안 그룹 설정 확인 필요")
        
        print("\n💡 확인사항:")
        print("   1. SSH 터널이 정상적으로 실행 중인가?")
        print("   2. SSH 키 파일 경로와 권한이 올바른가? (chmod 400)")
        print("   3. EC2 인스턴스가 실행 중인가?")
        print("   4. RDS가 EC2에서 접근 가능하도록 설정되어 있나?")
    
    return False

if __name__ == "__main__":
    check_connection_details()