#!/usr/bin/env python3
import socket
import mysql.connector
from mysql.connector import Error

def check_connection_details():
    """연결 정보 단계별 확인"""
    
    host = 'livithdb.c142i2022qs5.ap-northeast-2.rds.amazonaws.com'
    port = 3306
    
    print("🔍 연결 정보 단계별 확인")
    print("="*50)
    
    # 1. 네트워크 연결 확인
    print(f"\n1️⃣ 네트워크 연결 테스트 ({host}:{port})")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print("✅ 포트 연결 성공 - 서버가 해당 포트에서 실행 중")
        else:
            print("❌ 포트 연결 실패 - 서버가 실행되지 않거나 포트가 잘못됨")
            print("\n🤔 확인사항:")
            print("   - 서버가 실제로 실행 중인가요?")
            print("   - 포트 3307이 맞나요?")
            print("   - host가 'localhost'가 맞나요? (IP 주소나 도메인이 필요할 수도)")
            return False
            
    except Exception as e:
        print(f"❌ 네트워크 오류: {e}")
        return False
    
    # 2. MySQL 연결 테스트
    print(f"\n2️⃣ MySQL 인증 테스트")
    config_variations = [
        {
            'host': 'livithdb.c142i2022qs5.ap-northeast-2.rds.amazonaws.com',
            'port': 3306,
            'user': 'root',
            'password': 'livith0407',
            'database': 'livith_v2',
            'charset': 'utf8mb4'
        },
        {
            'host': 'livithdb.c142i2022qs5.ap-northeast-2.rds.amazonaws.com',
            'port': 3307,  # 혹시 3307 포트를 사용하는 경우
            'user': 'root',
            'password': 'livith0407',
            'database': 'livith_v2',
            'charset': 'utf8mb4'
        }
    ]
    
    for i, config in enumerate(config_variations, 1):
        print(f"\n   시도 {i}: {config['host']}:{config['port']}")
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
            
            cursor.close()
            connection.close()
            return True
            
        except Error as e:
            print(f"   ❌ MySQL 연결 실패: {e}")
            
            if "Access denied" in str(e):
                print("   🔑 인증 실패 - 사용자명이나 비밀번호가 틀릴 수 있습니다")
            elif "Unknown database" in str(e):
                print("   📁 데이터베이스가 존재하지 않습니다")
            elif "Can't connect" in str(e):
                print("   🌐 서버에 연결할 수 없습니다")
    
    print(f"\n❌ 모든 연결 시도 실패")
    print("\n💡 서버 관리자에게 확인해야 할 사항:")
    print("   1. MySQL 서버가 실행 중인가?")
    print("   2. 포트 3307이 맞나?")
    print("   3. 외부 접근이 허용되어 있나?")
    print("   4. 사용자 'root'의 권한이 설정되어 있나?")
    print("   5. 데이터베이스 'livith_v2'가 존재하나?")
    
    return False

if __name__ == "__main__":
    check_connection_details()