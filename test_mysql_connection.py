#!/usr/bin/env python3
import mysql.connector
from mysql.connector import Error

def test_connection_and_get_tables():
    """MySQL 연결 테스트 및 테이블 구조 확인"""
    
    config = {
        'host': 'localhost',
        'port': 3307,
        'user': 'root',
        'password': 'livith0407',
        'database': 'livith_v2',
        'charset': 'utf8mb4',
        'use_unicode': True
    }
    
    connection = None
    cursor = None
    
    try:
        print("MySQL 데이터베이스 연결 시도...")
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        print("✅ 연결 성공!")
        print(f"연결된 데이터베이스: {config['database']}")
        
        # 테이블 목록 확인
        print("\n📋 테이블 목록:")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        table_names = []
        for i, table in enumerate(tables, 1):
            table_name = table[0]
            table_names.append(table_name)
            print(f"  {i}. {table_name}")
        
        # 각 테이블 구조 확인
        print("\n🏗️  테이블 구조:")
        for table_name in table_names:
            print(f"\n{'='*60}")
            print(f"📊 {table_name} 테이블 구조:")
            print('='*60)
            
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            
            print(f"{'컬럼명':<20} {'타입':<20} {'NULL':<8} {'KEY':<8} {'기본값':<15} {'Extra'}")
            print('-' * 80)
            
            for col in columns:
                field, type_info, null, key, default, extra = col
                default_str = str(default) if default is not None else 'NULL'
                print(f"{field:<20} {type_info:<20} {null:<8} {key:<8} {default_str:<15} {extra}")
        
        print(f"\n🎯 총 {len(table_names)}개 테이블 확인 완료")
        
    except Error as e:
        print(f"❌ MySQL 연결 실패: {e}")
        return False
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("\n🔌 연결 종료")
    
    return True

if __name__ == "__main__":
    test_connection_and_get_tables()