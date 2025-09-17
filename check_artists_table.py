#!/usr/bin/env python3
"""
artists 테이블 구조 확인
"""
import subprocess
import mysql.connector
from mysql.connector import Error
import time
import os
from datetime import datetime

def main():
    # SSH 터널 생성
    ssh_command = [
        'ssh',
        '-i', '/Users/youz2me/Downloads/livith-key.pem',
        '-L', '3307:livithdb.c142i2022qs5.ap-northeast-2.rds.amazonaws.com:3306',
        '-N',
        '-o', 'StrictHostKeyChecking=no',
        'ubuntu@43.203.48.65'
    ]
    
    ssh_process = subprocess.Popen(
        ssh_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid
    )
    
    time.sleep(3)
    
    # MySQL 연결
    connection = mysql.connector.connect(
        host='127.0.0.1',
        port=3307,
        user='root',
        password='livith0407',
        database='livith_v3',
        charset='utf8mb4',
        collation='utf8mb4_unicode_ci'
    )
    
    cursor = connection.cursor()
    
    # artists 테이블 구조 확인
    print("🔍 artists 테이블 구조:")
    print("=" * 50)
    cursor.execute("DESCRIBE artists")
    columns = cursor.fetchall()
    
    for col in columns:
        field, type_, null, key, default, extra = col
        print(f"{field:<20} | {type_:<20} | {null:<5} | {key:<3} | {str(default):<7} | {extra}")
    
    print("\n🔍 artists 테이블 데이터 샘플 (3개):")
    print("=" * 50)
    cursor.execute("SELECT * FROM artists LIMIT 3")
    rows = cursor.fetchall()
    
    # 컬럼명 가져오기
    cursor.execute("DESCRIBE artists")
    column_names = [col[0] for col in cursor.fetchall()]
    
    for row in rows:
        print(f"\n레코드:")
        for i, value in enumerate(row):
            print(f"  {column_names[i]}: {value}")
    
    # 연결 종료
    cursor.close()
    connection.close()
    os.killpg(os.getpgid(ssh_process.pid), 9)

if __name__ == "__main__":
    main()