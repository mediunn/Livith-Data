#!/usr/bin/env python3
"""
MySQL 데이터베이스에서 CSV 파일로 데이터를 다운로드하는 스크립트
"""
import pandas as pd
import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from lib.db_utils import get_db_manager


def download_table(table_name):
    """MySQL 테이블을 CSV로 다운로드"""
    db = get_db_manager()
    
    # 연결
    if not db.connect_with_ssh():
        return False
    
    try:
        # 쿼리 실행
        db.cursor = db.connection.cursor(dictionary=True)
        query = f"SELECT * FROM {table_name}"
        db.cursor.execute(query)
        data = db.cursor.fetchall()
        
        if not data:
            print(f"⚠️ {table_name} 테이블이 비어있습니다.")
            return True
        
        # DataFrame 생성
        df = pd.DataFrame(data)
        
        # CSV 저장
        csv_file = f"{table_name}.csv"
        csv_path = db.get_data_path(csv_file)
        
        # 백업 생성
        if os.path.exists(csv_path):
            backup_file = f"{table_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            backup_path = db.get_backup_path(backup_file)
            df_backup = pd.read_csv(csv_path)
            df_backup.to_csv(backup_path, index=False, encoding='utf-8-sig')
            print(f"💾 백업 생성: {backup_file}")
        
        # 새 파일 저장
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"📁 {table_name} → {csv_file} ({len(df)}개 레코드)")
        
        return True
        
    except Exception as e:
        print(f"❌ 다운로드 실패: {e}")
        return False
    finally:
        db.disconnect()


def main():
    """concerts 테이블 다운로드"""
    print("🚀 MySQL → CSV 다운로드 시작")
    
    tables = ["concerts"]
    
    for table_name in tables:
        if not download_table(table_name):
            print(f"❌ {table_name} 다운로드 실패")
            return False
    
    print("🎉 모든 테이블 다운로드 완료!")
    return True


if __name__ == "__main__":
    main()