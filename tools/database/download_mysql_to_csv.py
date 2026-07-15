#!/usr/bin/env python3
"""
MySQL 데이터베이스에서 CSV 파일로 데이터를 다운로드하는 스크립트
"""
import pandas as pd
import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from lib.db_utils import get_db_manager, get_dev_db_manager
from lib.config import Config

ALL_TABLES = ["artists", "concerts", "concert_genres", "schedule", "songs", "setlist_songs", "users", "user_genres", "user_interest_concerts"]

def download_table(table_name, db_factory=get_db_manager):
    """MySQL 테이블을 CSV로 다운로드"""
    db = db_factory()
    
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
        if 'id' in df.columns:
            df['id'] = df['id'].astype('Int64')
        
        # CSV 저장
        csv_file = f"{table_name}.csv"
        csv_path = os.path.join(Config.OUTPUT_DIR, csv_file)
        
        # 백업 생성
        if os.path.exists(csv_path):
            now = datetime.now()
            today_str = now.strftime('%Y%m%d')
            
            # 날짜별 백업 폴더 생성
            date_backup_dir = os.path.join(Config.BACKUP_DIR, today_str)
            os.makedirs(date_backup_dir, exist_ok=True)

            # 백업 파일명 및 전체 경로 정의
            backup_file = f"{table_name}_backup_{now.strftime('%Y%m%d_%H%M%S')}.csv"
            backup_path = os.path.join(date_backup_dir, backup_file)
            
            # 백업 수행
            df_backup = pd.read_csv(csv_path)
            df_backup.to_csv(backup_path, index=False, encoding='utf-8-sig')
            print(f"💾 백업 생성: {os.path.join(today_str, backup_file)}")
        
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
    """다운로드 대상 DB와 테이블 선택 후 실행"""

    # DB 선택
    print("어느 DB에서 다운로드할까요?")
    print("  1. Dev DB")
    print("  2. Livith DB (프로덕션)")
    while True:
        db_choice = input("선택 (1/2): ").strip()
        if db_choice in ("1", "2"):
            break
        print("1 또는 2를 입력해주세요.")
    db_factory = get_dev_db_manager if db_choice == "1" else get_db_manager
    db_label = "개발" if db_choice == "1" else "프로덕션"

    # 테이블 선택
    print(f"\n다운로드할 테이블을 선택하세요.")
    for i, t in enumerate(ALL_TABLES, 1):
        print(f"  {i}. {t}")
    print(f"  {len(ALL_TABLES) + 1}. 전체")

    while True:
        table_input = input("선택 (번호, 쉼표로 여러 개 가능): ").strip()
        try:
            choices = [int(x.strip()) for x in table_input.split(',')]
            if all(1 <= c <= len(ALL_TABLES) + 1 for c in choices):
                break
        except ValueError:
            pass
        print(f"1~{len(ALL_TABLES) + 1} 사이 숫자를 입력해주세요.")

    if len(ALL_TABLES) + 1 in choices:
        tables = ALL_TABLES
    else:
        tables = [ALL_TABLES[c - 1] for c in choices]

    print(f"\n🚀 [{db_label}] {', '.join(tables)} 다운로드 시작")
    for table_name in tables:
        if not download_table(table_name, db_factory=db_factory):
            print(f"❌ {table_name} 다운로드 실패")
            return False

    print("🎉 모든 테이블 다운로드 완료!")
    return True


if __name__ == "__main__":
    main()