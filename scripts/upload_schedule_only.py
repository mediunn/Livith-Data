#!/usr/bin/env python3
"""
schedule.csv만 업로드하는 스크립트
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.selective_upsert_csv_to_mysql import SelectiveUpsertCSVToMySQL

def upload_schedule():
    """schedule.csv만 업로드"""
    uploader = SelectiveUpsertCSVToMySQL()
    
    try:
        # SSH 터널 및 MySQL 연결
        if uploader.create_ssh_tunnel() and uploader.connect_mysql():
            print("🚀 schedule.csv 업로드 시작...")
            uploader.upsert_schedule()
            print("✅ schedule.csv 업로드 완료!")
        else:
            print("❌ 연결 실패")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        uploader.close_connections()

if __name__ == "__main__":
    upload_schedule()