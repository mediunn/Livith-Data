#!/usr/bin/env python3
"""
songs.csv와 setlist_songs.csv를 순서대로 업로드하는 스크립트
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.selective_upsert_csv_to_mysql import SelectiveUpsertCSVToMySQL

def upload_songs_and_setlist_songs():
    """songs.csv와 setlist_songs.csv 순서대로 업로드"""
    uploader = SelectiveUpsertCSVToMySQL()
    
    try:
        # SSH 터널 및 MySQL 연결
        if uploader.create_ssh_tunnel() and uploader.connect_mysql():
            print("🚀 songs.csv 업로드 시작...")
            uploader.upsert_songs()
            
            print("\n🚀 setlist_songs.csv 업로드 시작...")
            uploader.upsert_setlist_songs()
            
            print("\n✅ 모든 업로드 완료!")
        else:
            print("❌ 연결 실패")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    finally:
        uploader.close_connections()

if __name__ == "__main__":
    upload_songs_and_setlist_songs()