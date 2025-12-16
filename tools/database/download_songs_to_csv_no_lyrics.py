#!/usr/bin/env python3
"""
가사 없는 곡들만 CSV로 다운로드
"""
import pandas as pd
import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from lib.db_utils import get_db_manager
from lib.config import Config

def download_songs_no_lyrics():
    """가사 없는 곡들 CSV로 다운로드"""
    db = get_db_manager()
    
    if not db.connect_with_ssh():
        return False
    
    try:
        db.cursor = db.connection.cursor(dictionary=True)
        query = "SELECT * FROM songs WHERE lyrics IS NULL OR lyrics = ''"
        db.cursor.execute(query)
        data = db.cursor.fetchall()
        
        if not data:
            print("✅ 가사 없는 곡이 없습니다!")
            return True
        
        df = pd.DataFrame(data)
        
        csv_file = "songs.csv"
        csv_path = os.path.join(Config.OUTPUT_DIR, csv_file)
        
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"📁 가사 없는 곡 → {csv_file} ({len(df)}개)")
        
        return True
        
    except Exception as e:
        print(f"❌ 다운로드 실패: {e}")
        return False
    finally:
        db.disconnect()


if __name__ == "__main__":
    print("🚀 가사 없는 곡 다운로드 시작")
    if download_songs_no_lyrics():
        print("🎉 완료!")