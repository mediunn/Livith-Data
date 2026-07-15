#!/usr/bin/env python3
"""
songs.csv만 MySQL DB에 UPDATE하는 스크립트 (프로덕션 + 개발 서버)
"""
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent.parent))
from lib.config import Config
from lib.db_utils import get_db_manager, get_dev_db_manager


def update_songs(db) -> bool:
    """songs.csv → songs 테이블 UPDATE"""
    try:
        csv_file_path = str(Config.OUTPUT_DIR / 'songs.csv')
        df = pd.read_csv(csv_file_path, encoding='utf-8').fillna('')

        print(f"  • CSV 레코드: {len(df)}개")

        df_with_lyrics = df[df['lyrics'].str.strip() != '']
        print(f"  • 가사 있는 곡: {len(df_with_lyrics)}개")

        query = """
            INSERT INTO songs (title, artist, lyrics, pronunciation, translation, youtube_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                lyrics = VALUES(lyrics),
                pronunciation = VALUES(pronunciation),
                translation = VALUES(translation),
                youtube_id = VALUES(youtube_id),
                updated_at = VALUES(updated_at)
        """

        current_time = datetime.now()
        update_count = 0
        insert_count = 0

        for i, row in df.iterrows():
            db.cursor.execute(query, (
                row['title'],
                row['artist'],
                row.get('lyrics', ''),
                row.get('pronunciation', ''),
                row.get('translation', ''),
                row.get('youtube_id', ''),
                current_time,
                current_time
            ))

            if db.cursor.rowcount == 1:
                insert_count += 1
            elif db.cursor.rowcount == 2:
                update_count += 1

            # 20개마다 중간 커밋
            if (i + 1) % 20 == 0:
                db.connection.commit()
                print(f"    처리 중... {i + 1}/{len(df)} (커밋)")

        db.connection.commit()

        print(f"\n  ✅ UPDATE 완료!")
        print(f"     • 업데이트: {update_count}개")
        print(f"     • 신규 추가: {insert_count}개")
        print(f"     • 변경 없음: {len(df) - update_count - insert_count}개")

        # 최종 통계
        db.cursor.execute("SELECT COUNT(*) FROM songs WHERE lyrics != ''")
        total_with_lyrics = db.cursor.fetchone()[0]
        db.cursor.execute("SELECT COUNT(*) FROM songs WHERE pronunciation != ''")
        total_with_pronunciation = db.cursor.fetchone()[0]
        db.cursor.execute("SELECT COUNT(*) FROM songs WHERE translation != ''")
        total_with_translation = db.cursor.fetchone()[0]

        print(f"\n📊 DB 통계:")
        print(f"     • 가사 있는 곡: {total_with_lyrics}개")
        print(f"     • 발음 있는 곡: {total_with_pronunciation}개")
        print(f"     • 번역 있는 곡: {total_with_translation}개")

        return True

    except Exception as e:
        print(f"  ❌ songs UPDATE 실패: {e}")
        db.rollback()
        return False


def run_update(db, label: str):
    """DB 연결 후 songs 업데이트 실행"""
    print(f"\n🚀 [{label}] songs UPDATE 시작")
    if not db.connect_with_ssh():
        print(f"❌ [{label}] DB 연결 실패")
        return
    try:
        update_songs(db)
    finally:
        db.disconnect()


def main():
    run_update(get_db_manager(), "프로덕션")
    run_update(get_dev_db_manager(), "개발")


if __name__ == "__main__":
    main()
