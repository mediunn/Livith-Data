#!/usr/bin/env python3
"""
songs.csv만 MySQL DB에 UPDATE하는 스크립트
"""
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import signal
import os
from datetime import datetime
import sys

class UpdateSongsOnly:
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None
        self.csv_file_path = '/Users/youz2me/Xcode/Livith-Data/data/main_output/songs.csv'

    def create_ssh_tunnel(self):
        """SSH 터널 생성"""
        try:
            print("🔧 SSH 터널 생성 중...")
            
            ssh_command = [
                'ssh',
                '-i', '/Users/youz2me/Downloads/livith-key.pem',
                '-L', '3307:livithdb.c142i2022qs5.ap-northeast-2.rds.amazonaws.com:3306',
                '-N',
                '-o', 'StrictHostKeyChecking=no',
                'ubuntu@43.203.48.65'
            ]
            
            self.ssh_process = subprocess.Popen(
                ssh_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid
            )
            
            time.sleep(3)
            
            if self.ssh_process.poll() is None:
                print("✅ SSH 터널 생성 완료!")
                return True
            else:
                return False
                
        except Exception as e:
            print(f"❌ SSH 터널 오류: {e}")
            return False

    def connect_mysql(self):
        """MySQL 연결"""
        try:
            print("🔌 MySQL 연결 중...")
            
            self.connection = mysql.connector.connect(
                host='127.0.0.1',
                port=3307,
                user='root',
                password='livith0407',
                database='livith_v3',
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci'
            )
            
            self.cursor = self.connection.cursor()
            self.cursor.execute("SET NAMES utf8mb4")
            self.cursor.execute("SET CHARACTER SET utf8mb4")
            self.cursor.execute("SET character_set_connection=utf8mb4")
            
            print("✅ MySQL 연결 성공!")
            return True
            
        except Error as e:
            print(f"❌ MySQL 연결 오류: {e}")
            return False

    def update_songs(self):
        """songs.csv → songs 테이블 UPDATE"""
        try:
            print("\n🎵 songs.csv UPDATE 시작...")
            
            # CSV 읽기
            df = pd.read_csv(self.csv_file_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # 가사가 있는 곡들만 필터링
            df_with_lyrics = df[df['lyrics'].str.strip() != '']
            print(f"  • 가사 있는 곡: {len(df_with_lyrics)}개")
            
            # UPDATE 쿼리 (title + artist 조합으로 업데이트)
            update_query = """
                UPDATE songs 
                SET lyrics = %s,
                    pronunciation = %s,
                    translation = %s,
                    youtube_id = %s,
                    updated_at = %s
                WHERE title = %s AND artist = %s
            """
            
            # INSERT 쿼리 (없는 곡은 추가)
            insert_query = """
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
            
            for _, row in df.iterrows():
                # UPDATE 시도
                self.cursor.execute(update_query, (
                    row.get('lyrics', ''),
                    row.get('pronunciation', ''),
                    row.get('translation', ''),
                    row.get('youtube_id', ''),
                    current_time,
                    row['title'],
                    row['artist']
                ))
                
                if self.cursor.rowcount > 0:
                    update_count += 1
                else:
                    # UPDATE가 안 되면 INSERT
                    self.cursor.execute(insert_query, (
                        row['title'],
                        row['artist'],
                        row.get('lyrics', ''),
                        row.get('pronunciation', ''),
                        row.get('translation', ''),
                        row.get('youtube_id', ''),
                        current_time,
                        current_time
                    ))
                    if self.cursor.rowcount > 0:
                        insert_count += 1
                
                # 진행 상황 출력 (50개마다)
                if (_ + 1) % 50 == 0:
                    print(f"    처리 중... {_ + 1}/{len(df)}")
            
            self.connection.commit()
            
            print(f"\n  ✅ UPDATE 완료!")
            print(f"     • 업데이트: {update_count}개")
            print(f"     • 신규 추가: {insert_count}개")
            print(f"     • 변경 없음: {len(df) - update_count - insert_count}개")
            
            # 최종 통계
            self.cursor.execute("SELECT COUNT(*) FROM songs WHERE lyrics != ''")
            total_with_lyrics = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM songs WHERE pronunciation != ''")
            total_with_pronunciation = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM songs WHERE translation != ''")
            total_with_translation = self.cursor.fetchone()[0]
            
            print(f"\n📊 DB 통계:")
            print(f"     • 가사 있는 곡: {total_with_lyrics}개")
            print(f"     • 발음 있는 곡: {total_with_pronunciation}개")
            print(f"     • 번역 있는 곡: {total_with_translation}개")
            
            return True
            
        except Exception as e:
            print(f"  ❌ songs UPDATE 실패: {e}")
            self.connection.rollback()
            return False

    def close_connection(self):
        """연결 종료"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            print("🔌 연결 종료")
        except:
            pass

    def cleanup(self):
        """정리"""
        self.close_connection()
        
        # SSH 터널 종료
        if self.ssh_process:
            try:
                os.killpg(os.getpgid(self.ssh_process.pid), signal.SIGTERM)
            except:
                pass

def main():
    """메인 실행"""
    updater = UpdateSongsOnly()
    
    try:
        # SSH 터널 생성
        if not updater.create_ssh_tunnel():
            return
        
        # MySQL 연결
        if not updater.connect_mysql():
            return
        
        # songs 테이블 업데이트
        updater.update_songs()
        
    except KeyboardInterrupt:
        print("\n⏹️ 사용자 중단")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        updater.cleanup()

if __name__ == "__main__":
    main()