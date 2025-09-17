#!/usr/bin/env python3
"""
setlist_songs.csv의 setlist_id를 실제 DB의 setlist ID로 매핑하여 업로드하는 스크립트
"""
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import os
from datetime import datetime

class SetlistSongsUploader:
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None
        self.cleaned_data_path = '/Users/youz2me/Xcode/Livith-Data/output/cleaned_data'

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
            
            config = {
                'host': '127.0.0.1',
                'port': 3307,
                'user': 'root',
                'password': 'livith0407',
                'database': 'livith_v3',
                'charset': 'utf8mb4',
                'use_unicode': True
            }
            
            self.connection = mysql.connector.connect(**config)
            self.cursor = self.connection.cursor()
            
            print("✅ MySQL 연결 성공!")
            return True
            
        except Error as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False

    def clear_result_buffer(self):
        """결과 버퍼 정리"""
        try:
            self.cursor.fetchall()
        except:
            pass

    def upload_setlist_songs(self):
        """setlist_songs.csv 업로드 (setlist_id 매핑 포함)"""
        try:
            print("\n🎶 setlist_songs.csv 매핑 업로드 중...")
            
            csv_path = f"{self.cleaned_data_path}/setlist_songs.csv"
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # 1. DB에서 setlist title -> id 매핑 생성
            print("  • setlist title → ID 매핑 생성 중...")
            self.cursor.execute("SELECT id, title FROM setlists")
            db_setlists = self.cursor.fetchall()
            self.clear_result_buffer()
            
            title_to_id = {}
            for db_id, db_title in db_setlists:
                title_to_id[db_title.strip()] = db_id
            
            print(f"  • DB setlist 개수: {len(title_to_id)}개")
            
            # 2. CSV에서 고유한 setlist title 확인
            csv_titles = df['setlist_title'].unique()
            print(f"  • CSV setlist 개수: {len(csv_titles)}개")
            
            # 3. 매핑되지 않는 title 확인
            missing_titles = []
            for title in csv_titles:
                if title.strip() not in title_to_id:
                    missing_titles.append(title)
            
            if missing_titles:
                print(f"  ⚠️ 매핑되지 않는 setlist: {len(missing_titles)}개")
                for title in missing_titles[:5]:
                    print(f"    - {title}")
                return False
            
            # 4. setlist_songs 업로드
            print("  • setlist_songs 데이터 업로드 중...")
            
            # 기존 데이터 확인 후 삭제 (중복 방지)
            self.cursor.execute("SELECT COUNT(*) FROM setlist_songs")
            existing_count = self.cursor.fetchone()[0]
            self.clear_result_buffer()
            
            if existing_count > 0:
                print(f"  • 기존 데이터 삭제: {existing_count}개")
                self.cursor.execute("DELETE FROM setlist_songs")
                self.connection.commit()
            
            insert_count = 0
            current_time = datetime.now()
            
            for _, row in df.iterrows():
                setlist_title = row['setlist_title'].strip()
                setlist_id = title_to_id[setlist_title]
                
                # song_id도 확인 (존재하는지)
                song_id = row['song_id']
                self.cursor.execute("SELECT id FROM songs WHERE id = %s", (song_id,))
                song_exists = self.cursor.fetchone()
                self.clear_result_buffer()
                
                if not song_exists:
                    print(f"    ⚠️ Song ID {song_id} 없음, 스킵")
                    continue
                
                insert_query = """
                    INSERT INTO setlist_songs (setlist_id, song_id, order_index, fanchant, 
                                             fanchant_point, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_query, (
                    setlist_id,
                    song_id,
                    row['order_index'],
                    row.get('fanchant', ''),
                    row.get('fanchant_point', ''),
                    current_time,
                    current_time
                ))
                insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ setlist_songs: {insert_count}개 삽입 완료")
            return True
            
        except Exception as e:
            print(f"  ❌ setlist_songs 업로드 실패: {e}")
            if self.connection:
                self.connection.rollback()
            return False

    def close_connections(self):
        """연결 종료"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            if self.ssh_process:
                self.ssh_process.terminate()
                self.ssh_process.wait()
            print("🔒 모든 연결 종료 완료")
        except:
            pass

    def run(self):
        """메인 실행"""
        try:
            print("=" * 60)
            print("🎶 SETLIST_SONGS 매핑 업로드")
            print("=" * 60)
            
            if not self.create_ssh_tunnel():
                return
                
            if not self.connect_mysql():
                return
                
            success = self.upload_setlist_songs()
            
            if success:
                print("\n✅ setlist_songs 업로드 완료!")
            else:
                print("\n❌ setlist_songs 업로드 실패")
                
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    uploader = SetlistSongsUploader()
    uploader.run()