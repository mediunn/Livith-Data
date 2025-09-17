#!/usr/bin/env python3
"""
setlist_songs.csv를 title+artist 매핑으로 올바른 song_id로 변환하여 업로드
"""
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import os
from datetime import datetime

class FinalSetlistSongsUploader:
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

    def upload_setlist_songs_with_mapping(self):
        """setlist_songs.csv 업로드 (title+artist 매핑 사용)"""
        try:
            print("\n🎶 setlist_songs.csv 완전 매핑 업로드 중...")
            
            # 1. CSV 파일들 로드
            setlist_songs_csv = f"{self.cleaned_data_path}/setlist_songs.csv"
            df_setlist_songs = pd.read_csv(setlist_songs_csv, encoding='utf-8')
            df_setlist_songs = df_setlist_songs.fillna('')
            
            print(f"  • setlist_songs CSV: {len(df_setlist_songs)}개")
            
            # 2. DB에서 매핑 테이블 생성
            print("  • DB 매핑 테이블 생성 중...")
            
            # setlist title -> id 매핑
            self.cursor.execute("SELECT id, title FROM setlists")
            db_setlists = self.cursor.fetchall()
            self.clear_result_buffer()
            
            setlist_title_to_id = {}
            for db_id, db_title in db_setlists:
                setlist_title_to_id[db_title.strip()] = db_id
                
            print(f"    - setlists 매핑: {len(setlist_title_to_id)}개")
            
            # song (title, artist) -> id 매핑
            self.cursor.execute("SELECT id, title, artist FROM songs")
            db_songs = self.cursor.fetchall()
            self.clear_result_buffer()
            
            song_key_to_id = {}
            for db_id, db_title, db_artist in db_songs:
                key = f"{db_title.strip()}|{db_artist.strip()}"
                song_key_to_id[key] = db_id
                
            print(f"    - songs 매핑: {len(song_key_to_id)}개")
            
            # 3. setlist_songs 변환 및 업로드
            print("  • setlist_songs 데이터 변환 및 업로드...")
            
            # 기존 데이터 삭제
            self.cursor.execute("SELECT COUNT(*) FROM setlist_songs")
            existing_count = self.cursor.fetchone()[0]
            self.clear_result_buffer()
            
            if existing_count > 0:
                print(f"    - 기존 데이터 삭제: {existing_count}개")
                self.cursor.execute("DELETE FROM setlist_songs")
                self.connection.commit()
            
            insert_count = 0
            skip_count = 0
            current_time = datetime.now()
            
            for _, row in df_setlist_songs.iterrows():
                # setlist_id 매핑
                setlist_title = row['setlist_title'].strip()
                if setlist_title not in setlist_title_to_id:
                    print(f"    ⚠️ Setlist '{setlist_title}' 매핑 없음, 스킵")
                    skip_count += 1
                    continue
                    
                new_setlist_id = setlist_title_to_id[setlist_title]
                
                # song_id 매핑
                song_title = row['song_title'].strip()
                song_artist = row.get('song_artist', '').strip()
                
                # song_artist가 CSV에 없는 경우, setlist의 artist 정보 사용
                if not song_artist:
                    # setlist에서 artist 정보 추출 시도
                    # DB에서 해당 setlist의 artist 정보 가져오기
                    self.cursor.execute("SELECT artist FROM setlists WHERE id = %s", (new_setlist_id,))
                    setlist_result = self.cursor.fetchone()
                    self.clear_result_buffer()
                    
                    if setlist_result:
                        song_artist = setlist_result[0].strip()
                
                song_key = f"{song_title}|{song_artist}"
                
                if song_key not in song_key_to_id:
                    # artist 없이 title로만 검색 시도
                    title_only_matches = [k for k in song_key_to_id.keys() if k.split('|')[0] == song_title]
                    if len(title_only_matches) == 1:
                        song_key = title_only_matches[0]
                        new_song_id = song_key_to_id[song_key]
                    else:
                        print(f"    ⚠️ Song '{song_title}' by '{song_artist}' 매핑 없음, 스킵")
                        skip_count += 1
                        continue
                else:
                    new_song_id = song_key_to_id[song_key]
                
                # setlist_songs에 삽입
                insert_query = """
                    INSERT INTO setlist_songs (setlist_id, song_id, order_index, fanchant, 
                                             fanchant_point, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_query, (
                    new_setlist_id,
                    new_song_id,
                    row['order_index'],
                    row.get('fanchant', ''),
                    row.get('fanchant_point', ''),
                    current_time,
                    current_time
                ))
                insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ setlist_songs: {insert_count}개 삽입, {skip_count}개 스킵")
            
            # 4. 결과 검증
            print("  • 업로드 결과 검증...")
            self.cursor.execute("SELECT COUNT(*) FROM setlist_songs")
            final_count = self.cursor.fetchone()[0]
            self.clear_result_buffer()
            
            self.cursor.execute("SELECT COUNT(DISTINCT setlist_id) FROM setlist_songs")
            setlist_count = self.cursor.fetchone()[0]
            self.clear_result_buffer()
            
            print(f"    - 최종 setlist_songs: {final_count}개")
            print(f"    - 포함된 setlist: {setlist_count}개")
            
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
            print("=" * 70)
            print("🎶 SETLIST_SONGS 완전 매핑 업로드 (최종)")
            print("=" * 70)
            
            if not self.create_ssh_tunnel():
                return
                
            if not self.connect_mysql():
                return
                
            success = self.upload_setlist_songs_with_mapping()
            
            if success:
                print("\n✅ setlist_songs 완전 업로드 완료!")
            else:
                print("\n❌ setlist_songs 업로드 실패")
                
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    uploader = FinalSetlistSongsUploader()
    uploader.run()