#!/usr/bin/env python3
"""
MySQL에서 실제 songs id를 조회해서 setlist_songs의 song_id를 올바르게 매핑
"""
import pandas as pd
from pathlib import Path
import subprocess
import mysql.connector
from mysql.connector import Error
import time
import os
import signal
from datetime import datetime

class SetlistSongsSongIdFixer:
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None
        self.csv_base_path = '/Users/youz2me/Xcode/Livith-Data/output/main_output'

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
            print(f"❌ SSH 터널 생성 실패: {e}")
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
            print("✅ MySQL 연결 성공!")
            return True
            
        except Error as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False

    def close_connections(self):
        """연결 종료"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        if self.ssh_process:
            os.killpg(os.getpgid(self.ssh_process.pid), signal.SIGTERM)
        print("\n👋 연결 종료 완료")

    def get_mysql_songs_mapping(self):
        """MySQL에서 실제 songs id 매핑 가져오기"""
        print("\n🔍 MySQL songs 테이블에서 실제 id 매핑 조회...")
        
        try:
            self.cursor.execute("SELECT id, title, artist FROM songs ORDER BY id")
            results = self.cursor.fetchall()
            
            song_mapping = {}
            for song_id, title, artist in results:
                key = (title, artist)
                song_mapping[key] = song_id
            
            print(f"  ✅ MySQL에서 {len(song_mapping)}개 곡 매핑 조회 완료")
            print(f"  📊 song_id 범위: {min([id for _, _, id in results])} ~ {max([id for _, _, id in results])}")
            
            return song_mapping
            
        except Exception as e:
            print(f"  ❌ MySQL songs 조회 실패: {e}")
            return {}

    def fix_setlist_songs_song_ids(self):
        """setlist_songs의 song_id를 MySQL 실제 id로 수정"""
        try:
            # 1. MySQL에서 실제 song_id 매핑 가져오기
            mysql_song_mapping = self.get_mysql_songs_mapping()
            
            if not mysql_song_mapping:
                print("❌ MySQL song 매핑을 가져올 수 없습니다.")
                return False
            
            # 2. 현재 setlist_songs.csv 읽기
            csv_path = Path(self.csv_base_path)
            setlist_songs_df = pd.read_csv(csv_path / 'setlist_songs.csv')
            
            print(f"\n📁 현재 setlist_songs.csv: {len(setlist_songs_df)}개 레코드")
            
            # 3. song_id 수정
            print("\n🔧 song_id 수정 중...")
            
            fixed_count = 0
            not_found_count = 0
            
            for idx, row in setlist_songs_df.iterrows():
                song_key = (row['song_title'], row.get('artist', ''))  # artist 정보가 없을 수도 있음
                
                # song_title만으로 매칭 시도
                if song_key not in mysql_song_mapping:
                    # artist 정보 없이 title만으로 매칭 시도
                    title_matches = [k for k in mysql_song_mapping.keys() if k[0] == row['song_title']]
                    if title_matches:
                        song_key = title_matches[0]  # 첫 번째 매칭 사용
                
                mysql_song_id = mysql_song_mapping.get(song_key)
                
                if mysql_song_id:
                    setlist_songs_df.at[idx, 'song_id'] = mysql_song_id
                    fixed_count += 1
                else:
                    print(f"  ⚠️ 곡을 찾을 수 없음: {row['song_title']}")
                    not_found_count += 1
            
            # 4. 백업 및 저장
            backup_path = csv_path / f"setlist_songs_backup_{int(time.time())}.csv"
            setlist_songs_df.to_csv(backup_path, index=False, encoding='utf-8')
            print(f"\n💾 백업 생성: {backup_path}")
            
            # 수정된 파일 저장
            setlist_songs_df.to_csv(csv_path / 'setlist_songs.csv', index=False, encoding='utf-8')
            
            print(f"\n✅ song_id 수정 완료!")
            print(f"  수정된 레코드: {fixed_count}개")
            print(f"  찾지 못한 곡: {not_found_count}개")
            
            if fixed_count > 0:
                # 샘플 출력
                sample = setlist_songs_df.head(3)
                print(f"\n📋 수정된 데이터 샘플:")
                for i, (_, row) in enumerate(sample.iterrows()):
                    print(f"  {i+1}. {row['song_title']} (song_id: {row['song_id']}, setlist_id: {row['setlist_id']})")
            
            return fixed_count > 0
            
        except Exception as e:
            print(f"❌ song_id 수정 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self):
        """전체 프로세스 실행"""
        try:
            print("=" * 60)
            print("🔧 setlist_songs song_id MySQL 매핑 수정")
            print("=" * 60)
            
            # MySQL 연결
            if not self.create_ssh_tunnel() or not self.connect_mysql():
                return
            
            # song_id 수정
            if self.fix_setlist_songs_song_ids():
                print("\n" + "=" * 60)
                print("✅ setlist_songs song_id 수정 완료!")
                print("이제 setlist_songs.csv를 업로드할 수 있습니다.")
                print("=" * 60)
            else:
                print("\n" + "=" * 60)
                print("❌ setlist_songs song_id 수정 실패")
                print("=" * 60)
            
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.close_connections()

if __name__ == "__main__":
    fixer = SetlistSongsSongIdFixer()
    fixer.run()