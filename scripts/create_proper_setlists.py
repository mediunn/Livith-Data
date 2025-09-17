#!/usr/bin/env python3
"""
아티스트/콘서트별로 적절한 셋리스트 생성 및 MySQL 업로드 필드 수정
- 각 콘서트별로 해당 아티스트의 곡들로 셋리스트 생성
- setlist_date, setlist_title, song_title 필드 추가
"""
import pandas as pd
import subprocess
import mysql.connector
from mysql.connector import Error
import time
import os
import signal
from pathlib import Path
from datetime import datetime

class ProperSetlistCreator:
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

    def create_setlists_by_concert(self):
        """콘서트별로 셋리스트 생성"""
        print("\n🎤 콘서트별 셋리스트 생성...")
        
        # CSV 파일 읽기
        concerts_df = pd.read_csv(f"{self.csv_base_path}/concerts.csv")
        songs_df = pd.read_csv(f"{self.csv_base_path}/songs.csv")
        
        print(f"  📁 {len(concerts_df)}개 콘서트, {len(songs_df)}개 곡 로드")
        
        # 기존 파일 확인
        existing_setlists = set()
        existing_setlist_songs = set()
        
        # 기존 setlists.csv 읽기
        setlists_path = Path(f"{self.csv_base_path}/setlists.csv")
        if setlists_path.exists():
            existing_setlists_df = pd.read_csv(setlists_path)
            existing_setlists = set(existing_setlists_df['title'])
            print(f"  📌 기존 셋리스트: {len(existing_setlists)}개")
        
        # 기존 setlist_songs.csv 읽기
        setlist_songs_path = Path(f"{self.csv_base_path}/setlist_songs.csv")
        if setlist_songs_path.exists():
            existing_setlist_songs_df = pd.read_csv(setlist_songs_path)
            # (title, artist, setlist_id) 조합으로 중복 체크
            valid_existing = existing_setlist_songs_df[
                (existing_setlist_songs_df['title'].notna()) & 
                (existing_setlist_songs_df['artist'].notna())
            ]
            existing_setlist_songs = set(
                zip(valid_existing['title'], valid_existing['artist'], valid_existing['setlist_id'])
            )
            print(f"  📌 기존 setlist_songs: {len(existing_setlist_songs)}개")
        
        # 새로운 setlists와 setlist_songs 생성
        new_setlists = []
        new_setlist_songs = []
        new_concert_setlists = []
        
        setlist_id = 1
        if setlists_path.exists():
            # 기존 최대 ID부터 시작
            existing_setlists_df = pd.read_csv(setlists_path)
            if 'id' in existing_setlists_df.columns and not existing_setlists_df.empty:
                setlist_id = existing_setlists_df['id'].max() + 1
        
        for _, concert in concerts_df.iterrows():
            concert_title = concert['title']
            concert_artist = concert['artist']
            concert_date = concert.get('start_date', '')
            
            # 해당 아티스트의 모든 곡 가져오기
            artist_songs = songs_df[songs_df['artist'] == concert_artist]
            
            if len(artist_songs) == 0:
                print(f"  ⚠️ {concert_artist}의 곡이 없음")
                continue
            
            # 셋리스트 생성 (중복 체크)
            setlist_name = f"{concert_title} - Setlist"
            
            # 이미 존재하는 셋리스트면 스킵
            if setlist_name in existing_setlists:
                print(f"  ⏭️  {concert_title}: 이미 셋리스트 존재")
                continue
            
            new_setlists.append({
                'id': setlist_id,
                'title': setlist_name,
                'artist': concert_artist,
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
                'img_url': concert.get('img_url', ''),
                'end_date': concert.get('end_date', ''),
                'start_date': concert.get('start_date', ''),
                'venue': concert.get('venue', '')
            })
            
            # concert_setlists 매핑
            new_concert_setlists.append({
                'concert_title': concert_title,
                'setlist_name': setlist_name,
                'setlist_id': setlist_id
            })
            
            # 해당 셋리스트의 곡들 추가 (중복 체크)
            for order_idx, (_, song) in enumerate(artist_songs.iterrows(), 1):
                # 중복 체크
                song_key = (song['title'], song['artist'], setlist_id)
                if song_key in existing_setlist_songs:
                    continue
                
                new_setlist_songs.append({
                    'title': song['title'],
                    'artist': song['artist'],
                    'setlist_id': setlist_id,
                    'setlist_name': setlist_name,
                    'order': order_idx,
                    'order_index': order_idx,
                    'lyrics': song.get('lyrics', ''),
                    'pronunciation': song.get('pronunciation', ''),
                    'translation': song.get('translation', ''),
                    'musixmatch_url': song.get('musixmatch_url', ''),
                    # MySQL 업로드용 추가 필드
                    'setlist_date': concert_date,
                    'setlist_title': setlist_name,
                    'song_title': song['title']
                })
            
            print(f"  ✅ {concert_title}: {len(artist_songs)}곡 셋리스트 생성")
            setlist_id += 1
        
        # DataFrame 생성
        setlists_df = pd.DataFrame(new_setlists)
        setlist_songs_df = pd.DataFrame(new_setlist_songs)
        concert_setlists_df = pd.DataFrame(new_concert_setlists)
        
        print(f"\n📊 생성 결과:")
        print(f"  • {len(setlists_df)}개 셋리스트")
        print(f"  • {len(setlist_songs_df)}개 셋리스트 곡")
        print(f"  • {len(concert_setlists_df)}개 콘서트-셋리스트 매핑")
        
        return setlists_df, setlist_songs_df, concert_setlists_df

    def save_csv_files(self, setlists_df, setlist_songs_df, concert_setlists_df):
        """CSV 파일 저장"""
        print("\n💾 CSV 파일 저장...")
        
        # 백업 생성
        timestamp = int(time.time())
        
        # setlists.csv - 기존 데이터와 병합
        setlists_path = Path(f"{self.csv_base_path}/setlists.csv")
        if setlists_path.exists():
            backup_path = Path(f"{self.csv_base_path}/setlists_backup_{timestamp}.csv")
            import shutil
            shutil.copy2(setlists_path, backup_path)
            print(f"  💾 setlists 백업: {backup_path}")
            
            # 기존 데이터 읽고 새 데이터 추가
            existing_setlists = pd.read_csv(setlists_path)
            combined_setlists = pd.concat([existing_setlists, setlists_df], ignore_index=True)
            # 중복 제거 (title 기준)
            combined_setlists = combined_setlists.drop_duplicates(subset=['title'], keep='first')
            combined_setlists.to_csv(setlists_path, index=False, encoding='utf-8')
            print(f"  ✅ setlists.csv 저장: 기존 {len(existing_setlists)}개 + 신규 {len(setlists_df)}개 = {len(combined_setlists)}개")
        else:
            setlists_df.to_csv(setlists_path, index=False, encoding='utf-8')
            print(f"  ✅ setlists.csv 저장: {len(setlists_df)}개")
        
        # setlist_songs.csv - 기존 데이터와 병합
        setlist_songs_path = Path(f"{self.csv_base_path}/setlist_songs.csv")
        if setlist_songs_path.exists():
            backup_path = Path(f"{self.csv_base_path}/setlist_songs_backup_{timestamp}.csv")
            import shutil
            shutil.copy2(setlist_songs_path, backup_path)
            print(f"  💾 setlist_songs 백업: {backup_path}")
            
            # 기존 데이터 읽고 새 데이터 추가
            existing_setlist_songs = pd.read_csv(setlist_songs_path)
            # 필요한 컬럼만 선택
            setlist_songs_columns = ['title', 'artist', 'setlist_id', 'order', 
                                     'lyrics', 'pronunciation', 'translation', 'musixmatch_url']
            new_setlist_songs = setlist_songs_df[setlist_songs_columns]
            combined_setlist_songs = pd.concat([existing_setlist_songs, new_setlist_songs], ignore_index=True)
            # 중복 제거 (title, artist, setlist_id 기준)
            combined_setlist_songs = combined_setlist_songs.drop_duplicates(
                subset=['title', 'artist', 'setlist_id'], keep='first'
            )
            combined_setlist_songs.to_csv(setlist_songs_path, index=False, encoding='utf-8')
            print(f"  ✅ setlist_songs.csv 저장: 기존 {len(existing_setlist_songs)}개 + 신규 {len(setlist_songs_df)}개 = {len(combined_setlist_songs)}개")
        else:
            # 필요한 컬럼만 선택하여 저장
            setlist_songs_columns = ['title', 'artist', 'setlist_id', 'order', 
                                     'lyrics', 'pronunciation', 'translation', 'musixmatch_url']
            setlist_songs_df[setlist_songs_columns].to_csv(setlist_songs_path, index=False, encoding='utf-8')
            print(f"  ✅ setlist_songs.csv 저장: {len(setlist_songs_df)}개")
        
        # concert_setlists.csv
        concert_setlists_path = Path(f"{self.csv_base_path}/concert_setlists.csv")
        if concert_setlists_path.exists():
            backup_path = Path(f"{self.csv_base_path}/concert_setlists_backup_{timestamp}.csv")
            import shutil
            shutil.copy2(concert_setlists_path, backup_path)
            print(f"  💾 concert_setlists 백업: {backup_path}")
        
        concert_setlists_df.to_csv(concert_setlists_path, index=False, encoding='utf-8')
        print(f"  ✅ concert_setlists.csv 저장: {len(concert_setlists_df)}개")

    def upload_to_mysql(self, setlists_df, setlist_songs_df):
        """MySQL에 업로드 (추가 필드 포함)"""
        print("\n⬆️ MySQL 업로드...")
        
        try:
            # 1. setlists 테이블 업로드
            print("  📤 setlists 테이블 업로드...")
            for _, row in setlists_df.iterrows():
                insert_query = """
                    INSERT INTO setlists (title, artist, created_at, updated_at, img_url, end_date, start_date, venue)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        updated_at = VALUES(updated_at)
                """
                self.cursor.execute(insert_query, (
                    row['title'],
                    row['artist'],
                    row['created_at'],
                    row['updated_at'],
                    row.get('img_url', ''),
                    row.get('end_date', ''),
                    row.get('start_date', ''),
                    row.get('venue', '')
                ))
            self.connection.commit()
            print(f"    ✅ {len(setlists_df)}개 셋리스트 업로드")
            
            # 2. setlist_id 매핑 가져오기
            self.cursor.execute("SELECT id, title FROM setlists")
            setlist_mapping = {title: id for id, title in self.cursor.fetchall()}
            
            # 3. song_id 매핑 가져오기
            self.cursor.execute("SELECT id, title, artist FROM songs")
            song_mapping = {(title, artist): id for id, title, artist in self.cursor.fetchall()}
            
            # 4. setlist_songs 테이블 업로드
            print("  📤 setlist_songs 테이블 업로드...")
            upload_count = 0
            skip_count = 0
            
            for _, row in setlist_songs_df.iterrows():
                setlist_id = setlist_mapping.get(row['setlist_name'])
                song_id = song_mapping.get((row['title'], row['artist']))
                
                if not setlist_id or not song_id:
                    skip_count += 1
                    continue
                
                # 중복 체크
                check_query = """
                    SELECT id FROM setlist_songs 
                    WHERE setlist_id = %s AND song_id = %s AND order_index = %s
                """
                self.cursor.execute(check_query, (setlist_id, song_id, row['order_index']))
                
                if not self.cursor.fetchone():
                    insert_query = """
                        INSERT INTO setlist_songs (setlist_id, song_id, order_index, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        setlist_id,
                        song_id,
                        row['order_index'],
                        datetime.now(),
                        datetime.now()
                    ))
                    upload_count += 1
            
            self.connection.commit()
            print(f"    ✅ {upload_count}개 setlist_songs 업로드 (스킵: {skip_count}개)")
            
            return True
            
        except Exception as e:
            print(f"  ❌ MySQL 업로드 실패: {e}")
            self.connection.rollback()
            return False

    def verify_data(self):
        """데이터 검증"""
        print("\n🔍 데이터 검증...")
        
        # CSV 데이터 확인
        setlist_songs_df = pd.read_csv(f"{self.csv_base_path}/setlist_songs.csv")
        
        # 유효한 데이터 확인
        valid_data = setlist_songs_df[
            (setlist_songs_df['title'].notna()) & 
            (setlist_songs_df['title'] != '') &
            (setlist_songs_df['artist'].notna()) & 
            (setlist_songs_df['artist'] != '')
        ]
        
        print(f"  ✅ 유효한 setlist_songs: {len(valid_data)}개")
        print(f"  📊 고유 곡: {len(valid_data[['title', 'artist']].drop_duplicates())}개")
        print(f"  📊 셋리스트 수: {valid_data['setlist_id'].nunique()}개")
        
        # 아티스트별 통계
        artist_stats = valid_data.groupby('artist').agg({
            'title': 'count',
            'setlist_id': 'nunique'
        }).rename(columns={'title': '곡수', 'setlist_id': '셋리스트수'})
        
        print("\n📈 아티스트별 통계 (상위 10개):")
        for artist, stats in artist_stats.nlargest(10, '곡수').iterrows():
            print(f"  • {artist}: {stats['곡수']}곡, {stats['셋리스트수']}개 셋리스트")

    def run(self):
        """전체 프로세스 실행"""
        try:
            print("=" * 70)
            print("🎤 콘서트별 셋리스트 생성 및 MySQL 업로드")
            print("=" * 70)
            
            # 1. 콘서트별 셋리스트 생성
            setlists_df, setlist_songs_df, concert_setlists_df = self.create_setlists_by_concert()
            
            # 2. CSV 파일 저장
            self.save_csv_files(setlists_df, setlist_songs_df, concert_setlists_df)
            
            # 3. MySQL 연결 및 업로드
            if self.create_ssh_tunnel() and self.connect_mysql():
                self.upload_to_mysql(setlists_df, setlist_songs_df)
            
            # 4. 데이터 검증
            self.verify_data()
            
            print("\n" + "=" * 70)
            print("✅ 셋리스트 생성 및 업로드 완료!")
            print("=" * 70)
            
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.close_connections()

if __name__ == "__main__":
    creator = ProperSetlistCreator()
    creator.run()