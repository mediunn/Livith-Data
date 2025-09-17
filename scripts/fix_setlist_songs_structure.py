#!/usr/bin/env python3
"""
setlist_songs.csv 구조 수정 및 누락 데이터 보완 스크립트
MySQL 구조에 맞춰 CSV 구조 변경하고, songs에 있는 모든 곡이 적절한 setlist에 포함되도록 보완
"""
import pandas as pd
import subprocess
import mysql.connector
from mysql.connector import Error
import time
import os
from pathlib import Path

class SetlistSongsFixerSystem:
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

    def analyze_current_state(self):
        """현재 상태 분석"""
        print("\n📊 현재 데이터 상태 분석...")
        
        # CSV 파일들 읽기
        songs_df = pd.read_csv(f"{self.csv_base_path}/songs.csv")
        setlist_songs_df = pd.read_csv(f"{self.csv_base_path}/setlist_songs.csv")
        setlists_df = pd.read_csv(f"{self.csv_base_path}/setlists.csv") if os.path.exists(f"{self.csv_base_path}/setlists.csv") else None
        
        print(f"  📁 songs.csv: {len(songs_df)}개 곡")
        print(f"  📁 setlist_songs.csv: {len(setlist_songs_df)}개 레코드")
        if setlists_df is not None:
            print(f"  📁 setlists.csv: {len(setlists_df)}개 셋리스트")
        
        # setlist_songs의 실제 곡 개수 확인
        valid_setlist_songs = setlist_songs_df[
            (setlist_songs_df['title'].notna()) & 
            (setlist_songs_df['title'] != '') &
            (setlist_songs_df['artist'].notna()) & 
            (setlist_songs_df['artist'] != '')
        ]
        print(f"  ✅ setlist_songs에서 유효한 곡: {len(valid_setlist_songs)}개")
        
        return songs_df, setlist_songs_df, setlists_df

    def download_mysql_data(self):
        """MySQL에서 최신 데이터 다운로드"""
        print("\n⬇️ MySQL에서 최신 데이터 다운로드...")
        
        try:
            # setlist_songs 테이블 다운로드
            query = """
            SELECT ss.setlist_id, ss.song_id, ss.order_index,
                   s.title, s.artist,
                   sl.name as setlist_name
            FROM setlist_songs ss
            JOIN songs s ON ss.song_id = s.id  
            JOIN setlists sl ON ss.setlist_id = sl.id
            ORDER BY ss.setlist_id, ss.order_index
            """
            
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            
            if results:
                # DataFrame 생성
                mysql_setlist_songs = pd.DataFrame(results, columns=[
                    'setlist_id', 'song_id', 'order_index', 'title', 'artist', 'setlist_name'
                ])
                
                print(f"  ✅ MySQL에서 {len(mysql_setlist_songs)}개 setlist_songs 레코드 다운로드")
                return mysql_setlist_songs
            else:
                print("  ⚠️ MySQL setlist_songs 테이블이 비어있음")
                return None
                
        except Exception as e:
            print(f"  ❌ MySQL 데이터 다운로드 실패: {e}")
            return None

    def create_setlist_for_orphaned_songs(self, songs_df, existing_setlist_songs):
        """셋리스트에 포함되지 않은 곡들을 위한 기본 셋리스트 생성"""
        print("\n🎵 셋리스트에 포함되지 않은 곡들 처리...")
        
        # 현재 setlist_songs에 있는 곡들
        if existing_setlist_songs is not None and len(existing_setlist_songs) > 0:
            existing_songs = set(zip(existing_setlist_songs['title'], existing_setlist_songs['artist']))
        else:
            existing_songs = set()
        
        # songs.csv의 모든 곡
        all_songs = set(zip(songs_df['title'], songs_df['artist']))
        
        # 누락된 곡들
        orphaned_songs = all_songs - existing_songs
        print(f"  📊 셋리스트에 포함되지 않은 곡: {len(orphaned_songs)}개")
        
        if not orphaned_songs:
            print("  ✅ 모든 곡이 이미 셋리스트에 포함됨")
            return existing_setlist_songs
        
        # 아티스트별로 그룹화하여 셋리스트 생성
        orphaned_by_artist = {}
        for title, artist in orphaned_songs:
            if artist not in orphaned_by_artist:
                orphaned_by_artist[artist] = []
            orphaned_by_artist[artist].append(title)
        
        print(f"  📊 {len(orphaned_by_artist)}명의 아티스트에 대해 추가 셋리스트 필요")
        
        # 새로운 setlist_songs 레코드 생성
        new_records = []
        setlist_id_counter = 1000  # 높은 숫자부터 시작하여 기존 ID와 충돌 방지
        
        if existing_setlist_songs is not None:
            max_existing_setlist_id = existing_setlist_songs['setlist_id'].max() if not existing_setlist_songs.empty else 0
            setlist_id_counter = max(setlist_id_counter, max_existing_setlist_id + 1)
        
        for artist, song_titles in orphaned_by_artist.items():
            setlist_name = f"{artist} - Complete Songs"
            
            for order_idx, title in enumerate(song_titles, 1):
                new_records.append({
                    'setlist_id': setlist_id_counter,
                    'song_id': 0,  # 임시 ID, 나중에 매핑 필요
                    'order_index': order_idx,
                    'title': title,
                    'artist': artist,
                    'setlist_name': setlist_name
                })
            
            setlist_id_counter += 1
        
        print(f"  ✅ {len(new_records)}개의 새로운 setlist_songs 레코드 생성")
        
        # 기존 데이터와 병합
        if existing_setlist_songs is not None and not existing_setlist_songs.empty:
            combined_df = pd.concat([existing_setlist_songs, pd.DataFrame(new_records)], ignore_index=True)
        else:
            combined_df = pd.DataFrame(new_records)
        
        return combined_df

    def create_correct_csv_structure(self, complete_setlist_songs):
        """올바른 CSV 구조로 변환"""
        print("\n📝 올바른 CSV 구조로 변환...")
        
        # 현재 setlist_songs.csv 구조에 맞춰 변환
        # 필요한 컬럼: title, artist, setlist_id, order, lyrics, pronunciation, translation, musixmatch_url
        
        # songs.csv에서 lyrics, pronunciation, translation 정보 가져오기
        songs_df = pd.read_csv(f"{self.csv_base_path}/songs.csv")
        songs_info = {}
        for _, row in songs_df.iterrows():
            key = (row['title'], row['artist'])
            songs_info[key] = {
                'lyrics': row.get('lyrics', ''),
                'pronunciation': row.get('pronunciation', ''), 
                'translation': row.get('translation', ''),
                'musixmatch_url': row.get('musixmatch_url', '')
            }
        
        # 새로운 구조로 변환
        new_setlist_songs = []
        for _, row in complete_setlist_songs.iterrows():
            key = (row['title'], row['artist'])
            song_info = songs_info.get(key, {})
            
            new_record = {
                'title': row['title'],
                'artist': row['artist'],
                'setlist_id': row['setlist_id'],
                'order': row['order_index'],
                'lyrics': song_info.get('lyrics', ''),
                'pronunciation': song_info.get('pronunciation', ''),
                'translation': song_info.get('translation', ''),
                'musixmatch_url': song_info.get('musixmatch_url', '')
            }
            new_setlist_songs.append(new_record)
        
        new_df = pd.DataFrame(new_setlist_songs)
        print(f"  ✅ {len(new_df)}개 레코드로 변환 완료")
        
        return new_df

    def save_corrected_csv(self, corrected_df):
        """수정된 CSV 저장"""
        print("\n💾 수정된 setlist_songs.csv 저장...")
        
        # 백업 생성
        original_path = Path(f"{self.csv_base_path}/setlist_songs.csv")
        backup_path = Path(f"{self.csv_base_path}/setlist_songs_backup_{int(time.time())}.csv")
        
        if original_path.exists():
            import shutil
            shutil.copy2(original_path, backup_path)
            print(f"  💾 백업 생성: {backup_path}")
        
        # 새 파일 저장
        corrected_df.to_csv(original_path, index=False, encoding='utf-8')
        print(f"  ✅ 새 파일 저장: {original_path}")
        
        # 통계 출력
        print(f"\n📊 최종 통계:")
        print(f"  • 총 레코드 수: {len(corrected_df)}")
        print(f"  • 고유 곡 수: {len(corrected_df[['title', 'artist']].drop_duplicates())}")
        print(f"  • 셋리스트 수: {corrected_df['setlist_id'].nunique()}")
        
        # 아티스트별 곡 수 (상위 10개)
        artist_counts = corrected_df['artist'].value_counts().head(10)
        print(f"\n📈 아티스트별 곡 수 (상위 10개):")
        for artist, count in artist_counts.items():
            print(f"  • {artist}: {count}곡")

    def run_fix(self):
        """전체 수정 프로세스 실행"""
        try:
            print("=" * 70)
            print("🔧 setlist_songs.csv 구조 수정 및 데이터 보완")
            print("=" * 70)
            
            # 1. 현재 상태 분석
            songs_df, setlist_songs_df, setlists_df = self.analyze_current_state()
            
            # 2. SSH 및 MySQL 연결
            if not self.create_ssh_tunnel():
                print("❌ SSH 터널 생성 실패")
                return
            
            if not self.connect_mysql():
                print("❌ MySQL 연결 실패")
                return
            
            # 3. MySQL에서 최신 데이터 다운로드
            mysql_setlist_songs = self.download_mysql_data()
            
            # 4. 누락된 곡들을 위한 셋리스트 생성
            complete_setlist_songs = self.create_setlist_for_orphaned_songs(songs_df, mysql_setlist_songs)
            
            # 5. 올바른 CSV 구조로 변환
            corrected_df = self.create_correct_csv_structure(complete_setlist_songs)
            
            # 6. 수정된 CSV 저장
            self.save_corrected_csv(corrected_df)
            
            print("\n" + "=" * 70)
            print("✅ setlist_songs.csv 수정 완료!")
            print("=" * 70)
            
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    fixer = SetlistSongsFixerSystem()
    fixer.run_fix()