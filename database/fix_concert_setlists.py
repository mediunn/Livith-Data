#!/usr/bin/env python3
"""
concert_setlists.csv를 title 매핑으로 업로드
"""
import subprocess
import mysql.connector
import pandas as pd
import time
import os
from datetime import datetime

class ConcertSetlistsUploader:
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
            return self.ssh_process.poll() is None
                
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
            
        except Exception as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False

    def clear_result_buffer(self):
        try:
            self.cursor.fetchall()
        except:
            pass

    def upload_concert_setlists(self):
        """concert_setlists 업로드"""
        try:
            print("\n🎭 concert_setlists.csv 업로드 중...")
            
            csv_path = f"{self.cleaned_data_path}/concert_setlists.csv"
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # 매핑 테이블 생성
            self.cursor.execute("SELECT id, title FROM concerts")
            concerts = {title.strip(): cid for cid, title in self.cursor.fetchall()}
            self.clear_result_buffer()
            
            self.cursor.execute("SELECT id, title FROM setlists")
            setlists = {title.strip(): sid for sid, title in self.cursor.fetchall()}
            self.clear_result_buffer()
            
            print(f"  • 매핑: concerts {len(concerts)}개, setlists {len(setlists)}개")
            
            # 기존 데이터 삭제
            self.cursor.execute("DELETE FROM concert_setlists")
            self.connection.commit()
            
            insert_count = 0
            skip_count = 0
            current_time = datetime.now()
            
            for _, row in df.iterrows():
                concert_title = row['concert_title'].strip()
                setlist_title = row['setlist_title'].strip()
                
                if concert_title not in concerts:
                    print(f"    ⚠️ Concert '{concert_title}' 매핑 없음")
                    skip_count += 1
                    continue
                    
                if setlist_title not in setlists:
                    print(f"    ⚠️ Setlist '{setlist_title}' 매핑 없음") 
                    skip_count += 1
                    continue
                
                concert_id = concerts[concert_title]
                setlist_id = setlists[setlist_title]
                
                insert_query = """
                    INSERT INTO concert_setlists (concert_id, setlist_id, type, status, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_query, (
                    concert_id,
                    setlist_id, 
                    row.get('type', 'EXPECTED'),
                    row.get('status', ''),
                    current_time,
                    current_time
                ))
                insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ concert_setlists: {insert_count}개 삽입, {skip_count}개 스킵")
            
            return True
            
        except Exception as e:
            print(f"  ❌ concert_setlists 업로드 실패: {e}")
            if self.connection:
                self.connection.rollback()
            return False

    def close_connections(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close() 
            if self.ssh_process:
                self.ssh_process.terminate()
                self.ssh_process.wait()
            print("🔒 연결 종료 완료")
        except:
            pass

    def run(self):
        try:
            if not self.create_ssh_tunnel():
                return
                
            if not self.connect_mysql():
                return
                
            success = self.upload_concert_setlists()
            
            if success:
                print("\n✅ concert_setlists 업로드 완료!")
            else:
                print("\n❌ concert_setlists 업로드 실패")
                
        except Exception as e:
            print(f"\n❌ 오류: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    uploader = ConcertSetlistsUploader()
    uploader.run()