#!/usr/bin/env python3
"""
남은 cleaned_data 테이블들을 간단히 업로드 (CSV의 ID가 이미 올바른 경우)
"""
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import os
from datetime import datetime

class SimpleRemainingUploader:
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

    def upload_simple_table(self, table_name, csv_filename):
        """간단한 테이블 업로드 (FK는 이미 올바른 값)"""
        try:
            print(f"\n📊 {table_name} 업로드 중...")
            
            csv_path = f"{self.cleaned_data_path}/{csv_filename}"
            if not os.path.exists(csv_path):
                print(f"  ⚠️ {csv_filename} 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # 기존 데이터 삭제 (필요한 경우만)
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                existing_count = self.cursor.fetchone()[0]
                self.clear_result_buffer()
                
                if existing_count > 0:
                    print(f"  • 기존 데이터 삭제: {existing_count}개")
                    self.cursor.execute(f"DELETE FROM {table_name}")
                    self.connection.commit()
            except:
                pass  # 외래키 제약으로 삭제 불가능한 경우 스킵
            
            # 테이블 컬럼 정보 가져오기
            self.cursor.execute(f"DESCRIBE {table_name}")
            table_columns = [row[0] for row in self.cursor.fetchall()]
            self.clear_result_buffer()
            
            # id와 timestamp 컬럼 제외하고 데이터 컬럼만 추출
            data_columns = [col for col in table_columns if col not in ['id', 'created_at', 'updated_at']]
            
            # 데이터 업로드
            insert_count = 0
            skip_count = 0
            current_time = datetime.now()
            
            for _, row in df.iterrows():
                # 데이터 값 준비
                values = []
                for col in data_columns:
                    if col in row:
                        values.append(row[col])
                    else:
                        values.append('')
                
                # created_at, updated_at 추가
                if 'created_at' in table_columns:
                    values.append(current_time)
                if 'updated_at' in table_columns:
                    values.append(current_time)
                
                insert_columns = data_columns[:]
                if 'created_at' in table_columns:
                    insert_columns.append('created_at')
                if 'updated_at' in table_columns:
                    insert_columns.append('updated_at')
                
                placeholders = ', '.join(['%s'] * len(values))
                columns_str = ', '.join(insert_columns)
                
                insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                
                try:
                    self.cursor.execute(insert_query, values)
                    insert_count += 1
                except Exception as e:
                    print(f"    ⚠️ 삽입 실패: {e}")
                    skip_count += 1
            
            self.connection.commit()
            print(f"  ✅ {table_name}: {insert_count}개 삽입, {skip_count}개 스킵")
            return True
            
        except Exception as e:
            print(f"  ❌ {table_name} 업로드 실패: {e}")
            if self.connection:
                self.connection.rollback()
            return False

    def upload_all_remaining(self):
        """모든 남은 테이블 업로드"""
        success_count = 0
        
        # 1. cultures (no FK)
        if self.upload_simple_table('cultures', 'cultures.csv'):
            success_count += 1
        
        # 2. concert_info (FK: concert_id는 이미 올바른 값)
        if self.upload_simple_table('concert_info', 'concert_info.csv'):
            success_count += 1
        
        # 3. concert_genres (FK: concert_id는 이미 올바른 값)
        if self.upload_simple_table('concert_genres', 'concert_genres.csv'):
            success_count += 1
        
        # 4. concert_setlists (FK: concert_id, setlist_id 모두 올바른 값)
        if self.upload_simple_table('concert_setlists', 'concert_setlists.csv'):
            success_count += 1
        
        # 5. md (FK: concert_id는 이미 올바른 값)
        if self.upload_simple_table('md', 'md.csv'):
            success_count += 1
        
        # 6. schedule (FK: concert_id는 이미 올바른 값)
        if self.upload_simple_table('schedule', 'schedule.csv'):
            success_count += 1
        
        # 7. home_concert_sections (FK: concert_id는 올바름, home_section_id는 기존 유지)
        if self.upload_simple_table('home_concert_sections', 'home_concert_sections.csv'):
            success_count += 1
        
        # 8. search_concert_sections (FK: concert_id는 올바름, search_section_id는 기존 유지)
        if self.upload_simple_table('search_concert_sections', 'search_concert_sections.csv'):
            success_count += 1
        
        return success_count

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
            print("📊 남은 테이블들 간단 업로드")
            print("=" * 70)
            
            if not self.create_ssh_tunnel():
                return
                
            if not self.connect_mysql():
                return
                
            success_count = self.upload_all_remaining()
            
            print(f"\n✅ 남은 테이블 업로드 완료: {success_count}/8개")
                
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    uploader = SimpleRemainingUploader()
    uploader.run()