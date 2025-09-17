#!/usr/bin/env python3
"""
남은 cleaned_data 테이블들을 안전하게 업로드하는 스크립트
"""
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import os
from datetime import datetime

class RemainingTablesUploader:
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
    
    def get_unique_columns(self, table_name):
        """테이블별 중복 체크용 고유 컬럼 정의"""
        unique_columns_map = {
            'home_concert_sections': ['home_section_id', 'concert_id'],
            'search_concert_sections': ['search_section_id', 'concert_id'], 
            'concert_setlists': ['concert_id', 'setlist_id'],
            'schedule': ['concert_id', 'category', 'scheduled_at'],
            'md': ['concert_id', 'name'],
            'concert_info': ['concert_id'],
            'concert_genres': ['concert_id', 'genre']
        }
        return unique_columns_map.get(table_name, [])

    def upload_with_foreign_key_mapping(self, table_name, csv_filename, fk_mappings={}):
        """Foreign Key 매핑과 함께 테이블 업로드"""
        try:
            print(f"\n📊 {table_name} 업로드 중...")
            
            csv_path = f"{self.cleaned_data_path}/{csv_filename}"
            if not os.path.exists(csv_path):
                print(f"  ⚠️ {csv_filename} 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # FK 매핑 테이블 생성
            mappings = {}
            for fk_column, (ref_table, ref_title_column, ref_id_column) in fk_mappings.items():
                self.cursor.execute(f"SELECT {ref_id_column}, {ref_title_column} FROM {ref_table}")
                ref_data = self.cursor.fetchall()
                self.clear_result_buffer()
                
                title_to_id = {}
                for ref_id, ref_title in ref_data:
                    title_to_id[str(ref_title).strip()] = ref_id
                mappings[fk_column] = title_to_id
                print(f"    - {fk_column} 매핑: {len(title_to_id)}개")
            
            # 기존 데이터 개수 확인 (삭제하지 않음)
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            existing_count = self.cursor.fetchone()[0]
            self.clear_result_buffer()
            
            print(f"  • 기존 데이터: {existing_count}개 (중복 방지 모드)")
            
            # 데이터 업로드
            insert_count = 0
            skip_count = 0
            current_time = datetime.now()
            
            # 컬럼 정보 가져오기
            self.cursor.execute(f"DESCRIBE {table_name}")
            table_columns = [row[0] for row in self.cursor.fetchall()]
            self.clear_result_buffer()
            
            # id와 timestamp 컬럼 제외하고 데이터 컬럼만 추출
            data_columns = [col for col in table_columns if col not in ['id', 'created_at', 'updated_at']]
            
            for _, row in df.iterrows():
                # FK 매핑
                mapped_row = row.copy()
                skip_row = False
                
                for fk_column, mapping in mappings.items():
                    if fk_column in row:
                        original_value = str(row[fk_column]).strip()
                        if original_value in mapping:
                            mapped_row[fk_column] = mapping[original_value]
                        else:
                            print(f"    ⚠️ {fk_column}='{original_value}' 매핑 없음, 스킵")
                            skip_row = True
                            break
                
                if skip_row:
                    skip_count += 1
                    continue
                
                # INSERT 쿼리 생성
                values = []
                for col in data_columns:
                    if col in mapped_row:
                        values.append(mapped_row[col])
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
                
                # 중복 체크 로직 추가
                unique_columns = self.get_unique_columns(table_name)
                if unique_columns:
                    # 중복 체크 쿼리 생성
                    check_conditions = []
                    check_values = []
                    
                    for col in unique_columns:
                        if col in insert_columns:
                            col_index = insert_columns.index(col)
                            check_conditions.append(f"{col} = %s")
                            check_values.append(values[col_index])
                    
                    if check_conditions:
                        check_query = f"SELECT id FROM {table_name} WHERE {' AND '.join(check_conditions)}"
                        self.cursor.execute(check_query, check_values)
                        existing = self.cursor.fetchone()
                        self.clear_result_buffer()
                        
                        if existing:
                            skip_count += 1
                            continue
                
                insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                self.cursor.execute(insert_query, values)
                insert_count += 1
            
            self.connection.commit()
            print(f"  ✅ {table_name}: {insert_count}개 삽입, {skip_count}개 스킵")
            return True
            
        except Exception as e:
            print(f"  ❌ {table_name} 업로드 실패: {e}")
            if self.connection:
                self.connection.rollback()
            return False

    def upload_remaining_tables(self):
        """남은 모든 테이블 업로드"""
        success_count = 0
        
        # 1. concert_info (FK: concerts.title -> concerts.id)
        if self.upload_with_foreign_key_mapping(
            'concert_info', 
            'concert_info.csv',
            {'concert_id': ('concerts', 'title', 'id')}
        ):
            success_count += 1
        
        # 2. cultures (no FK)
        if self.upload_with_foreign_key_mapping('cultures', 'cultures.csv'):
            success_count += 1
        
        # 3. concert_genres (FK: concerts.title -> concerts.id)
        if self.upload_with_foreign_key_mapping(
            'concert_genres', 
            'concert_genres.csv',
            {'concert_id': ('concerts', 'title', 'id')}
        ):
            success_count += 1
        
        # 4. concert_setlists (FK: concerts.title -> concerts.id, setlists.title -> setlists.id)
        if self.upload_with_foreign_key_mapping(
            'concert_setlists', 
            'concert_setlists.csv',
            {
                'concert_id': ('concerts', 'title', 'id'),
                'setlist_id': ('setlists', 'title', 'id')
            }
        ):
            success_count += 1
        
        # 5. home_sections, search_sections (no FK)
        if self.upload_with_foreign_key_mapping('home_sections', 'home_sections.csv'):
            success_count += 1
        if self.upload_with_foreign_key_mapping('search_sections', 'search_sections.csv'):
            success_count += 1
        
        # 6. home_concert_sections, search_concert_sections (FK: concerts.title -> concerts.id)
        if self.upload_with_foreign_key_mapping(
            'home_concert_sections', 
            'home_concert_sections.csv',
            {'concert_id': ('concerts', 'title', 'id')}
        ):
            success_count += 1
        
        if self.upload_with_foreign_key_mapping(
            'search_concert_sections', 
            'search_concert_sections.csv',
            {'concert_id': ('concerts', 'title', 'id')}
        ):
            success_count += 1
        
        # 7. md, schedule (FK: concerts.title -> concerts.id)
        if self.upload_with_foreign_key_mapping(
            'md', 
            'md.csv',
            {'concert_id': ('concerts', 'title', 'id')}
        ):
            success_count += 1
        
        if self.upload_with_foreign_key_mapping(
            'schedule', 
            'schedule.csv',
            {'concert_id': ('concerts', 'title', 'id')}
        ):
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
            print("📊 남은 테이블들 안전 업로드")
            print("=" * 70)
            
            if not self.create_ssh_tunnel():
                return
                
            if not self.connect_mysql():
                return
                
            success_count = self.upload_remaining_tables()
            
            print(f"\n✅ 남은 테이블 업로드 완료: {success_count}/10개")
                
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    uploader = RemainingTablesUploader()
    uploader.run()