#!/usr/bin/env python3
"""
최종 개선된 CSV to MySQL UPSERT 스크립트 - 완전한 중복 방지 로직 포함
"""
import subprocess
import mysql.connector
from mysql.connector import Error
import pandas as pd
import time
import signal
import os
from datetime import datetime

class FinalImprovedUpsertCSVToMySQL:
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

    def upsert_md_with_duplicate_prevention(self):
        """md.csv → md 테이블 (완전한 중복 방지)"""
        try:
            print("\n🛍️ md.csv UPSERT 중 (중복 방지 강화)...")
            
            csv_path = f"{self.csv_base_path}/md.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ md.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            self.clear_result_buffer()
            
            # 중복 제거를 위한 세트
            processed_items = set()
            insert_count = 0
            duplicate_count = 0
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                if not concert_id:
                    continue
                    
                item_name = row['item_name'][:100] if row['item_name'] else ''
                if not item_name:
                    continue
                
                # 1. CSV 내 중복 체크
                item_key = (concert_id, item_name)
                if item_key in processed_items:
                    duplicate_count += 1
                    continue
                processed_items.add(item_key)
                
                # 2. DB 중복 체크
                self.cursor.execute(
                    "SELECT id FROM md WHERE concert_id = %s AND name = %s",
                    (concert_id, item_name)
                )
                existing = self.cursor.fetchone()
                self.clear_result_buffer()
                
                if existing:
                    duplicate_count += 1
                    continue
                
                # 3. INSERT
                current_time = datetime.now()
                insert_query = """
                    INSERT INTO md (concert_id, name, price, img_url, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_query, (
                    concert_id,
                    item_name,
                    row.get('price', '')[:30],
                    row.get('img_url', ''),
                    current_time,
                    current_time
                ))
                insert_count += 1
            
            if insert_count > 0:
                self.connection.commit()
            
            print(f"  ✅ md 테이블: {insert_count}개 삽입, {duplicate_count}개 중복 스킵")
            return True
            
        except Exception as e:
            print(f"  ❌ md UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_schedule_with_duplicate_prevention(self):
        """schedule.csv → schedule 테이블 (완전한 중복 방지)"""
        try:
            print("\n📅 schedule.csv UPSERT 중 (중복 방지 강화)...")
            
            csv_path = f"{self.csv_base_path}/schedule.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ schedule.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # concert_id 매핑
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            self.clear_result_buffer()
            
            # scheduled_at 컬럼 처리
            def parse_scheduled_at(date_str):
                if not date_str:
                    return datetime.now()
                try:
                    for fmt in ['%Y-%m-%d %H:%M:%S', '%Y.%m.%d %H:%M', '%Y-%m-%d', '%Y.%m.%d']:
                        try:
                            return datetime.strptime(date_str, fmt)
                        except:
                            continue
                    return datetime.now()
                except:
                    return datetime.now()
            
            # 중복 제거를 위한 세트
            processed_items = set()
            insert_count = 0
            duplicate_count = 0
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                if not concert_id:
                    continue
                    
                category = row['category'][:50] if row['category'] else ''
                if not category:
                    continue
                
                # scheduled_at 파싱
                scheduled_at = parse_scheduled_at(row.get('scheduled_at', ''))
                scheduled_at_str = scheduled_at.strftime('%Y-%m-%d %H:%M:%S')
                
                # 1. CSV 내 중복 체크 (concert_id + category + scheduled_at)
                item_key = (concert_id, category, scheduled_at_str)
                if item_key in processed_items:
                    duplicate_count += 1
                    continue
                processed_items.add(item_key)
                
                # 2. DB 중복 체크 (concert_id + category + scheduled_at)
                self.cursor.execute(
                    "SELECT id FROM schedule WHERE concert_id = %s AND category = %s AND scheduled_at = %s",
                    (concert_id, category, scheduled_at)
                )
                existing = self.cursor.fetchone()
                self.clear_result_buffer()
                
                if existing:
                    duplicate_count += 1
                    continue
                
                # 3. INSERT
                current_time = datetime.now()
                schedule_type = row.get('type', 'CONCERT')
                if schedule_type not in ['CONCERT', 'TICKETING']:
                    schedule_type = 'CONCERT'
                
                insert_query = """
                    INSERT INTO schedule (concert_id, category, scheduled_at, type, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_query, (
                    concert_id,
                    category,
                    scheduled_at,
                    schedule_type,
                    current_time,
                    current_time
                ))
                insert_count += 1
            
            if insert_count > 0:
                self.connection.commit()
            
            print(f"  ✅ schedule 테이블: {insert_count}개 삽입, {duplicate_count}개 중복 스킵")
            return True
            
        except Exception as e:
            print(f"  ❌ schedule UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_home_concert_sections_with_duplicate_prevention(self):
        """home_concert_sections.csv → home_concert_sections 테이블 (완전한 중복 방지)"""
        try:
            print("\n🏠 home_concert_sections.csv UPSERT 중 (중복 방지 강화)...")
            
            csv_path = f"{self.csv_base_path}/home_concert_sections.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ home_concert_sections.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # 매핑 데이터
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            self.clear_result_buffer()
            
            self.cursor.execute("SELECT id, section_title FROM home_sections")
            home_section_mapping = {title: id for id, title in self.cursor.fetchall()}
            self.clear_result_buffer()
            
            # 중복 제거를 위한 세트
            processed_items = set()
            insert_count = 0
            duplicate_count = 0
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                section_id = home_section_mapping.get(row['section_title'])
                
                if not concert_id or not section_id:
                    continue
                
                # 1. CSV 내 중복 체크 (section_id + concert_id 조합)
                item_key = (section_id, concert_id)
                if item_key in processed_items:
                    duplicate_count += 1
                    continue
                processed_items.add(item_key)
                
                # 2. DB 중복 체크
                self.cursor.execute(
                    "SELECT id FROM home_concert_sections WHERE home_section_id = %s AND concert_id = %s",
                    (section_id, concert_id)
                )
                existing = self.cursor.fetchone()
                self.clear_result_buffer()
                
                if existing:
                    duplicate_count += 1
                    continue
                
                # 3. INSERT
                current_time = datetime.now()
                sorted_index = len(processed_items)  # 순서대로 인덱스 할당
                
                insert_query = """
                    INSERT INTO home_concert_sections 
                    (home_section_id, concert_id, section_title, concert_title, sorted_index, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_query, (
                    section_id,
                    concert_id,
                    row['section_title'],
                    row['concert_title'],
                    sorted_index,
                    current_time,
                    current_time
                ))
                insert_count += 1
            
            if insert_count > 0:
                self.connection.commit()
            
            print(f"  ✅ home_concert_sections 테이블: {insert_count}개 삽입, {duplicate_count}개 중복 스킵")
            return True
            
        except Exception as e:
            print(f"  ❌ home_concert_sections UPSERT 실패: {e}")
            self.connection.rollback()
            return False

    def upsert_search_concert_sections_with_duplicate_prevention(self):
        """search_concert_sections.csv → search_concert_sections 테이블 (완전한 중복 방지)"""
        try:
            print("\n🔍 search_concert_sections.csv UPSERT 중 (중복 방지 강화)...")
            
            csv_path = f"{self.csv_base_path}/search_concert_sections.csv"
            if not os.path.exists(csv_path):
                print("  ⚠️ search_concert_sections.csv 파일이 없습니다.")
                return True
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            df = df.fillna('')
            
            print(f"  • CSV 레코드: {len(df)}개")
            
            # 매핑 데이터
            self.cursor.execute("SELECT id, title FROM concerts")
            concert_mapping = {title: id for id, title in self.cursor.fetchall()}
            self.clear_result_buffer()
            
            self.cursor.execute("SELECT id, section_title FROM search_sections")
            search_section_mapping = {title: id for id, title in self.cursor.fetchall()}
            self.clear_result_buffer()
            
            # 중복 제거를 위한 세트
            processed_items = set()
            insert_count = 0
            duplicate_count = 0
            
            for _, row in df.iterrows():
                concert_id = concert_mapping.get(row['concert_title'])
                section_id = search_section_mapping.get(row['section_title'])
                
                if not concert_id or not section_id:
                    continue
                
                # 1. CSV 내 중복 체크 (section_id + concert_id 조합)
                item_key = (section_id, concert_id)
                if item_key in processed_items:
                    duplicate_count += 1
                    continue
                processed_items.add(item_key)
                
                # 2. DB 중복 체크
                self.cursor.execute(
                    "SELECT id FROM search_concert_sections WHERE search_section_id = %s AND concert_id = %s",
                    (section_id, concert_id)
                )
                existing = self.cursor.fetchone()
                self.clear_result_buffer()
                
                if existing:
                    duplicate_count += 1
                    continue
                
                # 3. INSERT
                current_time = datetime.now()
                sorted_index = len(processed_items)  # 순서대로 인덱스 할당
                
                insert_query = """
                    INSERT INTO search_concert_sections 
                    (search_section_id, concert_id, section_title, concert_title, sorted_index, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_query, (
                    section_id,
                    concert_id,
                    row['section_title'],
                    row['concert_title'],
                    sorted_index,
                    current_time,
                    current_time
                ))
                insert_count += 1
            
            if insert_count > 0:
                self.connection.commit()
            
            print(f"  ✅ search_concert_sections 테이블: {insert_count}개 삽입, {duplicate_count}개 중복 스킵")
            return True
            
        except Exception as e:
            print(f"  ❌ search_concert_sections UPSERT 실패: {e}")
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
                os.killpg(os.getpgid(self.ssh_process.pid), signal.SIGTERM)
            print("\n🔒 모든 연결 종료 완료")
        except Exception as e:
            print(f"⚠️ 연결 종료 중 오류: {e}")

    def run_duplicate_prone_tables_only(self):
        """중복 문제가 있는 테이블만 처리"""
        try:
            print("\n" + "="*70)
            print("🚀 중복 방지 강화 UPSERT (문제 테이블만)")
            print("="*70)
            
            # SSH 터널 생성
            if not self.create_ssh_tunnel():
                print("❌ SSH 터널 생성 실패")
                return
            
            # MySQL 연결
            if not self.connect_mysql():
                print("❌ MySQL 연결 실패")
                return
            
            print("\n" + "="*50)
            print("📊 중복 방지 강화 테이블 처리")
            print("="*50)
            
            # 중복 문제가 있는 테이블들만 처리
            success_count = 0
            total_count = 4
            
            if self.upsert_md_with_duplicate_prevention():
                success_count += 1
                
            if self.upsert_schedule_with_duplicate_prevention():
                success_count += 1
                
            if self.upsert_home_concert_sections_with_duplicate_prevention():
                success_count += 1
                
            if self.upsert_search_concert_sections_with_duplicate_prevention():
                success_count += 1
            
            print("\n" + "="*70)
            print(f"✅ 중복 방지 강화 처리 완료! ({success_count}/{total_count})")
            print("="*70)
            
        except KeyboardInterrupt:
            print("\n\n⚠️ 사용자에 의해 중단됨")
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    upserter = FinalImprovedUpsertCSVToMySQL()
    upserter.run_duplicate_prone_tables_only()