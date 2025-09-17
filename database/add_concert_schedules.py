#!/usr/bin/env python3
"""
각 콘서트마다 schedule 레코드 추가
"""
import subprocess
import mysql.connector
import pandas as pd
import time
import os
from datetime import datetime

class ConcertScheduleAdder:
    def __init__(self):
        self.ssh_process = None
        self.connection = None
        self.cursor = None

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

    def add_concert_schedules(self):
        """각 콘서트에 대한 schedule 추가"""
        try:
            print("\n📅 콘서트 일정 추가 중...")
            
            # 모든 콘서트 정보 가져오기
            self.cursor.execute("""
                SELECT id, title, start_date 
                FROM concerts
                WHERE start_date IS NOT NULL AND start_date != ''
                ORDER BY id
            """)
            concerts = self.cursor.fetchall()
            self.clear_result_buffer()
            
            print(f"  • 처리할 콘서트: {len(concerts)}개")
            
            # 기존 콘서트 카테고리 일정 확인
            self.cursor.execute("""
                SELECT concert_id 
                FROM schedule 
                WHERE category = '콘서트' AND type = 'concert'
            """)
            existing_concert_ids = set([row[0] for row in self.cursor.fetchall()])
            self.clear_result_buffer()
            
            print(f"  • 기존 콘서트 일정: {len(existing_concert_ids)}개")
            
            insert_count = 0
            skip_count = 0
            current_time = datetime.now()
            
            for concert_id, title, start_date in concerts:
                # 이미 존재하는 경우 스킵
                if concert_id in existing_concert_ids:
                    skip_count += 1
                    continue
                
                # start_date를 scheduled_at으로 변환
                # start_date 형식: "2025-09-06" 
                try:
                    # scheduled_at 형식으로 변환 (시간 추가)
                    scheduled_at = f"{start_date} 19:00:00"  # 기본 시간 19:00
                    
                    insert_query = """
                        INSERT INTO schedule (concert_id, category, type, scheduled_at, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    
                    self.cursor.execute(insert_query, (
                        concert_id,
                        '콘서트',
                        'concert',
                        scheduled_at,
                        current_time,
                        current_time
                    ))
                    
                    insert_count += 1
                    print(f"    ✅ {title[:30]}... -> {start_date}")
                    
                except Exception as e:
                    print(f"    ⚠️ {title[:30]}... 실패: {e}")
                    skip_count += 1
            
            self.connection.commit()
            
            print(f"\n  📊 결과:")
            print(f"    • 새로 추가: {insert_count}개")
            print(f"    • 스킵: {skip_count}개")
            
            # 최종 확인
            self.cursor.execute("SELECT COUNT(*) FROM schedule WHERE category = '콘서트'")
            total_concert_schedules = self.cursor.fetchone()[0]
            self.clear_result_buffer()
            
            print(f"    • 총 콘서트 일정: {total_concert_schedules}개")
            
            return True
            
        except Exception as e:
            print(f"  ❌ 일정 추가 실패: {e}")
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
            print("=" * 60)
            print("📅 콘서트 일정 Schedule 추가")
            print("=" * 60)
            
            if not self.create_ssh_tunnel():
                return
                
            if not self.connect_mysql():
                return
                
            success = self.add_concert_schedules()
            
            if success:
                print("\n✅ 콘서트 일정 추가 완료!")
            else:
                print("\n❌ 콘서트 일정 추가 실패")
                
        except Exception as e:
            print(f"\n❌ 오류: {e}")
        finally:
            self.close_connections()

if __name__ == "__main__":
    adder = ConcertScheduleAdder()
    adder.run()